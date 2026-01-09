package outbox

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/netbill/logium"
	"github.com/segmentio/kafka-go"
)

type Worker struct {
	log    logium.Logger
	addr   []string
	outbox Box
	cfg    WorkerConfig
}

type WorkerConfig struct {
	Name string

	// BetchLimit defines maximum number of events to process in one batch
	BatchLimit int32

	// LockTTL defines how much time wee need for processing one outbox key
	LockTTL time.Duration

	// EventRetryDelay How long time wee want to do not work eith this key after failure
	EventRetryDelay time.Duration

	// MinSleep defines minimum sleep time between ticks when no work is done
	MinSleep time.Duration
	// MaxSleep defines maximum sleep time between ticks when no work is done
	MaxSleep time.Duration

	RequiredAcks kafka.RequiredAcks
	Compression  kafka.Compression
	BatchTimeout time.Duration
	Balancer     kafka.Balancer
}

func NewWorker(log logium.Logger, ob Box, addr []string, cfg WorkerConfig) *Worker {
	if cfg.BatchLimit <= 0 {
		cfg.BatchLimit = 100
	}
	if cfg.LockTTL <= 0 {
		cfg.LockTTL = 1 * time.Minute
	}
	if cfg.EventRetryDelay <= 0 {
		cfg.EventRetryDelay = 1 * time.Minute
	}
	if cfg.MinSleep <= 0 {
		cfg.MinSleep = 100 * time.Millisecond
	}
	if cfg.MaxSleep <= 0 {
		cfg.MaxSleep = 2 * time.Second
	}

	if cfg.RequiredAcks == 0 {
		cfg.RequiredAcks = kafka.RequireAll
	}
	if cfg.Compression == 0 {
		cfg.Compression = kafka.Snappy
	}
	if cfg.BatchTimeout <= 0 {
		cfg.BatchTimeout = 50 * time.Millisecond
	}
	if cfg.Balancer == nil {
		cfg.Balancer = &kafka.LeastBytes{}
	}

	return &Worker{
		log:    log,
		addr:   addr,
		outbox: ob,
		cfg:    cfg,
	}
}

func (w *Worker) Run(ctx context.Context) {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(w.addr...),
		Balancer:     w.cfg.Balancer,
		RequiredAcks: w.cfg.RequiredAcks,
		Compression:  w.cfg.Compression,
		BatchTimeout: w.cfg.BatchTimeout,
	}
	defer func() { _ = writer.Close() }()

	sleep := time.Duration(0)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		worked := w.tick(ctx, writer)

		if worked {
			sleep = 0
			continue
		}

		if sleep == 0 {
			sleep = w.cfg.MinSleep
		} else {
			sleep *= 2
			if sleep > w.cfg.MaxSleep {
				sleep = w.cfg.MaxSleep
			}
		}

		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (w *Worker) tick(ctx context.Context, writer *kafka.Writer) bool {
	key, err := w.outbox.GetPendingOutboxKey(ctx)
	if err != nil {
		w.log.Errorf("outbox.GetPendingOutboxKey: %v", err)
		return false
	}
	if key == "" {
		return false
	}

	ok, err := w.outbox.LockOutboxKey(ctx, key, w.cfg.Name, time.Now().UTC().Add(w.cfg.LockTTL))
	if err != nil {
		w.log.Errorf("outbox.LockOutboxKey(key=%s): %v", key, err)
		return false
	}
	if !ok {
		return false
	}

	defer func() {
		if deferr := w.outbox.DeleteOutboxKey(ctx, key, w.cfg.Name); deferr != nil {
			w.log.Errorf("outbox.UnlockOutboxKey(key=%s): %v", key, deferr)
		}
	}()

	var events []Event

	if err = w.outbox.Transaction(ctx, func(txCtx context.Context) error {
		var txErr error
		events, txErr = w.outbox.GetPendingOutboxEvents(txCtx, key, w.cfg.BatchLimit)
		return txErr
	}); err != nil {
		w.log.Errorf("outbox.ClaimPendingOutboxEvents(key=%s): %v", key, err)
		return false
	}
	if len(events) == 0 {
		return false
	}

	sent := make([]uuid.UUID, 0, len(events))
	pending := make([]uuid.UUID, 0, len(events))

	for _, e := range events {
		if err = writer.WriteMessages(ctx, e.ToMessage()); err != nil {
			pending = append(pending, e.ID)
			w.log.Debugf("outbox: publish failed id=%s key=%s: %v", e.ID, key, err)
			continue
		}
		sent = append(sent, e.ID)
	}

	if err = w.outbox.Transaction(ctx, func(txCtx context.Context) error {
		var txErr error

		if len(sent) > 0 {
			if _, txErr = w.outbox.MarkOutboxEventsAsSent(txCtx, sent...); txErr != nil {
				return txErr
			}
		}

		if len(pending) > 0 {
			nextRetryAt := time.Now().UTC().Add(w.cfg.EventRetryDelay)

			if _, txErr = w.outbox.MarkOutboxEventAsPending(txCtx, nextRetryAt, pending[0]); txErr != nil {
				return txErr
			}

			if txErr = w.outbox.BlockOutboxKeyUntil(txCtx, key, nextRetryAt); txErr != nil {
				return txErr
			}
		} else {
			if txErr = w.outbox.UnblockOutboxKey(txCtx, key); txErr != nil {
				return txErr
			}
		}

		return nil
	}); err != nil {
		w.log.Errorf("outbox: finalize tx failed key=%s: %v", key, err)
		return false
	}

	w.log.Debugf("outbox: name %s, key=%s, sent=%d pending=%d",
		w.cfg.Name, key, len(sent), len(pending),
	)

	if len(sent) > 0 {
		return true
	}
	return false
}
