package consumer

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/netbill/evebox/box/inbox"
	"github.com/netbill/logium"
)

type InboxWorker struct {
	log      logium.Logger
	store    inbox.Box
	cfg      InboxConfigWorker
	handlers map[string]inbox.Handler
}

type InboxConfigWorker struct {
	Name string

	//BatchSize is the maximum number of events to process in one batch.
	BatchSize int32

	//RetryDelay is the delay before retrying a failed event.
	RetryDelay time.Duration

	//MinSleep is the minimum sleep time between ticks when no work is done.
	MinSleep time.Duration
	//MaxSleep is the maximum sleep time between ticks when no work is done.
	MaxSleep time.Duration

	//Unknown is the function to call when an unknown event type is encountered.
	Unknown func(ctx context.Context, ev inbox.Event) inbox.EventStatus
}

func NewInboxWorker(log logium.Logger, store inbox.Box, cfg InboxConfigWorker) *InboxWorker {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 50
	}
	if cfg.RetryDelay <= 0 {
		cfg.RetryDelay = 1 * time.Minute
	}
	if cfg.MinSleep <= 0 {
		cfg.MinSleep = 100 * time.Millisecond
	}
	if cfg.MaxSleep <= 0 {
		cfg.MaxSleep = 2 * time.Second
	}
	if cfg.Unknown == nil {
		cfg.Unknown = func(ctx context.Context, ev inbox.Event) inbox.EventStatus {
			return inbox.EventStatusFailed
		}
	}

	return &InboxWorker{
		log:      log,
		store:    store,
		cfg:      cfg,
		handlers: map[string]inbox.Handler{},
	}
}

func (w *InboxWorker) Handle(eventType string, h inbox.Handler) {
	w.handlers[eventType] = h
}

func (w *InboxWorker) Run(ctx context.Context) {
	sleep := time.Duration(0)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		worked := w.tick(ctx)

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

func (w *InboxWorker) tick(ctx context.Context) bool {
	worked := false

	err := w.store.Transaction(ctx, func(txCtx context.Context) error {
		key, err := w.store.PickPendingInboxKey(txCtx)
		if err != nil {
			w.log.Errorf("failed to pick pending inbox key: %v", err)
			return err
		}

		locked, err := w.store.LockInboxKey(txCtx, key, w.cfg.Name, time.Now().UTC().Add(w.cfg.RetryDelay))
		if err != nil {
			w.log.Errorf("failed to lock inbox key=%s: %v", key, err)
			return err
		}
		if !locked {
			return nil
		}
		defer func() { _ = w.store.UnlockInboxKey(txCtx, key, w.cfg.Name) }()

		events, err := w.store.GetPendingInboxEvents(txCtx, key, w.cfg.BatchSize)
		if err != nil {
			w.log.Errorf("failed to get pending inbox events: %v", err)
			return err
		}
		if len(events) == 0 {
			return nil
		}

		if err = w.processBatch(txCtx, key, events); err != nil {
			w.log.Errorf("process batch failed: %v", err)
			return err
		}

		worked = true
		return nil
	})
	if err != nil {
		return false
	}

	return worked
}

func (w *InboxWorker) processBatch(ctx context.Context, key string, events []inbox.Event) error {
	processed := make([]uuid.UUID, 0, len(events))
	pending := make([]uuid.UUID, 0, 1)
	failed := make([]uuid.UUID, 0, len(events))

	distribute := func(id uuid.UUID, status inbox.EventStatus) {
		switch status {
		case inbox.EventStatusProcessed:
			processed = append(processed, id)
		case inbox.EventStatusPending:
			pending = append(pending, id)
		case inbox.EventStatusFailed:
			failed = append(failed, id)
		default:
			failed = append(failed, id)
		}
	}

	for _, event := range events {
		func(ev inbox.Event) {
			defer func() {
				if r := recover(); r != nil {
					w.log.Errorf("panic while handling inbox event id=%s type=%s: %v", ev.ID, ev.Type, r)
					failed = append(failed, ev.ID)
				}
			}()

			h, ok := w.handlers[ev.Type]
			if !ok {
				distribute(ev.ID, w.cfg.Unknown(ctx, ev))
				return
			}

			st := h(ctx, ev)
			distribute(ev.ID, st)
		}(event)

		if len(pending) != 0 {
			w.log.Infof(
				"inbox event processed id=%s type=%s: processed=%d pending=%d failed=%d",
				event.ID, event.Type, len(processed), len(pending), len(failed),
			)

			break
		}
	}

	if len(processed) > 0 {
		if _, err := w.store.MarkInboxEventsAsProcessed(ctx, processed...); err != nil {
			return err
		}
	}

	if len(failed) > 0 {
		if _, err := w.store.MarkInboxEventsAsFailed(ctx, failed...); err != nil {
			return err
		}
	}

	if len(pending) > 0 {
		next := time.Now().UTC().Add(w.cfg.RetryDelay)
		if err := w.store.BlockInboxKeyUntil(ctx, key, next); err != nil {
			return err
		}

		if _, err := w.store.MarkInboxEventsAsPending(ctx, next, pending[0]); err != nil {
			return err
		}
	} else {
		if err := w.store.UnblockInboxKey(ctx, key); err != nil {
			return err
		}
	}

	return nil
}
