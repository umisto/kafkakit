package consumer

import (
	"context"
	"math/rand"
	"time"

	"github.com/netbill/evebox/box/inbox"
	"github.com/netbill/evebox/consumer/subscriber"
	"github.com/netbill/evebox/header"
	"github.com/netbill/logium"
	"github.com/segmentio/kafka-go"
)

const (
	backoffMin = 1 * time.Second
	backoffMax = 30 * time.Second

	jitterFactor = 0.20
)

type Consumer struct {
	log       logium.Logger
	name      string
	inbox     inbox.Box
	routes    map[string]subscriber.HandlerFunc
	onUnknown func(ctx context.Context, m kafka.Message, eventType string) error
}

func New(log logium.Logger, name string, inbox inbox.Box) *Consumer {
	return &Consumer{
		log:       log,
		name:      name,
		inbox:     inbox,
		routes:    map[string]subscriber.HandlerFunc{},
		onUnknown: func(ctx context.Context, m kafka.Message, et string) error { return nil },
	}
}

func (c *Consumer) Handle(eventType string, h inbox.Handler) {
	c.routes[eventType] = func(ctx context.Context, m kafka.Message) error {
		return c.inbox.WriteAndHandle(ctx, m, c.name, h)
	}
}

func (c *Consumer) Route(m kafka.Message) (subscriber.HandlerFunc, bool) {
	et, ok := subscriber.Header(m, header.EventType)
	if !ok {
		return nil, false
	}
	h, ok := c.routes[et]
	if ok {
		return h, true
	}
	return func(ctx context.Context, m kafka.Message) error {
		return c.onUnknown(ctx, m, et)
	}, true
}

func (c *Consumer) Run(ctx context.Context, group, topic string, addr ...string) {
	restartBackoff := backoffMin
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for ctx.Err() == nil {
		sub := subscriber.New(addr, topic, group)

		err := sub.Consume(ctx, c.Route)

		if err != nil && ctx.Err() == nil {
			if c.log != nil {
				c.log.Warnf("consumer on topic %s stopped: %v", topic, err)
			}

			sleepFor := withJitter(rng, restartBackoff, jitterFactor)
			if !sleepCtx(ctx, sleepFor) {
				return
			}
			restartBackoff = nextBackoff(restartBackoff, backoffMax)
			continue
		}
		return
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func nextBackoff(cur, max time.Duration) time.Duration {
	if cur >= max {
		return max
	}
	n := cur * 2
	if n > max {
		return max
	}
	return n
}

func withJitter(rng *rand.Rand, base time.Duration, factor float64) time.Duration {
	if base <= 0 || factor <= 0 {
		return base
	}

	delta := (rng.Float64()*2 - 1) * factor
	j := float64(base) * delta

	out := time.Duration(float64(base) + j)
	if out < 0 {
		return 0
	}
	return out
}
