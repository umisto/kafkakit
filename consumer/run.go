package consumer

import (
	"context"
	"math/rand"
	"time"

	"github.com/netbill/evebox/consumer/subscriber"
	"github.com/netbill/logium"
	"github.com/segmentio/kafka-go"
)

func Run(
	ctx context.Context,
	log logium.Logger,
	group string,
	topic string,
	router func(kafka.Message) (subscriber.HandlerFunc, bool),
	addr ...string,
) {
	restartBackoff := backoffMin

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for ctx.Err() == nil {
		sub := subscriber.New(addr, topic, group)

		err := sub.Consume(ctx, router)

		if err != nil && ctx.Err() == nil {
			log.Warnf("consumer on topic %s stopped: %v", topic, err)

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
