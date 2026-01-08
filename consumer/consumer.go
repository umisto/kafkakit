package consumer

import (
	"context"
	"time"

	"github.com/netbill/evebox/box/inbox"
	"github.com/netbill/evebox/consumer/subscriber"
	"github.com/netbill/evebox/header"
	"github.com/segmentio/kafka-go"
)

const (
	backoffMin = 1 * time.Second
	backoffMax = 30 * time.Second

	jitterFactor = 0.20
)

type Consumer struct {
	owner     string
	inbox     inbox.Box
	routes    map[string]subscriber.HandlerFunc
	onUnknown func(ctx context.Context, m kafka.Message, eventType string) error
}

func NewRouter(owner string, inbox inbox.Box) *Consumer {
	return &Consumer{
		owner:  owner,
		inbox:  inbox,
		routes: map[string]subscriber.HandlerFunc{},
		onUnknown: func(ctx context.Context, m kafka.Message, et string) error {
			return nil // if wee don't know the event type, just skip it because we don't need it
		},
	}
}

func (c *Consumer) Handle(eventType string, h inbox.Handler) {
	c.routes[eventType] = func(ctx context.Context, m kafka.Message) error {
		return c.inbox.WriteAndHandle(ctx, m, c.owner, h)
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
