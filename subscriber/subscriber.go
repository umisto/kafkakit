package subscriber

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type HandlerFunc func(ctx context.Context, m kafka.Message) error

type Service struct {
	reader *kafka.Reader
}

func New(addr, topic, groupID string) *Service {
	cfg := kafka.ReaderConfig{
		Brokers:         strings.Split(addr, ","),
		Topic:           topic,
		GroupID:         groupID,
		MinBytes:        1e3,
		MaxBytes:        10e6,
		MaxWait:         500 * time.Millisecond,
		ReadLagInterval: -1,
	}
	if groupID == "" {
		cfg.StartOffset = kafka.FirstOffset
	}
	return &Service{reader: kafka.NewReader(cfg)}
}

func (s *Service) Close() {
	if err := s.reader.Close(); err != nil {
		log.Printf("failed to close reader: %v", err)
	}
}

func (s *Service) Consume(
	ctx context.Context,
	router func(m kafka.Message) (HandlerFunc, bool),
) error {
	defer s.Close()

	for {
		m, err := s.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("fetch error: %v", err)
			continue
		}

		h, ok := router(m)
		if !ok {
			if err := s.reader.CommitMessages(ctx, m); err != nil {
				log.Printf("commit skipped message error: %v", err)
			}
			continue
		}

		err = safeCall(ctx, m, h)
		if err != nil {
			log.Printf("handler error: %v", err)
			continue
		}

		if err := s.reader.CommitMessages(ctx, m); err != nil {
			log.Printf("commit error: %v", err)
		}
	}
}

func safeCall(ctx context.Context, m kafka.Message, h HandlerFunc) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("handler panic recovered: %v", r)
			err = nil
		}
	}()
	return h(ctx, m)
}

func Header(m kafka.Message, key string) (string, bool) {
	for _, h := range m.Headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}
