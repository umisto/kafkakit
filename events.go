package kafkakit

import "encoding/json"

type Message struct {
	Topic        string
	EventType    string
	EventVersion uint
	Key          string
	Payload      json.RawMessage
}
