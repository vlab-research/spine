package spine

import (

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func MakeMessages(vals []string) []*kafka.Message {
	msgs := []*kafka.Message{}
	for _, v := range vals {
		msg := &kafka.Message{}
		msg.Value = []byte(v)
		msgs = append(msgs, msg)
	}

	return msgs
}

type TestConsumer struct {
	Messages []*kafka.Message
	Commits int
	commitError bool
}
