package spine

import (
	"time"

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
	CommitError bool
}

func (c *TestConsumer) ReadMessage(d time.Duration) (*kafka.Message, error) {
	if len(c.Messages) == 0 {
		return nil, kafka.NewError(kafka.ErrTimedOut, "test", false)
	}

	head := c.Messages[0]
	c.Messages = c.Messages[1:]
	return head, nil
}

type TestError struct{ msg string }
func (e *TestError) Error() string {
    return e.msg
}

func (c *TestConsumer) Commit() ([]kafka.TopicPartition, error) {
	c.Commits += 1
	if c.CommitError {
		return nil, &TestError{"foo"}
	}
	return []kafka.TopicPartition{}, nil
}
