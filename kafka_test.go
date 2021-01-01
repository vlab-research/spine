package spine

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockProcessor struct {
	mock.Mock
}

func (m *MockProcessor) Fn(messages []*kafka.Message) error {
	time.Sleep(10 * time.Millisecond)
	_ = m.Called(messages)
	return nil
}

func (m *MockProcessor) FnError(messages []*kafka.Message) error {
	_ = m.Called(messages)
	return errors.New("hey")
}

func TestProcessBlocksUntilFunctionsFinish(t *testing.T) {
	messages := make(chan []*kafka.Message)
	go func() {
		defer close(messages)
		for range []int{1, 2, 3} {
			messages <- []*kafka.Message{{}}
		}
	}()

	m := new(MockProcessor)
	m.On("Fn", []*kafka.Message{{}})

	errs := process(messages, m.Fn)
	for range errs {

	}
	m.AssertNumberOfCalls(t, "Fn", 3)
	m.AssertExpectations(t)
}

func TestProcessReturnsChannelWithAllErrors(t *testing.T) {
	messages := make(chan []*kafka.Message)
	go func() {
		defer close(messages)
		for range []int{1, 2, 3} {
			messages <- []*kafka.Message{{}}
		}
	}()

	m := new(MockProcessor)
	m.On("FnError", []*kafka.Message{{}})

	errs := process(messages, m.FnError)
	es := []error{}
	for e := range errs {
		es = append(es, e)
	}

	assert.Equal(t, len(es), 3, "returned all 3 errors in channel")
	m.AssertNumberOfCalls(t, "FnError", 3)
	m.AssertExpectations(t)
}

func TestSideEffectReadPartial(t *testing.T) {
	msgs := makeMessages([]string{
		`{"userid": "bar",
          "pageid": "foo",
          "updated": 1598706047838,
          "current_state": "QOUT",
          "state_json": { "token": "bar", "state": "QOUT", "tokens": ["foo"]}}`,
		`{"userid": "baz",
          "pageid": "foo",
          "updated": 1598706047838,
          "current_state": "RESPONDING",
          "state_json": { "token": "bar", "state": "QOUT", "tokens": ["foo"]}}`,
	})

	c := &TestConsumer{Messages: msgs, Commits: 0}
	consumer := KafkaConsumer{c, time.Second, 1, 1}
	count := 0
	fn := func([]*kafka.Message) error {
		count += 1
		return nil
	}

	errs := make(chan error)
	consumer.SideEffect(fn, errs)
	assert.Equal(t, c.Commits, 1)
	assert.Equal(t, len(c.Messages), 1)
}

func TestSideEffectErrorsDoesntCommitBeforeHandlingError(t *testing.T) {
	msgs := makeMessages([]string{
		`{"userid": "bar",
          "pageid": "foo",
          "updated": 1598706047838,
          "current_state": "QOUT",
          "state_json": { "token": "bar", "state": "QOUT", "tokens": ["foo"]}}`,
	})

	c := &TestConsumer{Messages: msgs, Commits: 0}
	consumer := KafkaConsumer{c, time.Second, 1, 1}
	count := 0
	fn := func([]*kafka.Message) error {
		count += 1
		return errors.New("test")
	}

	done := make(chan bool)
	errs := make(chan error)
	go func() {
		for range errs {
			assert.Equal(t, 0, c.Commits)
			close(done)
		}
	}()
	consumer.SideEffect(fn, errs)

	<-done
}

func TestSideEffectOutputsCommitErrorOnErrorChannel(t *testing.T) {
	msgs := makeMessages([]string{
		`{"userid": "bar",
          "pageid": "foo",
          "updated": 1598706047838,
          "current_state": "QOUT",
          "state_json": { "token": "bar", "state": "QOUT", "tokens": ["foo"]}}`,
	})

	c := &TestConsumer{Messages: msgs, Commits: 0, CommitError: true}
	consumer := KafkaConsumer{c, time.Second, 1, 1}
	count := 0
	fn := func([]*kafka.Message) error {
		count += 1
		return nil
	}

	done := make(chan bool)
	errs := make(chan error)
	go func() {
		for e := range errs {
			t.Log(e)
			_, ok := e.(*testError)
			assert.Equal(t, true, ok)
			close(done)
		}
	}()
	consumer.SideEffect(fn, errs)

	<-done
}

func TestSideEffectReadAllOutstanding(t *testing.T) {
	msgs := makeMessages([]string{
		`{"userid": "bar",
          "pageid": "foo",
          "updated": 1598706047838,
          "current_state": "QOUT",
          "state_json": { "token": "bar", "state": "QOUT", "tokens": ["foo"]}}`,
		`{"userid": "baz",
          "pageid": "foo",
          "updated": 1598706047838,
          "current_state": "RESPONDING",
          "state_json": { "token": "bar", "state": "QOUT", "tokens": ["foo"]}}`,
	})

	c := &TestConsumer{Messages: msgs, Commits: 0}
	consumer := KafkaConsumer{c, time.Second, 3, 3}
	count := 0
	fn := func([]*kafka.Message) error {
		count += 1
		return nil
	}

	errs := make(chan error)
	consumer.SideEffect(fn, errs)
	assert.Equal(t, 1, c.Commits)
	assert.Equal(t, 0, len(c.Messages))
}

func ExampleKafkaConsumer_SideEffect() {
	consumer, _ := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "foo",
	})

	timeout, _ := time.ParseDuration("1m")

	c := KafkaConsumer{
		Consumer:  consumer,
		Timeout:   timeout,
		BatchSize: 12,
		ChunkSize: 4,
	}

	// create error channel for
	// monitoring in a go loop
	errs := make(chan error)

	// this is your side effect function,
	// does some work.
	fn := func(msgs []*kafka.Message) error {
		for _, m := range msgs {
			fmt.Print(m)
		}
		return nil
	}

	// Perform work!
	// commits after every "batch" is processed
	for {
		c.SideEffect(fn, errs)
	}
}
