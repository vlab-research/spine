// Package spine provides a simple abstraction for processing
// a Kafka stream as an at-least-once "sink" of any kind.
// Messages are split into "chunks" which can be processed by a
// user-provided blocking function that performs some side effect.
// The Kafka stream is then consumed in "batches", with each batch
// consisting of one or more chunks of messages. Chunks within
// the batch are parallelized to the user-provided side effect
// function in go loops. After each batch is sucessfully processed,
// the kafka consumer commits the offset, and consumes another batch.
package spine

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"sync"
	"time"
)

type ConsumerInterface interface {
	ReadMessage(time.Duration) (*kafka.Message, error)
	Commit() ([]kafka.TopicPartition, error)
}

// KafkaConsumer is the primary type exposed by this package.
// A KafkaConsumer consumes messages in batches of size BatchSize
// and commits after each batch. It splits the batch into chunks
// of size ChunkSize for processing by a user-provided SideEffectFn.
// All chunks in a batch are processed in parallel.
type KafkaConsumer struct {
	Consumer  ConsumerInterface
	Timeout   time.Duration
	BatchSize int
	ChunkSize int
}

// TODO: make config struct
// include max poll interval

// NewKafkaConsumer is a convenience function to create the kafka.Consumer
// and return a new KafkaConsumer struct for later use.
func NewKafkaConsumer(topic string, brokers string, group string, timeout time.Duration, batchSize int, chunkSize int) KafkaConsumer {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":    brokers,
		"group.id":             group,
		"auto.offset.reset":    "earliest",
		"enable.auto.commit":   "false",
		"max.poll.interval.ms": "300000",
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{topic}, nil)

	return KafkaConsumer{c, timeout, batchSize, chunkSize}
}

func merge(cs ...<-chan error) <-chan error {
	var wg sync.WaitGroup
	out := make(chan error)

	output := func(c <-chan error) {
		for n := range c {
			out <- n
		}
		wg.Done()
	}
	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func chunk(ch chan *kafka.Message, size int) chan []*kafka.Message {
	var b []*kafka.Message
	out := make(chan []*kafka.Message)

	go func() {
		for v := range ch {
			b = append(b, v)
			if len(b) == size {
				out <- b
				b = make([]*kafka.Message, 0)
			}
		}
		// send the remaining partial buffer
		if len(b) > 0 {
			out <- b
		}
		close(out)
	}()

	return out
}

func chanWrap(fn SideEffectFn, m []*kafka.Message) <-chan error {
	errs := make(chan error)
	go func() {
		defer close(errs)
		err := fn(m)
		if err != nil {
			errs <- err
		}
	}()

	return errs
}

type SideEffectFn func([]*kafka.Message) error

func process(messages chan []*kafka.Message, fn SideEffectFn) <-chan error {
	chans := []<-chan error{}
	for m := range messages {
		chans = append(chans, chanWrap(fn, m))
	}
	return merge(chans...)
}

func (consumer KafkaConsumer) consume(errs chan error) chan []*kafka.Message {
	return chunk(consumer.consumeStream(errs), consumer.ChunkSize)
}

func (consumer KafkaConsumer) consumeStream(errs chan error) chan *kafka.Message {
	messages := make(chan *kafka.Message)
	c := consumer.Consumer

	// runs until n messages consumed
	go func() {
		defer close(messages)
		count := 0
		for i := 1; i <= consumer.BatchSize; i++ {

			msg, err := c.ReadMessage(consumer.Timeout)

			if err != nil {
				if e, ok := err.(kafka.Error); ok && e.Code() == kafka.ErrTimedOut {
					break
				}
				errs <- err
				break
			}

			messages <- msg
			count += 1
		}
		log.Printf("Consumed %v messages as batch from Kafka", count)
	}()

	return messages
}

// SideEffect is a blocking call that will calls the given function for each
// chunk of data in the batch, in parallel, and commits after the batch is
// processed. All errors are emitted on the error channel.
func (consumer KafkaConsumer) SideEffect(fn SideEffectFn, errs chan error) {
	messages := consumer.consume(errs)

	// block until finished processing
	// and all errors are checked
	perrs := process(messages, fn)
	for err := range perrs {
		errs <- err
	}

	// commit!
	_, err := consumer.Consumer.Commit()
	if err != nil {
		if e, ok := err.(kafka.Error); ok && e.Code() == kafka.ErrNoOffset {
			log.Print("Finished batch but committing 0 messages")
		} else {
			errs <- err
		}
	}
}
