package mq

import (
	"fmt"
	"sync"
	"time"

	"github.com/dynamicgo/go-config"
)

// Record message queue record
type Record interface {
	Key() []byte
	Value() []byte
}

// Producer message producer interface
type Producer interface {
	// Record create new record
	Record(key []byte, value []byte) (Record, error)
	// Send send record
	Send(record Record) error
	// Batch batch send message record
	Batch(records []Record) error
}

// Consumer message queue consumer
type Consumer interface {
	// Recv get recv record chan
	Recv() <-chan Record
	// Errors get errors chan
	Errors() <-chan error
	// Commit commit record offset
	Commit(record Record) error
}

// ConsumerF .
type ConsumerF func(config config.Config) (Consumer, error)

// ProducerF .
type ProducerF func(config config.Config) (Producer, error)

// Register mq driver register
type Register interface {
	Set(driver string, consumerF ConsumerF, producerF ProducerF)
	Get(driver string) (ConsumerF, ProducerF)
}

type registerImpl struct {
	sync.RWMutex
	consumers map[string]ConsumerF
	producers map[string]ProducerF
}

func newRegisterImpl() Register {
	return &registerImpl{
		consumers: make(map[string]ConsumerF),
		producers: make(map[string]ProducerF),
	}
}

func (register *registerImpl) Set(driver string, consumerF ConsumerF, producerF ProducerF) {
	register.Lock()
	defer register.Unlock()

	register.consumers[driver] = consumerF
	register.producers[driver] = producerF
}

func (register *registerImpl) Get(driver string) (ConsumerF, ProducerF) {
	register.RLock()
	defer register.RUnlock()

	consumerF := register.consumers[driver]
	producerF := register.producers[driver]

	return consumerF, producerF
}

var register = newRegisterImpl()

// OpenDriver register new driver
func OpenDriver(driver string, consumerF ConsumerF, producerF ProducerF) {
	register.Set(driver, consumerF, producerF)
}

// NewConsumer create new consumer with driver name and config
func NewConsumer(driver string, config config.Config) (Consumer, error) {
	consumerF, _ := register.Get(driver)

	if consumerF == nil {
		return nil, fmt.Errorf("unknown mq driver %s", driver)
	}

	return consumerF(config)
}

// NewProducer create new producer with driver name and config
func NewProducer(driver string, config config.Config) (Producer, error) {
	_, producerF := register.Get(driver)

	if producerF == nil {
		return nil, fmt.Errorf("unknown mq driver %s", driver)
	}

	return producerF(config)
}

// Retry .
type Retry struct {
	Max     int
	Backoff time.Duration
	Handler func(record Record) bool
}

// Do do process a record with retry policy
func (retry *Retry) Do(consumer Consumer, record Record) (bool, error) {
	for i := 0; i < retry.Max; i++ {
		if retry.Handler(record) {
			return true, consumer.Commit(record)
		}

		time.Sleep(retry.Backoff)
	}

	return false, consumer.Commit(record)
}
