package stream

import (
	"context"
	"reflect"

	"github.com/dynamicgo/xerrors"

	"github.com/dynamicgo/slf4go"

	"github.com/dynamicgo/mq"
)

// Handler .
type Handler func([]byte) ([]byte, error)

// Lambda .
type Lambda interface {
	Source(consumer mq.Consumer) Lambda
	Handle(handler Handler) Lambda
	Target(producer mq.Producer) Lambda
	Start() error
}

type lambdaImpl struct {
	slf4go.Logger
	sources  []mq.Consumer
	targets  []mq.Producer
	handlers []Handler
	ctx      context.Context
}

// New .
func New() Lambda {
	return &lambdaImpl{
		Logger: slf4go.Get("steam-lambda"),
		ctx:    context.Background(),
	}
}

// NewWithContext ...
func NewWithContext(ctx context.Context) Lambda {
	return &lambdaImpl{
		Logger: slf4go.Get("steam-lambda"),
		ctx:    ctx,
	}
}

func (lambda *lambdaImpl) Source(consumer mq.Consumer) Lambda {
	lambda.sources = append(lambda.sources, consumer)
	return lambda
}

func (lambda *lambdaImpl) Handle(handler Handler) Lambda {
	lambda.handlers = append(lambda.handlers, handler)
	return lambda
}

func (lambda *lambdaImpl) Target(producer mq.Producer) Lambda {
	lambda.targets = append(lambda.targets, producer)
	return lambda
}

func (lambda *lambdaImpl) Start() error {

	if len(lambda.sources) == 0 {
		return xerrors.New("lambda input sources required")
	}

	go lambda.runLoop()

	return nil
}

func (lambda *lambdaImpl) runLoop() {

	sizen := len(lambda.sources)*2 + 1

	selectCase := make([]reflect.SelectCase, sizen)

	selectCase[0].Dir = reflect.SelectRecv
	selectCase[0].Chan = reflect.ValueOf(lambda.ctx.Done())

	for i := 1; i < len(lambda.sources)+1; i++ {
		selectCase[i].Dir = reflect.SelectRecv
		selectCase[i].Chan = reflect.ValueOf(lambda.sources[i-1].Recv())
	}

	for i := len(lambda.sources) + 1; i < sizen; i++ {
		selectCase[i].Dir = reflect.SelectRecv
		selectCase[i].Chan = reflect.ValueOf(lambda.sources[i-1].Errors())
	}

	errorStart := len(lambda.sources) + 1

	for {
	Start:
		chosen, recv, recvOk := reflect.Select(selectCase)

		lambda.TraceF("chosen %d status: %s", chosen, recvOk)

		if !recvOk {
			continue
		}

		if chosen == 0 {
			lambda.InfoF("lambda exit ...")
			return
		}

		if chosen >= errorStart {
			lambda.ErrorF("lambda source error: %s", recv.Interface())
			continue
		}

		record := recv.Interface().(mq.Record)

		key := record.Value()
		buff := record.Value()

		lambda.InfoF("handle record %s", string(key))

		for _, handler := range lambda.handlers {
			var err error

			buff, err = handler(buff)

			if err != nil {
				lambda.ErrorF("lambda handle error: %s", err)
				goto Start
			}
		}

		for _, target := range lambda.targets {
			newRecord, err := target.Record(record.Key(), buff)

			if err != nil {
				lambda.ErrorF("lambda target error: %s", err)
				goto Start
			}

			if err := target.Send(newRecord); err != nil {
				lambda.ErrorF("lambda target error: %s", err)
				goto Start
			}
		}

		if err := lambda.sources[chosen-1].Commit(record); err != nil {
			lambda.ErrorF("lambda source commit error: %s", err)
		}
	}
}
