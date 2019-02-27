package lambda

import (
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"github.com/dynamicgo/mq"
	"github.com/dynamicgo/slf4go"
	"github.com/dynamicgo/xerrors"
)

// Errors .
var (
	ErrParam = errors.New("invalid param")
)

// Lambda .
type Lambda interface {
	Handle(f interface{}) Lambda
	Do(sink mq.Producer)
}

type lambdaFunc struct {
	funcValue reflect.Value
	In        reflect.Type
	Out       reflect.Type
}

func newLambdaFunc(f interface{}) (*lambdaFunc, error) {

	funcType := reflect.TypeOf(f)

	if funcType.Kind() != reflect.Func {
		return nil, xerrors.Wrapf(ErrParam, "lambda handle must be a function,but got %s", funcType.Kind())
	}

	if funcType.NumIn() != 1 {
		return nil, xerrors.Wrapf(ErrParam, "lambda handle type error, expect func(arg)(ret,error) ")
	}

	inType := funcType.In(0)

	if inType.Kind() != reflect.Ptr || inType.Elem().Kind() != reflect.Struct {
		return nil, xerrors.Wrapf(ErrParam, "lambda handle in param must be a point to struct ")
	}

	if funcType.NumOut() != 2 {
		return nil, xerrors.Wrapf(ErrParam, "lambda handle type error, expect func(arg)(ret,error) ")
	}

	if !funcType.Out(1).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		return nil, xerrors.Wrapf(ErrParam, "lambda handle type error, expect func(arg)(ret,error) ")
	}

	outType := funcType.Out(0)

	if outType.Kind() != reflect.Ptr || outType.Elem().Kind() != reflect.Struct {
		return nil, xerrors.Wrapf(ErrParam, "lambda handle out param must be a point to struct ")
	}

	return &lambdaFunc{
		funcValue: reflect.ValueOf(f),
		In:        inType,
		Out:       outType,
	}, nil
}

func (f *lambdaFunc) Next(next interface{}) (*lambdaFunc, error) {

	nextLambda, err := newLambdaFunc(next)

	if err != nil {
		return nil, err
	}

	if nextLambda.In != f.Out {
		return nil, xerrors.Wrapf(ErrParam, "lambda chain handle's in param type must be equal to prev handle's out param type")
	}

	return nextLambda, nil
}

type lambdaImpl struct {
	slf4go.Logger
	chainF   []*lambdaFunc
	consumer mq.Consumer
	sink     mq.Producer
	backoff  time.Duration
}

// Option .
type Option func(impl *lambdaImpl)

// WithBackoff .
func WithBackoff(backoff time.Duration) Option {
	return func(impl *lambdaImpl) {
		impl.backoff = backoff
	}
}

// New .
func New(consumer mq.Consumer, options ...Option) Lambda {
	impl := &lambdaImpl{
		Logger:   slf4go.Get("lambda"),
		consumer: consumer,
		backoff:  time.Second * 5,
	}

	for _, option := range options {
		option(impl)
	}

	return impl
}

func (lambda *lambdaImpl) Handle(f interface{}) Lambda {

	var lambdaF *lambdaFunc
	var err error

	if len(lambda.chainF) == 0 {

		lambdaF, err = newLambdaFunc(f)

		if err != nil {
			panic(err)
		}

	} else {
		lambdaF, err = lambda.chainF[len(lambda.chainF)-1].Next(f)

		if err != nil {
			panic(err)
		}
	}

	lambda.chainF = append(lambda.chainF, lambdaF)

	return lambda
}

func (lambda *lambdaImpl) Do(sink mq.Producer) {
	lambda.sink = sink
	lambda.recvLoop()
}

func (lambda *lambdaImpl) recvLoop() {

	if len(lambda.chainF) == 0 {
		lambda.WarnF("lambda do nothing ...")
		return
	}

	for {
		select {
		case record := <-lambda.consumer.Recv():
			lambda.handleRecord(record)

			if err := lambda.consumer.Commit(record); err != nil {
				lambda.ErrorF("onchain watcher record commit error :%s", err)
			}
		case err := <-lambda.consumer.Errors():
			lambda.ErrorF("onchain watcher error :%s", err)
		}
	}
}

func (lambda *lambdaImpl) handleRecord(record mq.Record) {
	for !lambda.tryOnce(record) {
		lambda.DebugF("retry handle record %s", string(record.Key()))
		time.Sleep(lambda.backoff)
	}
}

func (lambda *lambdaImpl) tryOnce(record mq.Record) bool {
	lambda.DebugF("execute record %s", string(record.Key()))

	value := reflect.New(lambda.chainF[0].In.Elem())

	err := json.Unmarshal(record.Value(), value.Interface())

	if err != nil {
		lambda.ErrorF("decode input record %s error: %s", string(record.Value()), err)
		return true
	}

	for _, f := range lambda.chainF {

		returns := f.funcValue.Call([]reflect.Value{value})

		if !returns[1].IsNil() {
			lambda.ErrorF("lambda execute err: %s", returns[1].Interface())
			return false
		}

		value = returns[0]
	}

	if lambda.sink == nil {
		lambda.DebugF("execute record %s -- complete", string(record.Key()))
		return true
	}

	lambda.DebugF("execute record %s -- write result to sink", string(record.Key()))

	buff, err := json.Marshal(value.Interface())

	if err != nil {
		lambda.DebugF("execute record %s -- write result to sink err: %s", string(record.Key()), err)
		return false
	}

	newRecord, err := lambda.sink.Record(record.Key(), buff)

	if err != nil {
		lambda.DebugF("execute record %s -- write result to sink err: %s", string(record.Key()), err)
		return false
	}

	if err := lambda.sink.Send(newRecord); err != nil {
		lambda.DebugF("execute record %s -- write result to sink err: %s", string(record.Key()), err)
		return false
	}

	lambda.DebugF("execute record %s -- success", string(record.Key()))

	return true
}
