package pubsub

import (
	"context"
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
	"github.com/soulgarden/go-amqp-reconnect/rabbitmq"
)

type FakeChannel struct {
	Error           error
	PublishedNumber int64
	ch              chan amqp.Delivery
}

func NewFakeChannel() *FakeChannel {
	return &FakeChannel{
		ch: make(chan amqp.Delivery, 1),
	}
}

func (f *FakeChannel) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	if f.Error == nil {
		f.ch <- amqp.Delivery{}
	}

	return f.Error
}

func (f *FakeChannel) Qos(prefetchCount, prefetchSize int, global bool) error { return nil }

func (f *FakeChannel) Consume(
	queue, consumer string,
	autoAck, exclusive, noLocal, noWait bool,
	args amqp.Table,
) (<-chan amqp.Delivery, error) {
	return f.ch, nil
}

func (f *FakeChannel) QueueDeclare(
	name string,
	durable bool,
	autoDelete bool,
	exclusive bool,
	noWait bool,
	args amqp.Table,
) (amqp.Queue, error) {
	return amqp.Queue{}, nil
}

func (f *FakeChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	return nil
}

func (f *FakeChannel) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	return nil
}

type FakeRmq struct {
	ch *FakeChannel
}

func NewFakeRmq() *FakeRmq { return &FakeRmq{ch: NewFakeChannel()} }

func (f *FakeRmq) OpenChannel() (Channel, error)        { return f.ch, nil }
func (f *FakeRmq) QueueDeclare(sendCh Channel) error    { return nil }
func (f *FakeRmq) ExchangeDeclare(sendCh Channel) error { return nil }
func (f *FakeRmq) QueueBind(sendCh Channel) error       { return nil }

func TestPub_sendToQueue(t *testing.T) {
	t.Parallel()

	type fields struct {
		errorsNumber int64
		events       chan amqp.Publishing
		conn         *rabbitmq.Connection
		cfg          *Cfg
		logger       *zerolog.Logger
	}

	type args struct {
		e      amqp.Publishing
		sendCh Channel
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "success",
			fields: fields{
				errorsNumber: 0,
				events:       make(chan amqp.Publishing),
				conn: &rabbitmq.Connection{
					Connection: &amqp.Connection{
						Config: amqp.Config{},
					},
				},
				cfg:    NewMinimalCfg("exchange", "queue"),
				logger: &zerolog.Logger{},
			},
			args: args{
				e:      amqp.Publishing{},
				sendCh: NewFakeChannel(),
			},
			wantErr: false,
		},
		{
			name: "fail",
			fields: fields{
				errorsNumber: 0,
				events:       make(chan amqp.Publishing),
				conn: &rabbitmq.Connection{
					Connection: &amqp.Connection{
						Config: amqp.Config{},
					},
				},
				cfg:    NewMinimalCfg("exchange", "queue"),
				logger: &zerolog.Logger{},
			},
			args: args{
				e:      amqp.Publishing{},
				sendCh: &FakeChannel{Error: errors.New("test error")},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &pub{
				errorsNumber: tt.fields.errorsNumber,
				events:       tt.fields.events,
				conn:         tt.fields.conn,
				cfg:          tt.fields.cfg,
				logger:       tt.fields.logger,
			}

			go func() {
				_ = p.dequeue()
			}()

			if err := p.sendToQueue(tt.args.e, tt.args.sendCh); (err != nil) != tt.wantErr {
				t.Errorf("sendToQueue() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestPub_StartPublisher(t *testing.T) {
	t.Parallel()

	type fields struct {
		errorsNumber int64
		events       chan interface{}
		conn         *rabbitmq.Connection
		rmq          Rmqer
		cfg          *Cfg
		logger       *zerolog.Logger
	}

	type args struct {
		ctx context.Context
	}

	tests := []struct {
		name         string
		fields       fields
		args         args
		wantErr      bool
		wantShutdown bool
	}{
		{
			name: "shutdown",
			fields: fields{
				errorsNumber: 0,
				events:       make(chan interface{}),
				conn: &rabbitmq.Connection{
					Connection: &amqp.Connection{
						Config: amqp.Config{},
					},
				},
				rmq:    NewFakeRmq(),
				cfg:    NewMinimalCfg("exchange", "queue"),
				logger: &zerolog.Logger{},
			},
			args: args{
				ctx: context.Background(),
			},
			wantErr:      false,
			wantShutdown: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := NewPub(tt.fields.conn, tt.fields.cfg, tt.fields.rmq, tt.fields.logger)

			ctx, cancel := context.WithCancel(tt.args.ctx)
			defer cancel()

			if tt.wantShutdown {
				go func() {
					p.Publish(amqp.Publishing{})
					cancel()
				}()
			}

			if err := p.StartPublisher(ctx); (err != nil) != tt.wantErr {
				t.Errorf("StartPublisher() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
