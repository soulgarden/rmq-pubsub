package pubsub

import (
	"context"
	"crypto/tls"
	"errors"
	"testing"

	"github.com/isayme/go-amqp-reconnect/rabbitmq"
	"github.com/rs/zerolog"
	"github.com/streadway/amqp"
)

type FakeChannel struct {
	Error           error
	PublishedNumber int64
}

func (f *FakeChannel) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	return f.Error
}

func (f *FakeChannel) Qos(prefetchCount, prefetchSize int, global bool) error {
	return nil
}

func (f *FakeChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	return nil, nil
}

type FakeRmq struct {
	Ch *FakeChannel
}

func (f *FakeRmq) OpenChannel() (Channel, error) {
	if f.Ch == nil {
		f.Ch = &FakeChannel{}
	}
	return f.Ch, nil
}
func (f *FakeRmq) QueueDeclare(sendCh Channel) error { return nil }

func TestPub_sendToQueue(t *testing.T) {
	t.Parallel()

	type fields struct {
		errorsNumber int64
		events       chan interface{}
		conn         *rabbitmq.Connection
		cfg          *Cfg
		logger       *zerolog.Logger
	}

	type args struct {
		e      interface{}
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
				events:       make(chan interface{}),
				conn: &rabbitmq.Connection{
					Connection: &amqp.Connection{
						Config: amqp.Config{
							TLSClientConfig: &tls.Config{},
						},
					},
				},
				cfg:    NewMinimalCfg("test"),
				logger: &zerolog.Logger{},
			},
			args: args{
				e:      nil,
				sendCh: &FakeChannel{},
			},
			wantErr: false,
		},
		{
			name: "fail",
			fields: fields{
				errorsNumber: 0,
				events:       make(chan interface{}),
				conn: &rabbitmq.Connection{
					Connection: &amqp.Connection{
						Config: amqp.Config{
							TLSClientConfig: &tls.Config{},
						},
					},
				},
				cfg:    NewMinimalCfg("test"),
				logger: &zerolog.Logger{},
			},
			args: args{
				e:      nil,
				sendCh: &FakeChannel{Error: errors.New("test error")},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &Pub{
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
						Config: amqp.Config{
							TLSClientConfig: &tls.Config{},
						},
					},
				},
				rmq:    &FakeRmq{},
				cfg:    NewMinimalCfg("test"),
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

			p := &Pub{
				errorsNumber: tt.fields.errorsNumber,
				events:       tt.fields.events,
				conn:         tt.fields.conn,
				rmq:          tt.fields.rmq,
				cfg:          tt.fields.cfg,
				logger:       tt.fields.logger,
			}

			ctx, cancel := context.WithCancel(tt.args.ctx)
			defer cancel()

			if tt.wantShutdown {
				go func() {
					p.Publish(struct{}{})
					cancel()
				}()
			}

			if err := p.StartPublisher(ctx); (err != nil) != tt.wantErr {
				t.Errorf("StartPublisher() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
