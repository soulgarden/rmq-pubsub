package pubsub

import (
	"context"
	"testing"

	"github.com/isayme/go-amqp-reconnect/rabbitmq"
	"github.com/rs/zerolog"
	"github.com/streadway/amqp"
)

type FakeService struct{}

func (f *FakeService) Process(
	ctx context.Context,
	delivery <-chan amqp.Delivery,
) {
	select {
	case msg := <-delivery:
		_ = msg.Ack(false)
	case <-ctx.Done():
		return
	}
}

func TestSub_StartConsumer(t *testing.T) {
	t.Parallel()

	type fields struct {
		conn   *rabbitmq.Connection
		svc    Service
		rmq    Rmqer
		cfg    *Cfg
		logger *zerolog.Logger
	}

	type args struct {
		ctx context.Context
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
				conn: &rabbitmq.Connection{
					Connection: &amqp.Connection{
						Config: amqp.Config{},
					},
				},
				svc:    &FakeService{},
				rmq:    NewFakeRmq(),
				cfg:    NewMinimalCfg("exchange", "queue"),
				logger: &zerolog.Logger{},
			},
			args: args{
				ctx: context.Background(),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := NewSub(tt.fields.conn, tt.fields.svc, tt.fields.rmq, tt.fields.cfg, tt.fields.logger)

			ch, err := tt.fields.rmq.OpenChannel()
			if err != nil {
				t.Errorf("open channel error: %v", err)
			}

			err = ch.Publish("", "", false, false, amqp.Publishing{})
			if err != nil {
				t.Errorf("publish error: %v", err)
			}

			if err := s.StartConsumer(tt.args.ctx); (err != nil) != tt.wantErr {
				t.Errorf("StartConsumer() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
