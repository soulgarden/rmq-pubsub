package pubsub

import (
	"context"

	"github.com/isayme/go-amqp-reconnect/rabbitmq"
	"github.com/rs/zerolog"
	"github.com/streadway/amqp"
)

type Sub struct {
	conn   *rabbitmq.Connection
	svc    Service
	rmq    Rmqer
	cfg    *Cfg
	logger *zerolog.Logger
}

func NewSub(
	conn *rabbitmq.Connection,
	svc Service,
	rmq Rmqer,
	cfg *Cfg,
	logger *zerolog.Logger,
) *Sub {
	return &Sub{
		conn:   conn,
		svc:    svc,
		rmq:    rmq,
		cfg:    cfg,
		logger: logger,
	}
}

func (s *Sub) StartConsumer(ctx context.Context) error {
	delivery, err := s.GetDeliveryChannel()
	if err != nil {
		s.logger.Err(err).Msg("consume")

		return err
	}

	s.svc.Process(ctx, delivery)

	return nil
}

func (s *Sub) GetDeliveryChannel() (<-chan amqp.Delivery, error) {
	consumeCh, err := s.rmq.OpenChannel()
	if err != nil {
		s.logger.Err(err).Msg("open conn channel")

		return nil, err
	}

	err = s.rmq.QueueDeclare(consumeCh)
	if err != nil {
		s.logger.Err(err).Str("name", s.cfg.QueueName).Msg("declare queue")

		return nil, err
	}

	_ = consumeCh.Qos(s.cfg.PrefetchCount, 0, false)

	delivery, err := consumeCh.Consume(
		s.cfg.QueueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		s.logger.Err(err).Msg("start consume")

		return nil, err
	}

	return delivery, err
}
