package pubsub

import (
	"context"

	"github.com/isayme/go-amqp-reconnect/rabbitmq"
	"github.com/rs/zerolog"
	"github.com/streadway/amqp"
)

type Sub interface {
	StartConsumer(ctx context.Context) error
	GetDeliveryChannel() (<-chan amqp.Delivery, error)
}

type sub struct {
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
) *sub {
	return &sub{
		conn:   conn,
		svc:    svc,
		rmq:    rmq,
		cfg:    cfg,
		logger: logger,
	}
}

func (s *sub) StartConsumer(ctx context.Context) error {
	delivery, err := s.GetDeliveryChannel()
	if err != nil {
		s.logger.Err(err).Msg("get delivery channel")

		return err
	}

	return s.svc.Process(ctx, delivery)
}

func (s *sub) GetDeliveryChannel() (<-chan amqp.Delivery, error) {
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

	if s.cfg.ExchangeName != "" {
		err = s.rmq.ExchangeDeclare(consumeCh)
		if err != nil {
			s.logger.Err(err).Str("name", s.cfg.ExchangeName).Msg("declare exchange")

			return nil, err
		}

		err = s.rmq.QueueBind(consumeCh)
		if err != nil {
			s.logger.Err(err).
				Str("queue name", s.cfg.ExchangeName).
				Str("exchange name", s.cfg.ExchangeName).
				Msg("declare exchange")

			return nil, err
		}
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
		s.logger.Err(err).Str("name", s.cfg.QueueName).Msg("start consume")

		return nil, err
	}

	return delivery, err
}
