package pubsub

import (
	"context"
	"sync/atomic"

	"github.com/isayme/go-amqp-reconnect/rabbitmq"
	"github.com/rs/zerolog"
	"github.com/streadway/amqp"
)

type Pub interface {
	Publish(e amqp.Publishing)
	StartPublisher(ctx context.Context) error
}

type pub struct {
	errorsNumber int64
	events       chan amqp.Publishing
	conn         *rabbitmq.Connection
	rmq          Rmqer
	cfg          *Cfg
	logger       *zerolog.Logger
}

func NewPub(conn *rabbitmq.Connection, cfg *Cfg, rmq Rmqer, logger *zerolog.Logger) *pub {
	return &pub{
		errorsNumber: 0,
		events:       make(chan amqp.Publishing, cfg.QueueLen),
		conn:         conn,
		rmq:          rmq,
		cfg:          cfg,
		logger:       logger,
	}
}

func (p *pub) Publish(e amqp.Publishing) {
	p.events <- e
}

func (p *pub) StartPublisher(ctx context.Context) error {
	sendCh, err := p.rmq.OpenChannel()
	if err != nil {
		p.logger.Err(err).Msg("open channel")

		return err
	}

	if p.cfg.ExchangeName != "" {
		err = p.rmq.ExchangeDeclare(sendCh)
		if err != nil {
			p.logger.Err(err).Str("name", p.cfg.ExchangeName).Msg("declare exchange")

			return err
		}
	}

	if p.cfg.QueueName != "" {
		err = p.rmq.QueueDeclare(sendCh)
		if err != nil {
			p.logger.Err(err).Str("name", p.cfg.QueueName).Msg("declare queue")

			return err
		}
	}

	for {
		select {
		case <-ctx.Done():
			p.logger.Warn().Int("count", len(p.events)).Msg("send all remaining events")

			for len(p.events) > 0 {
				_ = p.sendToQueue(<-p.events, sendCh)
			}

			p.logger.Warn().Msg("publisher stopped")

			return nil
		case e := <-p.events:
			err = p.sendToQueue(e, sendCh)
			if err != nil {
				if atomic.LoadInt64(&p.errorsNumber) >= p.cfg.ErrorsThreshold {
					p.logger.Err(err).Interface("events", p.events).Msg("errors threshold reached")

					return err
				}
			} else {
				atomic.StoreInt64(&p.errorsNumber, 0)
			}
		}
	}
}

func (p *pub) dequeue() interface{} {
	return <-p.events
}

func (p *pub) sendToQueue(e amqp.Publishing, sendCh Channel) error {
	err := sendCh.Publish(p.cfg.ExchangeName, p.cfg.QueueName, false, false, e)
	if err != nil {
		atomic.AddInt64(&p.errorsNumber, 1)

		p.logger.Err(err).Str("id", e.MessageId).Msg("publish event")
		p.events <- e

		return err
	}

	return nil
}
