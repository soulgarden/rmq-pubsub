package pubsub

import (
	"context"
	"encoding/json"
	"sync/atomic"

	"github.com/isayme/go-amqp-reconnect/rabbitmq"
	"github.com/rs/zerolog"
	"github.com/streadway/amqp"
)

type Pub struct {
	errorsNumber int64
	events       chan interface{}
	conn         *rabbitmq.Connection
	rmq          Rmqer
	cfg          *Cfg
	logger       *zerolog.Logger
}

func NewPub(conn *rabbitmq.Connection, cfg *Cfg, rmq Rmqer, logger *zerolog.Logger) *Pub {
	return &Pub{
		errorsNumber: 0,
		events:       make(chan interface{}, cfg.QueueLen),
		conn:         conn,
		rmq:          rmq,
		cfg:          cfg,
		logger:       logger,
	}
}

func (p *Pub) Publish(e interface{}) {
	p.events <- e
}

func (p *Pub) StartPublisher(ctx context.Context) error {
	sendCh, err := p.rmq.OpenChannel()
	if err != nil {
		p.logger.Err(err).Msg("open conn channel")

		return err
	}

	err = p.rmq.QueueDeclare(sendCh)
	if err != nil {
		p.logger.Err(err).Str("name", p.cfg.QueueName).Msg("declare queue")

		return err
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

func (p *Pub) dequeue() interface{} {
	return <-p.events
}

func (p *Pub) sendToQueue(e interface{}, sendCh Channel) error {
	body, err := json.Marshal(e)
	if err != nil {
		p.logger.Err(err).Interface("event", e).Msg("marshall event")
		p.events <- e

		return err
	}

	err = sendCh.Publish("", p.cfg.QueueName, false, false, amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: amqp.Persistent,
	})

	if err != nil {
		atomic.AddInt64(&p.errorsNumber, 1)

		p.logger.Err(err).Bytes("event", body).Msg("publish event")
		p.events <- e

		return err
	}

	return nil
}
