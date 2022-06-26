package pubsub

import (
	"github.com/isayme/go-amqp-reconnect/rabbitmq"
	"github.com/rs/zerolog"
	"github.com/streadway/amqp"
)

type Rmqer interface {
	OpenChannel() (Channel, error)
	QueueDeclare(sendCh Channel) error
	ExchangeDeclare(sendCh Channel) error
	QueueBind(sendCh Channel) error
}

type Rmq struct {
	rmq    *rabbitmq.Connection
	cfg    *Cfg
	logger *zerolog.Logger
}

func NewRmq(rmq *rabbitmq.Connection, cfg *Cfg, logger *zerolog.Logger) *Rmq {
	return &Rmq{rmq: rmq, cfg: cfg, logger: logger}
}

func (r *Rmq) OpenChannel() (Channel, error) {
	sendCh, err := r.rmq.Channel()

	r.logger.Err(err).Msg("open conn channel")

	return sendCh, err
}

func (r *Rmq) QueueDeclare(ch Channel) error {
	var autoDelete bool

	if r.cfg.ExchangeName != "" {
		autoDelete = true
	}

	_, err := ch.QueueDeclare(r.cfg.QueueName, true, autoDelete, false, false, nil)

	r.logger.Err(err).Str("name", r.cfg.QueueName).Msg("declare queue")

	return err
}

func (r *Rmq) ExchangeDeclare(ch Channel) error {
	err := ch.ExchangeDeclare(r.cfg.ExchangeName, amqp.ExchangeFanout, true, false, false, false, nil)

	r.logger.Err(err).Str("name", r.cfg.ExchangeName).Msg("declare exchange")

	return err
}

func (r *Rmq) QueueBind(ch Channel) error {
	err := ch.QueueBind(r.cfg.QueueName, "", r.cfg.ExchangeName, false, nil)

	r.logger.Err(err).
		Str("queue name", r.cfg.QueueName).
		Str("exchange name", r.cfg.ExchangeName).
		Msg("bind queue")

	return err
}
