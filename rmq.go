package pubsub

import (
	"github.com/isayme/go-amqp-reconnect/rabbitmq"
	"github.com/rs/zerolog"
)

type Rmqer interface {
	OpenChannel() (Channel, error)
	QueueDeclare(sendCh Channel) error
}

type Rmq struct {
	rmq    *rabbitmq.Connection
	cfg    *Cfg
	logger *zerolog.Logger
}

func NewRmq(rmq *rabbitmq.Connection, cfg *Cfg, logger *zerolog.Logger) *Rmq {
	return &Rmq{rmq: rmq, cfg: cfg, logger: logger}
}

func (p *Rmq) OpenChannel() (*rabbitmq.Channel, error) {
	sendCh, err := p.rmq.Channel()

	p.logger.Err(err).Msg("open conn channel")

	return sendCh, err
}

func (p *Rmq) QueueDeclare(sendCh *rabbitmq.Channel) error {
	_, err := sendCh.QueueDeclare(p.cfg.QueueName, true, false, false, false, nil)

	p.logger.Err(err).Str("name", p.cfg.QueueName).Msg("declare queue")

	return err
}
