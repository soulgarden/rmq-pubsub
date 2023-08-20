package pubsub

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Service interface {
	Process(
		ctx context.Context,
		delivery <-chan amqp.Delivery,
	) error
}
