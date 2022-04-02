package pubsub

import (
	"context"

	"github.com/streadway/amqp"
)

type Service interface {
	process(
		ctx context.Context,
		delivery <-chan amqp.Delivery,
	)
}
