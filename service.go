package pubsub

import (
	"context"

	"github.com/streadway/amqp"
)

type Service interface {
	Process(
		ctx context.Context,
		delivery <-chan amqp.Delivery,
	)
}
