package examples

import (
	"context"
	"os"

	"github.com/isayme/go-amqp-reconnect/rabbitmq"
	"github.com/rs/zerolog"
	pubsub "github.com/soulgarden/rmq-pubsub"
	"github.com/streadway/amqp"
	"golang.org/x/sync/errgroup"
)

type Service struct{}

func (f *Service) Process(
	ctx context.Context,
	delivery <-chan amqp.Delivery,
) {
	select {
	case msg := <-delivery:
		// process

		_ = msg.Ack(false)

		// or _ = msg.Reject(true)
	case <-ctx.Done():
		return
	}
}

func runSub() {
	cfg := pubsub.NewMinimalCfg("queue_name")

	logger := zerolog.New(os.Stdout)

	conn, err := rabbitmq.Dial(
		"amqp://user:pass@127.0.0.1:5672",
	)
	if err != nil {
		logger.Err(err).Msg("dial rmq")

		os.Exit(1)
	}

	pub := pubsub.NewSub(conn, &Service{}, pubsub.NewRmq(conn, cfg, &logger), cfg, &logger)

	g, ctx := errgroup.WithContext(context.Background())

	g.Go(func() error {
		return pub.StartConsumer(ctx)
	})

	if err := g.Wait(); err != nil {
		logger.Err(err).Msg("publisher")

		os.Exit(1)
	}
}
