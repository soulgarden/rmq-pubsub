package examples

import (
	"context"
	"os"

	"github.com/isayme/go-amqp-reconnect/rabbitmq"
	"github.com/rs/zerolog"
	pubsub "github.com/soulgarden/rmq-pubsub"
	"golang.org/x/sync/errgroup"
)

type Event struct {
	ID    string
	Value string
}

func runPub() {
	cfg := pubsub.NewMinimalCfg("queue_name")

	logger := zerolog.New(os.Stdout)

	conn, err := rabbitmq.Dial(
		"amqp://user:pass@127.0.0.1:5672",
	)
	if err != nil {
		logger.Err(err).Msg("dial rmq")

		os.Exit(1)
	}

	pub := pubsub.NewPub(conn, cfg, pubsub.NewRmq(conn, cfg, &logger), &logger)

	g, ctx := errgroup.WithContext(context.Background())

	g.Go(func() error {
		return pub.StartPublisher(ctx)
	})

	pub.Publish(Event{
		ID:    "1",
		Value: "10",
	})

	if err := g.Wait(); err != nil {
		logger.Err(err).Msg("publisher")

		os.Exit(1)
	}
}
