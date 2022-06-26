//go:build ignore

package examples

import (
	"context"
	"encoding/json"
	"github.com/streadway/amqp"
	"os"
	"time"

	"github.com/isayme/go-amqp-reconnect/rabbitmq"
	"github.com/rs/zerolog"
	uuid "github.com/satori/go.uuid"
	pubsub "github.com/soulgarden/rmq-pubsub"
	"golang.org/x/sync/errgroup"
)

type Event struct {
	ID    string `json:"id"`
	Value string `json:"value"`
}

func runPub() {
	// use non-empty exchange name and non-empty queue name to bind queue on exchange
	// use non-empty exchange name and empty queue name to bind auto-generated auto-deleted queue on exchange
	cfg := pubsub.NewMinimalCfg("queue_exchange", "queue_name")

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

	e := Event{
		ID:    "1",
		Value: "value",
	}

	body, err := json.Marshal(e)
	if err != nil {
		logger.Err(err).Msg("marshal event")

		os.Exit(1)
	}

	pub.Publish(amqp.Publishing{
		ContentType:  "application/json",
		MessageId:    uuid.NewV4().String(),
		DeliveryMode: amqp.Persistent,
		Body:         body,
		Timestamp:    time.Now(),
	})

	if err := g.Wait(); err != nil {
		logger.Err(err).Msg("publisher")

		os.Exit(1)
	}
}
