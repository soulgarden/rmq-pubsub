package pubsub

import "github.com/streadway/amqp"

type Channel interface {
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Qos(prefetchCount, prefetchSize int, global bool) error
	Consume(
		queue, consumer string,
		autoAck, exclusive, noLocal, noWait bool,
		args amqp.Table,
	) (<-chan amqp.Delivery, error)
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
}
