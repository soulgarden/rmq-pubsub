package pubsub

type Cfg struct {
	QueueName       string
	QueueLen        int
	ErrorsThreshold int64
	PrefetchCount   int
	Durable         bool
	Exclusive       bool
}

func NewMinimalCfg(queueName string) *Cfg {
	return &Cfg{
		QueueName:       queueName,
		QueueLen:        DefaultQueueLen,
		ErrorsThreshold: DefaultErrorsThreshold,
		PrefetchCount:   DefaultPrefetchCount,
		Durable:         true,
		Exclusive:       false,
	}
}
