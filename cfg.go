package pubsub

type Cfg struct {
	ExchangeName    string `json:"exchange_name"`
	QueueName       string `json:"queue_name"`
	QueueLen        int    `json:"queue_len"`
	ErrorsThreshold int64  `json:"errors_threshold"`
	PrefetchCount   int    `json:"prefetch_count"`
	Durable         bool   `json:"durable"`
	Exclusive       bool   `json:"exclusive"`
}

func NewMinimalCfg(exchangeName, queueName string) *Cfg {
	return &Cfg{
		ExchangeName:    exchangeName,
		QueueName:       queueName,
		QueueLen:        DefaultQueueLen,
		ErrorsThreshold: DefaultErrorsThreshold,
		PrefetchCount:   DefaultPrefetchCount,
		Durable:         true,
		Exclusive:       false,
	}
}
