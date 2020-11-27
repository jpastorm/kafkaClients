package consumer

//Consumer represents the common interface for Kafka consumers
type Consumer interface {
	FetchMessage()
	InitConsumer(options ConsumerOptions)
}

//ConsumerOptions to supply the config for consumer initialisation
type ConsumerOptions struct {
	Broker string
	Topic  string
}
