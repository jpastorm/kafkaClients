package producer


//Producer represents the common interface for Kafka producers
type Producer interface {
	Publish(msg string)
	InitProducer(options ProducerOptions)
}

//ProducerOptions to supply the config for producer initialisation
type ProducerOptions struct {
	Broker string
	Topic  string
}
