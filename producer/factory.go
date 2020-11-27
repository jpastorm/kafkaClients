package producer

//GetNewKafkaProducer To get an instance of sarama producer
func GetNewKafkaProducer(options ProducerOptions) SaramaProducer {
	var saramaProducer SaramaProducer
	saramaProducer.InitProducer(options)
	return saramaProducer
}
