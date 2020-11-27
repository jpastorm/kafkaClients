package consumer

//GetNewKafkaConsumer To get an instance of sarama producer
func GetNewKafkaConsumer(options ConsumerOptions) SaramaConsumer {
	var saramaConsumer SaramaConsumer
	saramaConsumer.InitConsumer(options)
	return saramaConsumer
}

//GetNewKafkaConsumerGroup To get an instance of sarama producer
func GetNewKafkaConsumerGroup(options ConsumerOptions) SaramaConsumerGroup {
	var saramaConsumerGroup SaramaConsumerGroup
	saramaConsumerGroup.InitConsumerGroup(options)
	return saramaConsumerGroup
}
