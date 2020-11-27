package main

import (
	"github.com/akhiljob/kafkaClients/consumer"
	"github.com/akhiljob/kafkaClients/producer"
)

func main() {
	var options producer.ProducerOptions
	options.Broker = "localhost:9092"
	options.Topic = "testMultiTopic"
	saramaProducer := producer.GetNewKafkaProducer(options)
	saramaProducer.Publish("Helllllllllllllooooooooooooooooooooo", options)
	var options2 consumer.ConsumerOptions
	options2.Broker = "localhost:9092"
	options2.Topic = "testTopic"
	// saramaConsumer := consumer.GetNewKafkaConsumer(options2)
	// saramaConsumer.FetchMessage(options2)
	saramaConsumerGroup := consumer.GetNewKafkaConsumerGroup(options2)
	saramaConsumerGroup.FetchMessage(options2)

}
