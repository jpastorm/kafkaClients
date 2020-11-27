package producer

import (
	"fmt"
	"log"
	"os"

	"github.com/Shopify/sarama"
)

//SaramaProducer defines a struct for the producer implementation based on shopify sarama
type SaramaProducer struct {
	Producer sarama.SyncProducer
}

//Publish fn for publishing messages to queue
func (saramaProducer SaramaProducer) Publish(msg string, options ProducerOptions) {
	// publish sync
	message := &sarama.ProducerMessage{
		Topic: options.Topic,
		Value: sarama.StringEncoder(msg),
	}
	p, o, err := saramaProducer.Producer.SendMessage(message)
	if err != nil {
		fmt.Println("Error publish: ", err.Error())
	}

	// publish async
	//producer.Input() <- &sarama.ProducerMessage{

	fmt.Println("Partition: ", p)
	fmt.Println("Offset: ", o)

}

//InitProducer fo rinitialising the producer object
func (saramaProducer *SaramaProducer) InitProducer(options ProducerOptions) {
	sarama.Logger = log.New(os.Stdout, "", log.Ltime)

	// producer config
	config := sarama.NewConfig()
	config.Net.TLS.Enable = false
	config.ClientID = "KafkaProducer"
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	brokers := []string{options.Broker}
	kafkaEventClient, _ := sarama.NewClient(brokers, config)
	producer, err := sarama.NewSyncProducerFromClient(kafkaEventClient)
	if err != nil {
		fmt.Printf("error occured while creating Sarama producer")
	}
	saramaProducer.Producer = producer
}
