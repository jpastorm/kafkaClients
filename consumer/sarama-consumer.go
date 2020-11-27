package consumer

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/Shopify/sarama"
)

//SaramaProducer defines a struct for the producer implementation based on shopify sarama
type SaramaConsumer struct {
	Consumer sarama.Consumer
}

//FetchMessage fn for getting messages
func (saramaConsumer SaramaConsumer) FetchMessage(options ConsumerOptions) {
	consumerPartition, err := saramaConsumer.Consumer.ConsumePartition(options.Topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}
	// Get signal for finish
	doneCh := make(chan struct{})
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	// Count how many message processed
	msgCount := 0
	go func() {
		for {
			select {
			case err := <-consumerPartition.Errors():
				fmt.Println(err)
			case msg := <-consumerPartition.Messages():
				msgCount++
				fmt.Println("Received messages", string(msg.Key), string(msg.Value))

			case <-signals:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()
	<-doneCh

	fmt.Println("Processed", msgCount, "messages")

}

//InitConsumer fo rinitialising the producer object
func (saramaConsumer *SaramaConsumer) InitConsumer(options ConsumerOptions) {
	sarama.Logger = log.New(os.Stdout, "", log.Ltime)

	// producer config
	config := sarama.NewConfig()
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.CommitInterval = 1 * time.Second
	config.Consumer.Return.Errors = true
	config.ClientID = "kafkaConsumer"
	config.Net.TLS.Enable = false
	brokers := []string{options.Broker}
	// Create new consumer
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}

	if err != nil {
		fmt.Printf("error occured while creating Sarama consumer")
	}
	saramaConsumer.Consumer = consumer
}
