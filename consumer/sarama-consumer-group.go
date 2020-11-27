package consumer

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"
)

// SaramaConsumerGroup type for represent a consumer group based on Sarama
type SaramaConsumerGroup struct {
	consumer      GroupConsumer
	consumerGroup sarama.ConsumerGroup
}

//InitConsumerGroup fo rinitialising the producer object
func (saramaConsumerGroup *SaramaConsumerGroup) InitConsumerGroup(options ConsumerOptions) {

	// Sarama configuration options
	var (
		brokers  = ""
		version  = ""
		group    = ""
		topics   = ""
		assignor = ""
		oldest   = true
		verbose  = false
	)

	flag.StringVar(&brokers, "brokers", "localhost:9092", "Kafka bootstrap brokers to connect to, as a comma separated list")
	flag.StringVar(&group, "group", "2", "Kafka consumer group definition")
	flag.StringVar(&version, "version", "2.1.1", "Kafka cluster version")
	flag.StringVar(&topics, "topics", "testMultiTopic", "Kafka topics to be consumed, as a comma seperated list")
	flag.StringVar(&assignor, "assignor", "roundrobin", "Consumer group partition assignment strategy (range, roundrobin, sticky)")
	flag.BoolVar(&oldest, "oldest", true, "Kafka consumer consume initial offset from oldest")
	flag.BoolVar(&verbose, "verbose", false, "Sarama logging")
	flag.Parse()

	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	// producer config
	config := sarama.NewConfig()
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Return.Errors = true
	config.ClientID = "saramaConsumer"
	config.Net.TLS.Enable = false
	//brokers := []string{options.Broker}

	switch assignor {
	case "sticky":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	case "roundrobin":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	case "range":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	default:
		log.Panicf("Unrecognized consumer group partition assignor: %s", assignor)
	}

	if oldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	/**
	 * Setup a new Sarama consumer group
	 */
	consumer := GroupConsumer{
		ready: make(chan bool),
	}
	client, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), group, config)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}

	saramaConsumerGroup.consumerGroup = client
	saramaConsumerGroup.consumer = consumer
	return
}

// GroupConsumer represents a Sarama consumer group consumer
type GroupConsumer struct {
	ready chan bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *GroupConsumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *GroupConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

//FetchMessage fn for getting messages
func (saramaConsumerGroup *SaramaConsumerGroup) FetchMessage(options ConsumerOptions) {

	ctx, cancel := context.WithCancel(context.Background())

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Println("Checking for messages")
		for {
			if err := saramaConsumerGroup.consumerGroup.Consume(ctx, strings.Split(options.Topic, ","), &saramaConsumerGroup.consumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			saramaConsumerGroup.consumer.ready = make(chan bool)
		}
	}()

	<-saramaConsumerGroup.consumer.ready // Await till the consumer has been set up
	log.Println("Sarama consumer up and running!...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Println("terminating: context cancelled")
	case <-sigterm:
		log.Println("terminating: via signal")
	}
	cancel()
	wg.Wait()
	if err := saramaConsumerGroup.consumerGroup.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *GroupConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29

	for {
		message := <-claim.Messages()
		log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
		//session.MarkMessage(message, "")

		//	if i == 5 {
		session.MarkMessage(message, "")
		session.Commit()
		//	}
	}

	return nil
}
