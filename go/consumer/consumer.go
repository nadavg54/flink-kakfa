package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

const (
	defaultBroker = "localhost:9092"
	defaultTopic  = "word-count-output"
)

// Config holds the configuration for the Consumer
type Config struct {
	Broker  string
	Topic   string
	GroupID string
}

// Consumer handles reading messages from Kafka
type Consumer struct {
	config Config
	reader *kafka.Reader
}

// NewConsumer creates a new Consumer instance
func NewConsumer(config Config) *Consumer {
	config = applyDefaultConfig(config)
	reader := createKafkaReader(config)

	return &Consumer{
		config: config,
		reader: reader,
	}
}

// Start begins consuming messages from Kafka
func (c *Consumer) Start(ctx context.Context) error {
	log.Printf("Consumer started. Reading from topic '%s' on broker '%s' (group: %s)",
		c.config.Topic, c.config.Broker, c.config.GroupID)
	log.Println("Press Ctrl+C to stop")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := c.readAndProcessMessage(ctx); err != nil {
				if err == context.Canceled || err == context.DeadlineExceeded {
					return err
				}
				log.Printf("Error reading message: %v", err)
				continue
			}
		}
	}
}

// Stop closes the Kafka reader
func (c *Consumer) Stop() error {
	return c.reader.Close()
}

// readAndProcessMessage reads a single message from Kafka and processes it
func (c *Consumer) readAndProcessMessage(ctx context.Context) error {
	message, err := c.reader.ReadMessage(ctx)
	if err != nil {
		return fmt.Errorf("failed to read message: %w", err)
	}

	c.processMessage(message)
	return nil
}

// processMessage processes a single Kafka message
func (c *Consumer) processMessage(message kafka.Message) {
	log.Printf("Received: %s", string(message.Value))
}

// applyDefaultConfig applies default values to config fields that are not set
func applyDefaultConfig(config Config) Config {
	if config.Broker == "" {
		config.Broker = defaultBroker
	}
	if config.Topic == "" {
		config.Topic = defaultTopic
	}
	if config.GroupID == "" {
		config.GroupID = "word-count-consumer-group"
	}
	return config
}

// createKafkaReader creates a new Kafka reader with the given configuration
func createKafkaReader(config Config) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{config.Broker},
		Topic:    config.Topic,
		GroupID:  config.GroupID,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
}

