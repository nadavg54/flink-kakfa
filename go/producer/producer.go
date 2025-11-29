package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	defaultBroker = "localhost:9092"
	defaultTopic  = "input-words"
)

var words = []string{
	"hello", "world", "flink", "kafka", "streaming", "processing",
	"data", "pipeline", "real-time", "distributed", "scalable",
	"fault-tolerant", "checkpoint", "recovery", "state", "window",
}

// Config holds the configuration for the Producer
type Config struct {
	Broker   string
	Topic    string
	Interval time.Duration
	WordList []string
}

// Producer handles sending messages to Kafka
type Producer struct {
	config Config
	writer *kafka.Writer
	rand   *rand.Rand
}

// NewProducer creates a new Producer instance
func NewProducer(config Config) *Producer {
	config = applyDefaultConfig(config)
	writer := createKafkaWriter(config)

	return &Producer{
		config: config,
		writer: writer,
		rand:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// applyDefaultConfig applies default values to config fields that are not set
func applyDefaultConfig(config Config) Config {
	if config.Broker == "" {
		config.Broker = defaultBroker
	}
	if config.Topic == "" {
		config.Topic = defaultTopic
	}
	if config.Interval == 0 {
		config.Interval = 2 * time.Second
	}
	if config.WordList == nil {
		config.WordList = words
	}
	return config
}

// createKafkaWriter creates a new Kafka writer with the given configuration
func createKafkaWriter(config Config) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(config.Broker),
		Topic:    config.Topic,
		Balancer: &kafka.LeastBytes{},
	}
}

// Start begins producing messages to Kafka
func (p *Producer) Start(ctx context.Context) error {
	log.Printf("Producer started. Sending words to topic '%s' on broker '%s'",
		p.config.Topic, p.config.Broker)

	ticker := time.NewTicker(p.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := p.sendMessage(ctx); err != nil {
				log.Printf("Error sending message: %v", err)
				// Continue producing even if one message fails
			}
		}
	}
}

// Stop closes the Kafka writer
func (p *Producer) Stop() error {
	return p.writer.Close()
}

// sendMessage generates and sends a single message to Kafka
func (p *Producer) sendMessage(ctx context.Context) error {
	message := p.generateRandomSentence()
	kafkaMessage := p.createKafkaMessage(message)

	if err := p.writer.WriteMessages(ctx, kafkaMessage); err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}

	log.Printf("Sent: %s", message)
	return nil
}

// createKafkaMessage creates a Kafka message with a timestamp-based key
func (p *Producer) createKafkaMessage(value string) kafka.Message {
	key := fmt.Sprintf("key-%d", time.Now().Unix())
	return kafka.Message{
		Key:   []byte(key),
		Value: []byte(value),
	}
}

// generateRandomSentence creates a random sentence from the word list
func (p *Producer) generateRandomSentence() string {
	wordCount := p.selectRandomWordCount()
	selectedWords := p.selectRandomWords(wordCount)
	return strings.Join(selectedWords, " ")
}

// selectRandomWordCount returns a random number between 1 and 5
func (p *Producer) selectRandomWordCount() int {
	return p.rand.Intn(5) + 1
}

// selectRandomWords selects random words from the word list
func (p *Producer) selectRandomWords(count int) []string {
	words := make([]string, count)
	for i := 0; i < count; i++ {
		randomIndex := p.rand.Intn(len(p.config.WordList))
		words[i] = p.config.WordList[randomIndex]
	}
	return words
}
