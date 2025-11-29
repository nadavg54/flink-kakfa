package integration_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testBroker     = "localhost:9092"
	testInputTopic = "input-words-test"
)

func TestProducerConsumer_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Setup topic
	ensureTestTopicExists(t, testInputTopic)

	// Create producer writer
	producerWriter := createTestProducerWriter(t)
	defer producerWriter.Close()

	// Create consumer reader
	consumerReader := createTestConsumerReader(t)
	defer consumerReader.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Test messages to send
	testMessages := []string{
		"hello world",
		"flink kafka streaming",
		"data processing pipeline",
	}

	// Send messages via producer
	sendTestMessages(t, ctx, producerWriter, testMessages)

	// Give Kafka a moment to make messages available
	time.Sleep(1 * time.Second)

	// Read messages via consumer
	receivedMessages := readMessagesUntilCount(t, ctx, consumerReader, len(testMessages), 10*time.Second)

	// Verify all messages were received
	assert.GreaterOrEqual(t, len(receivedMessages), len(testMessages),
		"Should receive at least as many messages as sent")

	for _, sentMsg := range testMessages {
		assert.Contains(t, receivedMessages, sentMsg, "Message '%s' should be received", sentMsg)
	}
}

// Helper functions

func ensureTestTopicExists(t *testing.T, topicName string) {
	conn, err := kafka.Dial("tcp", testBroker)
	require.NoError(t, err, "Failed to connect to Kafka broker")
	defer conn.Close()

	controller, err := conn.Controller()
	require.NoError(t, err, "Failed to get controller")

	controllerConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	require.NoError(t, err, "Failed to connect to controller")
	defer controllerConn.Close()

	topicConfig := kafka.TopicConfig{
		Topic:             topicName,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	err = controllerConn.CreateTopics(topicConfig)
	if err != nil && !strings.Contains(err.Error(), "topic already exists") {
		t.Logf("Note: Topic creation returned: %v", err)
	}

	// Give Kafka a moment to create the topic
	time.Sleep(1 * time.Second)
}

func createTestProducerWriter(t *testing.T) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(testBroker),
		Topic:    testInputTopic,
		Balancer: &kafka.LeastBytes{},
	}
}

func createTestConsumerReader(t *testing.T) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{testBroker},
		Topic:    testInputTopic,
		GroupID:  fmt.Sprintf("test-consumer-group-%d", time.Now().UnixNano()),
		MinBytes: 1,    // Read immediately
		MaxBytes: 10e6, // 10MB
	})
}

func sendTestMessages(t *testing.T, ctx context.Context, writer *kafka.Writer, messages []string) {
	for _, msg := range messages {
		err := writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte("test-key"),
			Value: []byte(msg),
		})
		require.NoError(t, err, "Failed to write message: %s", msg)
		t.Logf("Producer sent: %s", msg)
	}
}

func readMessagesUntilCount(t *testing.T, ctx context.Context, reader *kafka.Reader, expectedCount int, timeout time.Duration) []string {
	receivedMessages := make([]string, 0, expectedCount)
	deadline := time.Now().Add(timeout)

	for len(receivedMessages) < expectedCount && time.Now().Before(deadline) {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if err == context.DeadlineExceeded || err == context.Canceled {
				break
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		receivedMessages = append(receivedMessages, string(msg.Value))
		t.Logf("Consumer received: %s", string(msg.Value))
	}

	return receivedMessages
}

