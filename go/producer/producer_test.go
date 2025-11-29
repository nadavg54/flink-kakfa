package main

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
	testBroker = "localhost:9092"
	testTopic  = "input-words-test"
)

func TestProducer_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ensureTestTopicExists(t)
	writer := createTestKafkaWriter(t)
	defer writer.Close()

	// Try to use consumer group, but fall back to direct partition reading if coordinator isn't ready
	reader := createTestKafkaReaderWithFallback(t)
	defer reader.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testMessages := []string{
		"hello world",
		"flink kafka streaming",
		"data processing pipeline",
	}

	sendTestMessages(t, ctx, writer, testMessages)
	receivedMessages := readMessagesUntilDeadline(t, ctx, reader, len(testMessages), 10*time.Second)
	verifyAllMessagesReceived(t, testMessages, receivedMessages)
}

// ensureTestTopicExists creates the test topic if it doesn't already exist
func ensureTestTopicExists(t *testing.T) {
	conn, err := kafka.Dial("tcp", testBroker)
	require.NoError(t, err, "Failed to connect to Kafka broker")
	defer conn.Close()

	controller, err := conn.Controller()
	require.NoError(t, err, "Failed to get controller")

	controllerConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	require.NoError(t, err, "Failed to connect to controller")
	defer controllerConn.Close()

	topicConfig := kafka.TopicConfig{
		Topic:             testTopic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	err = controllerConn.CreateTopics(topicConfig)
	if err != nil && err.Error() != "topic already exists" {
		t.Logf("Note: Topic creation returned: %v", err)
	}

	// Give Kafka a moment to create the topic
	time.Sleep(1 * time.Second)
}

// createTestKafkaWriter creates a Kafka writer for testing
func createTestKafkaWriter(t *testing.T) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(testBroker),
		Topic:    testTopic,
		Balancer: &kafka.LeastBytes{},
	}
}

// createTestKafkaReaderWithFallback creates a Kafka reader using consumer group
// Falls back to direct partition reading if consumer group coordinator isn't available
func createTestKafkaReaderWithFallback(t *testing.T) *kafka.Reader {
	// First, try to create a reader with consumer group
	groupID := fmt.Sprintf("test-consumer-group-%d", time.Now().UnixNano())
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{testBroker},
		Topic:    testTopic,
		GroupID:  groupID,
		MinBytes: 1,    // Read immediately
		MaxBytes: 10e6, // 10MB
	})

	// Test if consumer group works by trying to read (with short timeout)
	testCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Try to fetch metadata to see if coordinator is available
	_, err := reader.ReadMessage(testCtx)
	if err != nil && isGroupCoordinatorError(err) {
		// Consumer group coordinator not available, fall back to direct partition reading
		t.Logf("Consumer group coordinator not available, using direct partition reading for test")
		reader.Close()
		return createDirectPartitionReader(t)
	}

	// Consumer group works, return the reader
	// Reset the reader to start from beginning
	reader.SetOffsetAt(context.Background(), time.Time{})
	return reader
}

// createDirectPartitionReader creates a reader that reads directly from partition 0
// This bypasses consumer groups and is useful when coordinator isn't ready
func createDirectPartitionReader(t *testing.T) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{testBroker},
		Topic:     testTopic,
		Partition: 0,
		MinBytes:  1,    // Read immediately
		MaxBytes:  10e6, // 10MB
	})
}

// sendTestMessages sends all test messages to Kafka
func sendTestMessages(t *testing.T, ctx context.Context, writer *kafka.Writer, messages []string) {
	for _, msg := range messages {
		err := writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte("test-key"),
			Value: []byte(msg),
		})
		require.NoError(t, err, "Failed to write message: %s", msg)
		t.Logf("Sent message: %s", msg)
	}
}

// readMessagesUntilDeadline reads messages from Kafka until the expected count is reached or deadline expires
func readMessagesUntilDeadline(t *testing.T, ctx context.Context, reader *kafka.Reader, expectedCount int, timeout time.Duration) []string {
	receivedMessages := make([]string, 0, expectedCount)
	deadline := time.Now().Add(timeout)
	retryDelay := 100 * time.Millisecond

	// If reading from partition directly, start from beginning
	// (Consumer groups handle this automatically)
	if reader.Config().Partition >= 0 {
		reader.SetOffset(0)
	}

	for len(receivedMessages) < expectedCount && time.Now().Before(deadline) {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if err == context.DeadlineExceeded || err == context.Canceled {
				break
			}

			// Retry on transient errors
			if time.Now().Before(deadline) {
				time.Sleep(retryDelay)
				continue
			}

			t.Logf("Error reading message: %v", err)
			break
		}
		receivedMessages = append(receivedMessages, string(msg.Value))
		t.Logf("Received message: %s", string(msg.Value))
	}

	return receivedMessages
}

// isGroupCoordinatorError checks if the error is related to group coordinator not being available
func isGroupCoordinatorError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "Group Coordinator Not Available") ||
		strings.Contains(errStr, "group coordinator") ||
		strings.Contains(errStr, "coordinator")
}

// verifyAllMessagesReceived verifies that all sent messages were received
func verifyAllMessagesReceived(t *testing.T, sentMessages []string, receivedMessages []string) {
	assert.GreaterOrEqual(t, len(receivedMessages), len(sentMessages),
		"Should receive at least as many messages as sent")

	for _, sentMsg := range sentMessages {
		assert.Contains(t, receivedMessages, sentMsg, "Message '%s' should be received", sentMsg)
	}
}

func TestGenerateRandomSentence(t *testing.T) {
	producer := NewProducer(Config{})
	sentence := producer.generateRandomSentence()
	assert.NotEmpty(t, sentence, "Generated sentence should not be empty")

	words := strings.Split(sentence, " ")
	assert.GreaterOrEqual(t, len(words), 1, "Sentence should have at least 1 word")
	assert.LessOrEqual(t, len(words), 5, "Sentence should have at most 5 words")

	// Test multiple generations to ensure randomness
	sentences := make(map[string]bool)
	for i := 0; i < 10; i++ {
		s := producer.generateRandomSentence()
		sentences[s] = true
	}
	// With randomness, we should get at least a few different sentences
	assert.Greater(t, len(sentences), 1, "Should generate different sentences")
}
