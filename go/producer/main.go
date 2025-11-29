package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	configPath := flag.String("config", "", "Path to configuration file (optional, can use env vars only)")
	flag.Parse()

	config, err := LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	producer := NewProducer(config)
	defer producer.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down producer...")
		cancel()
	}()

	log.Println("Press Ctrl+C to stop")

	if err := producer.Start(ctx); err != nil && err != context.Canceled {
		log.Fatalf("Producer error: %v", err)
	}
}
