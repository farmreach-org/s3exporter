package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"sava-s3-export/internal/config"
	"sava-s3-export/internal/syncer"
)

func main() {
	// Load configuration
	cfg := config.Load()

	// Create a new syncer
	s, err := syncer.NewSyncer(cfg)
	if err != nil {
		log.Fatalf("Failed to create syncer: %v", err)
	}

	// Create a context that is canceled on interruption
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up a channel to listen for OS signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Run the syncer in a separate goroutine
	go func() {
		if err := s.Run(ctx); err != nil {
			log.Printf("Syncer finished with an error: %v", err)
		}
		cancel() // Cancel the context when the syncer is done
	}()

	// Wait for an interruption signal or for the context to be canceled
	select {
	case <-sigChan:
		log.Println("Received interrupt signal, shutting down...")
		cancel()
	case <-ctx.Done():
		log.Println("Syncer has completed its work.")
	}

	log.Println("Application has shut down.")
}