// Tencent is pleased to support the open source community by making trpc-a2a-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-a2a-go is licensed under the Apache License Version 2.0.

// Package main implements a simple A2A server example built on the MessageProcessor
// contract: one code path serves both message/send and message/stream, and the
// framework owns the task lifecycle (creation, persistence, fan-out).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"trpc.group/trpc-go/trpc-a2a-go/v2/log"
	"trpc.group/trpc-go/trpc-a2a-go/v2/protocol"
	"trpc.group/trpc-go/trpc-a2a-go/v2/server"
	"trpc.group/trpc-go/trpc-a2a-go/v2/taskmanager"
	"trpc.group/trpc-go/trpc-a2a-go/v2/taskmanager/memory"
)

// simpleProcessor implements the proposed taskmanager.Processor contract:
// emit through the framework-owned handle and return. No channel to build, no
// goroutine to spawn, no Close to remember.
type simpleProcessor struct{}

// Process reverses the input and completes the task. One code path serves
// message/send and message/stream; the framework owns the round and closes the
// stream when Process returns.
func (p *simpleProcessor) Process(
	ctx context.Context,
	ec *taskmanager.ExecContext,
	h *taskmanager.TaskHandle,
) error {
	text := extractText(ec.Message)
	if text == "" {
		// A pure-message reply: no task comes into existence for this round.
		return h.Reply(taskmanager.ReplyText("input message must contain text."))
	}

	log.Infof("Processing message with input: %s", text)

	// The framework creates the task on this first task event and stamps the IDs.
	h.Working(nil)
	result := reverseString(text)
	h.AddArtifact(*protocol.NewArtifactWithID(
		stringPtr("Reversed Text"),
		stringPtr("The input text reversed"),
		[]*protocol.Part{protocol.NewTextPart(result)},
	), true)

	// A terminal verb ends the round; returning it reads as the conclusion.
	return h.Complete(taskmanager.ReplyText(fmt.Sprintf("Processed result: %s", result)))
}

// extractText extracts the text content from a message.
func extractText(message protocol.Message) string {
	for _, part := range message.Parts {
		if t := part.TextContent(); t != "" {
			return t
		}
	}
	return ""
}

// reverseString reverses a string.
func reverseString(s string) string {
	runes := []rune(s)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

// Helper function to create string pointers.
func stringPtr(s string) *string {
	return &s
}

// Helper function to create bool pointers.
func boolPtr(b bool) *bool {
	return &b
}

func main() {
	// Parse command-line flags.
	host := flag.String("host", "localhost", "Host to listen on")
	port := flag.Int("port", 8080, "Port to listen on")
	flag.Parse()

	// Create the agent card.
	agentCard := server.AgentCard{
		Name:        "Simple A2A Example Server",
		Description: "A simple example A2A server that reverses text",
		URL:         fmt.Sprintf("http://%s:%d/", *host, *port),
		Version:     "1.0.0",
		Provider: &server.AgentProvider{
			Organization: "tRPC-A2A-Go Examples",
			URL:          stringPtr(fmt.Sprintf("http://%s:%d/", *host, *port)),
		},
		Capabilities: server.AgentCapabilities{
			Streaming:              boolPtr(true),
			PushNotifications:      boolPtr(false),
			StateTransitionHistory: boolPtr(true),
		},
		DefaultInputModes:  []string{"text"},
		DefaultOutputModes: []string{"text"},
		Skills: []server.AgentSkill{
			{
				ID:          "text_reversal",
				Name:        "Text Reversal",
				Description: stringPtr("Reverses the input text"),
				Tags:        []string{"text", "processing"},
				Examples:    []string{"Hello, world!"},
				InputModes:  []string{"text"},
				OutputModes: []string{"text"},
			},
		},
	}

	// Create the processor and inject it into a task manager.
	// (redis.NewTaskManager accepts the same MessageProcessor for persistent storage.)
	taskManager, err := memory.NewTaskManager(taskmanager.AsMessageProcessor(&simpleProcessor{}))
	if err != nil {
		log.Fatalf("Failed to create task manager: %v", err)
	}

	// Create the server.
	srv, err := server.NewA2AServer(taskManager, server.WithAgentCard(agentCard))
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Set up a channel to listen for termination signals.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start the server in a goroutine.
	go func() {
		serverAddr := fmt.Sprintf("%s:%d", *host, *port)
		log.Infof("Starting server on %s...", serverAddr)
		if err := srv.Start(serverAddr); err != nil {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// Wait for termination signal.
	sig := <-sigChan
	log.Infof("Received signal %v, shutting down...", sig)
}
