// Tencent is pleased to support the open source community by making tRPC available.
//
// Copyright (C) 2025 THL A29 Limited, a Tencent company.
// All rights reserved.
//
// If you have downloaded a copy of the tRPC source code from Tencent,
// please note that tRPC source code is licensed under the  Apache 2.0 License,
// A copy of the Apache 2.0 License is included in this file.

// Package client provides a basic client implementation for the A2A protocol.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"trpc.group/trpc-go/a2a-go/internal/sse"
	"trpc.group/trpc-go/a2a-go/jsonrpc"
	"trpc.group/trpc-go/a2a-go/log"
	"trpc.group/trpc-go/a2a-go/protocol"
	"trpc.group/trpc-go/a2a-go/taskmanager"
)

const (
	defaultTimeout   = 60 * time.Second
	defaultUserAgent = "a2a-go-client/0.1"
)

// A2AClient provides methods to interact with an A2A agent server.
// It handles making HTTP requests and encoding/decoding JSON-RPC messages.
type A2AClient struct {
	baseURL    *url.URL     // Parsed base URL of the agent server.
	httpClient *http.Client // Underlying HTTP client.
	userAgent  string       // User-Agent header string.
}

// NewA2AClient creates a new A2A client targeting the specified agentURL.
// The agentURL should be the base endpoint for the agent (e.g., "http://localhost:8080/").
// Options can be provided to configure the client, such as setting a custom
// http.Client or timeout.
// Returns an error if the agentURL is invalid.
func NewA2AClient(agentURL string, opts ...Option) (*A2AClient, error) {
	if !strings.HasSuffix(agentURL, "/") {
		agentURL += "/" // Ensure base URL ends with a slash for correct path joining.
	}
	parsedURL, err := url.ParseRequestURI(agentURL)
	if err != nil {
		return nil, fmt.Errorf("invalid agent URL %q: %w", agentURL, err)
	}
	client := &A2AClient{
		baseURL: parsedURL,
		httpClient: &http.Client{
			Timeout: defaultTimeout,
		},
		userAgent: defaultUserAgent,
	}
	// Apply functional options.
	for _, opt := range opts {
		opt(client)
	}
	return client, nil
}

// SendTasks sends a message using the tasks/send method.
// It returns the initial task state received from the agent.
func (c *A2AClient) SendTasks(
	ctx context.Context,
	params taskmanager.SendTaskParams,
) (*taskmanager.Task, error) {
	request := jsonrpc.NewRequest(protocol.MethodTasksSend, params.ID)
	paramsBytes, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("a2aClient.SendTasks: failed to marshal params: %w", err)
	}
	request.Params = paramsBytes
	// Execute the request and decode the result field directly into task.
	task, err := c.doRequestAndDecodeTask(ctx, request)
	if err != nil {
		// Return error, potentially wrapping a *jsonrpc.JSONRPCError.
		return nil, fmt.Errorf("a2aClient.SendTasks: %w", err)
	}
	return task, nil
}

// GetTasks retrieves the status of a task using the tasks_get method.
func (c *A2AClient) GetTasks(
	ctx context.Context,
	params taskmanager.TaskQueryParams,
) (*taskmanager.Task, error) {
	request := jsonrpc.NewRequest(protocol.MethodTasksGet, params.ID)
	paramsBytes, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("a2aClient.GetTasks: failed to marshal params: %w", err)
	}
	request.Params = paramsBytes
	task, err := c.doRequestAndDecodeTask(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("a2aClient.GetTasks: %w", err)
	}
	return task, nil
}

// CancelTasks cancels an in-progress task using the tasks/cancel method.
// It returns the task state immediately after the cancellation request.
func (c *A2AClient) CancelTasks(
	ctx context.Context,
	params taskmanager.TaskIDParams,
) (*taskmanager.Task, error) {
	request := jsonrpc.NewRequest(protocol.MethodTasksCancel, params.ID)
	paramsBytes, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("a2aClient.CancelTasks: failed to marshal params: %w", err)
	}
	request.Params = paramsBytes
	task, err := c.doRequestAndDecodeTask(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("a2aClient.CancelTasks: %w", err)
	}
	return task, nil
}

// StreamTask sends a message using tasks_sendSubscribe and returns a channel for receiving SSE events.
// It handles setting up the SSE connection and parsing events.
// The returned channel will be closed when the stream ends (task completion, error, or context cancellation).
func (c *A2AClient) StreamTask(
	ctx context.Context,
	params taskmanager.SendTaskParams,
) (<-chan taskmanager.TaskEvent, error) {
	// Create the JSON-RPC request.
	request := jsonrpc.NewRequest(protocol.MethodTasksSendSubscribe, params.ID)
	paramsBytes, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("a2aClient.StreamTask: failed to marshal params: %w", err)
	}
	request.Params = paramsBytes
	reqBody, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("a2aClient.StreamTask: failed to marshal request body: %w", err)
	}
	// Construct the target URL.
	targetURL := c.baseURL.String()
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		targetURL,
		bytes.NewReader(reqBody),
	)
	if err != nil {
		return nil, fmt.Errorf("a2aClient.StreamTask: failed to create http request: %w", err)
	}
	// Set headers, including Accept for event stream.
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("Accept", "text/event-stream") // Crucial for SSE.
	if c.userAgent != "" {
		req.Header.Set("User-Agent", c.userAgent)
	}
	log.Debugf("A2A Client Stream Request -> Method: %s, ID: %v, URL: %s", request.Method, request.ID, targetURL)
	// Make the initial request to establish the stream.
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("a2aClient.StreamTask: http request failed: %w", err)
	}
	// Check for non-success HTTP status codes.
	// For SSE, a successful setup should result in 200 OK.
	if resp.StatusCode != http.StatusOK {
		// Read body for error details if possible.
		bodyBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf(
			"a2aClient.StreamTask: unexpected http status %d establishing stream: %s",
			resp.StatusCode, string(bodyBytes),
		)
	}
	// Check if the response is actually an event stream.
	if !strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
		resp.Body.Close()
		return nil, fmt.Errorf(
			"a2aClient.StreamTask: server did not respond with Content-Type 'text/event-stream', got %s",
			resp.Header.Get("Content-Type"),
		)
	}
	log.Debugf("A2A Client Stream Response <- Status: %d, ID: %v. Stream established.", resp.StatusCode, request.ID)
	// Create the channel to send events back to the caller.
	eventsChan := make(chan taskmanager.TaskEvent, 10) // Buffered channel.
	// Start a goroutine to read from the SSE stream.
	go c.processSSEStream(ctx, resp, params.ID, eventsChan)
	return eventsChan, nil
}

// processSSEStream reads Server-Sent Events from the response body and sends them
// onto the provided channel. It handles closing the channel and response body.
// Runs in its own goroutine.
func (c *A2AClient) processSSEStream(
	ctx context.Context,
	resp *http.Response,
	taskID string,
	eventsChan chan<- taskmanager.TaskEvent,
) {
	// Ensure resources are cleaned up when the goroutine exits.
	defer resp.Body.Close()
	defer close(eventsChan)
	reader := sse.NewEventReader(resp.Body)
	log.Debugf("SSE Processor started for task %s", taskID)
	for {
		select {
		case <-ctx.Done():
			// Context canceled (e.g., timeout or manual cancellation by caller).
			log.Debugf("SSE context canceled for task %s: %v", taskID, ctx.Err())
			return
		default:
			// Read the next event from the stream.
			eventBytes, eventType, err := reader.ReadEvent()
			if err != nil {
				if err == io.EOF {
					log.Debugf("SSE stream ended cleanly (EOF) for task %s", taskID)
				} else {
					// Log unexpected errors (like network issues or parsing problems).
					log.Errorf("Error reading SSE stream for task %s: %v", taskID, err)
				}
				return // Stop processing on any error or EOF.
			}
			// Skip comments or events without data.
			if len(eventBytes) == 0 {
				continue
			}
			// Handle close event immediately before any other processing.
			if eventType == protocol.EventClose {
				log.Debugf(
					"Received explicit '%s' event from server for task %s. Data: %s",
					protocol.EventClose, taskID, string(eventBytes),
				)
				return // Exit immediately, do not process any more events
			}
			// Deserialize the event data based on the event type from SSE.
			var taskEvent taskmanager.TaskEvent
			switch eventType {
			case protocol.EventTaskStatusUpdate:
				var statusEvent taskmanager.TaskStatusUpdateEvent
				if err := json.Unmarshal(eventBytes, &statusEvent); err != nil {
					log.Errorf(
						"Error unmarshaling TaskStatusUpdateEvent for task %s: %v. Data: %s",
						taskID, err, string(eventBytes),
					)
					continue // Skip malformed event.
				}
				taskEvent = statusEvent
			case protocol.EventTaskArtifactUpdate:
				var artifactEvent taskmanager.TaskArtifactUpdateEvent
				if err := json.Unmarshal(eventBytes, &artifactEvent); err != nil {
					log.Errorf(
						"Error unmarshaling TaskArtifactUpdateEvent for task %s: %v. Data: %s",
						taskID, err, string(eventBytes),
					)
					continue // Skip malformed event.
				}
				taskEvent = artifactEvent
			default:
				log.Warnf(
					"Received unknown SSE event type '%s' for task %s. Data: %s",
					eventType, taskID, string(eventBytes),
				)
				continue // Skip unknown event types.
			}
			// Send the deserialized event to the caller's channel.
			// Use a select to avoid blocking if the caller isn't reading fast enough
			// or if the context was canceled concurrently.
			select {
			case eventsChan <- taskEvent:
				// Event sent successfully.
			case <-ctx.Done():
				log.Debugf(
					"SSE context canceled while sending event for task %s: %v",
					taskID, ctx.Err(),
				)
				return // Stop processing.
			}
		}
	}
}

func (c *A2AClient) doRequestAndDecodeTask(
	ctx context.Context,
	request *jsonrpc.Request,
) (*taskmanager.Task, error) {
	// Perform the HTTP request and basic JSON unmarshaling into fullResponse.
	fullResponse, err := c.doRequest(ctx, request)
	if err != nil {
		return nil, err // Error is already contextualized by doRequest.
	}
	// Check for JSON-RPC level error included in the response.
	if fullResponse.Error != nil {
		return nil, fullResponse.Error // Return the specific JSONRPCError.
	}
	// Check if the result field is missing (and not null).
	if len(fullResponse.Result) == 0 {
		// Allow empty/null result only if responseTarget is nil interface or pointer to nil.
		// This is tricky to check reliably. A missing result is generally an error for non-notification calls.
		return nil, fmt.Errorf("rpc response missing required 'result' field for id %v", request.ID)
	}
	// Unmarshal the raw JSON 'result' field directly into the specific target structure provided by the caller.
	task := &taskmanager.Task{}
	if err := json.Unmarshal(fullResponse.Result, task); err != nil {
		return nil, fmt.Errorf(
			"failed to unmarshal rpc result: %w. Raw result: %s", err, string(fullResponse.Result),
		)
	}
	return task, nil
}

// doRequest performs the HTTP POST request for a JSON-RPC call.
// It handles request marshaling, setting headers, sending the request,
// checking the HTTP status, and decoding the base JSON response structure.
// It does NOT specifically handle the 'result' or 'error' fields, leaving that
// to the caller or doRequestAndDecodeResult.
func (c *A2AClient) doRequest(
	ctx context.Context, request *jsonrpc.Request,
) (*jsonrpc.RawResponse, error) {
	reqBody, err := json.Marshal(request)
	if err != nil {
		// Use a more specific error message prefix.
		return nil, fmt.Errorf("a2aClient.doRequest: failed to marshal request: %w", err)
	}
	// Construct the target URL using the base URL.
	// Assume the RPC endpoint is at the root of the baseURL.
	targetURL := c.baseURL.String()
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		targetURL,
		bytes.NewReader(reqBody),
	)
	if err != nil {
		return nil, fmt.Errorf("a2aClient.doRequest: failed to create http request: %w", err)
	}
	// Set required headers.
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("Accept", "application/json")
	if c.userAgent != "" {
		req.Header.Set("User-Agent", c.userAgent)
	}
	log.Debugf("A2A Client Request -> Method: %s, ID: %v, URL: %s", request.Method, request.ID, targetURL)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("a2aClient.doRequest: http request failed: %w", err)
	}
	// Ensure body is always closed.
	defer resp.Body.Close()
	// Read the body first for potential error reporting.
	respBodyBytes, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		log.Warnf(
			"Warning: a2aClient.doRequest: failed to read response body (status %d): %v",
			resp.StatusCode, readErr,
		)
		// Continue to check status code, but decoding will likely fail.
	}
	log.Debugf("A2A Client Response <- Status: %d, ID: %v", resp.StatusCode, request.ID)
	// Check for non-success HTTP status codes. This is separate from JSON-RPC errors.
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf(
			"a2aClient.doRequest: unexpected http status %d: %s",
			resp.StatusCode, string(respBodyBytes),
		)
	}
	response := &jsonrpc.RawResponse{}
	// Decode the full JSON response body into the provided target.
	if err := json.Unmarshal(respBodyBytes, response); err != nil {
		// Provide more context in the decode error message.
		return nil, fmt.Errorf(
			"a2aClient.doRequest: failed to decode response body (status %d): %w. Body: %s",
			resp.StatusCode, err, string(respBodyBytes),
		)
	}
	return response, nil
}
