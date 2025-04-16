// Tencent is pleased to support the open source community by making tRPC available.
//
// Copyright (C) 2025 THL A29 Limited, a Tencent company.
// All rights reserved.
//
// If you have downloaded a copy of the tRPC source code from Tencent,
// please note that tRPC source code is licensed under the  Apache 2.0 License,
// A copy of the Apache 2.0 License is included in this file.
package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"trpc.group/trpc-go/a2a-go/jsonrpc"
	"trpc.group/trpc-go/a2a-go/taskmanager"
)

// getCurrentTimestamp returns the current time in ISO 8601 format
func getCurrentTimestamp() string {
	return time.Now().UTC().Format(time.RFC3339)
}

// mockTaskManager implements the taskmanager.TaskManager interface for testing.
type mockTaskManager struct {
	mu sync.Mutex
	// Store tasks for basic Get/Cancel simulation
	tasks map[string]*taskmanager.Task

	// Configure responses/behavior for testing
	SendResponse    *taskmanager.Task
	SendError       error
	GetResponse     *taskmanager.Task
	GetError        error
	CancelResponse  *taskmanager.Task
	CancelError     error
	SubscribeEvents []taskmanager.TaskEvent // Events to send for subscription
	SubscribeError  error

	// Push notification fields
	pushNotificationSetResponse *taskmanager.TaskPushNotificationConfig
	pushNotificationSetError    error
	pushNotificationGetResponse *taskmanager.TaskPushNotificationConfig
	pushNotificationGetError    error
}

// newMockTaskManager creates a new mockTaskManager for testing.
func newMockTaskManager() *mockTaskManager {
	return &mockTaskManager{
		tasks: make(map[string]*taskmanager.Task),
	}
}

// OnSendTask implements the TaskManager interface.
func (m *mockTaskManager) OnSendTask(
	ctx context.Context,
	params taskmanager.SendTaskParams,
) (*taskmanager.Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Return configured error if set
	if m.SendError != nil {
		return nil, m.SendError
	}

	// Validate required fields
	if params.ID == "" {
		return nil, jsonrpc.ErrInvalidParams("task ID is required")
	}

	if len(params.Message.Parts) == 0 {
		return nil, jsonrpc.ErrInvalidParams("message must have at least one part")
	}

	// Return configured response if set
	if m.SendResponse != nil {
		// Store for later retrieval
		m.tasks[m.SendResponse.ID] = m.SendResponse
		return m.SendResponse, nil
	}

	// Default behavior: create a simple task
	task := taskmanager.NewTask(params.ID, params.SessionID)
	now := getCurrentTimestamp()
	task.Status = taskmanager.TaskStatus{
		State:     taskmanager.TaskStateSubmitted,
		Timestamp: now,
	}

	// Store for later retrieval
	m.tasks[task.ID] = task
	return task, nil
}

// OnGetTask implements the TaskManager interface.
func (m *mockTaskManager) OnGetTask(
	ctx context.Context, params taskmanager.TaskQueryParams,
) (*taskmanager.Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.GetError != nil {
		return nil, m.GetError
	}

	if m.GetResponse != nil {
		return m.GetResponse, nil
	}

	// Check if task exists
	task, exists := m.tasks[params.ID]
	if !exists {
		return nil, taskmanager.ErrTaskNotFound(params.ID)
	}
	return task, nil
}

// OnCancelTask implements the TaskManager interface.
func (m *mockTaskManager) OnCancelTask(
	ctx context.Context, params taskmanager.TaskIDParams,
) (*taskmanager.Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.CancelError != nil {
		return nil, m.CancelError
	}

	if m.CancelResponse != nil {
		return m.CancelResponse, nil
	}

	// Check if task exists
	task, exists := m.tasks[params.ID]
	if !exists {
		return nil, taskmanager.ErrTaskNotFound(params.ID)
	}

	// Update task status to canceled
	task.Status.State = taskmanager.TaskStateCanceled
	task.Status.Timestamp = getCurrentTimestamp()
	return task, nil
}

// OnSendTaskSubscribe implements the TaskManager interface.
func (m *mockTaskManager) OnSendTaskSubscribe(
	ctx context.Context, params taskmanager.SendTaskParams,
) (<-chan taskmanager.TaskEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.SubscribeError != nil {
		return nil, m.SubscribeError
	}

	// Create a task like OnSendTask would
	task := taskmanager.NewTask(params.ID, params.SessionID)
	task.Status = taskmanager.TaskStatus{
		State:     taskmanager.TaskStateSubmitted,
		Timestamp: getCurrentTimestamp(),
	}

	// Store for later retrieval
	m.tasks[task.ID] = task

	// Create a channel and send events
	eventCh := make(chan taskmanager.TaskEvent, len(m.SubscribeEvents)+1)

	// Send configured events in background
	if len(m.SubscribeEvents) > 0 {
		go func() {
			for _, event := range m.SubscribeEvents {
				select {
				case <-ctx.Done():
					close(eventCh)
					return
				case eventCh <- event:
					// If this is the final event, close the channel
					if event.IsFinal() {
						close(eventCh)
						return
					}
				}
			}
			// If we didn't have a final event, close the channel anyway
			close(eventCh)
		}()
	} else {
		// No events configured, send a default working and completed status
		go func() {
			// Working status
			workingEvent := taskmanager.TaskStatusUpdateEvent{
				ID: params.ID,
				Status: taskmanager.TaskStatus{
					State:     taskmanager.TaskStateWorking,
					Timestamp: getCurrentTimestamp(),
				},
				Final: false,
			}

			// Completed status
			completedEvent := taskmanager.TaskStatusUpdateEvent{
				ID: params.ID,
				Status: taskmanager.TaskStatus{
					State:     taskmanager.TaskStateCompleted,
					Timestamp: getCurrentTimestamp(),
				},
				Final: true,
			}

			select {
			case <-ctx.Done():
				close(eventCh)
				return
			case eventCh <- workingEvent:
				// Continue
			}

			select {
			case <-ctx.Done():
				close(eventCh)
				return
			case eventCh <- completedEvent:
				close(eventCh)
				return
			}
		}()
	}

	return eventCh, nil
}

// OnPushNotificationSet implements the TaskManager interface for push notifications.
func (m *mockTaskManager) OnPushNotificationSet(
	ctx context.Context, params taskmanager.TaskPushNotificationConfig,
) (*taskmanager.TaskPushNotificationConfig, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.pushNotificationSetError != nil {
		return nil, m.pushNotificationSetError
	}

	if m.pushNotificationSetResponse != nil {
		return m.pushNotificationSetResponse, nil
	}

	// Default implementation if response not configured
	return &taskmanager.TaskPushNotificationConfig{
		ID:                     params.ID,
		PushNotificationConfig: params.PushNotificationConfig,
	}, nil
}

// OnPushNotificationGet implements the TaskManager interface for push notifications.
func (m *mockTaskManager) OnPushNotificationGet(
	ctx context.Context, params taskmanager.TaskIDParams,
) (*taskmanager.TaskPushNotificationConfig, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.pushNotificationGetError != nil {
		return nil, m.pushNotificationGetError
	}

	if m.pushNotificationGetResponse != nil {
		return m.pushNotificationGetResponse, nil
	}

	// Default not found response
	return nil, fmt.Errorf("push notification config not found for task %s", params.ID)
}

// OnResubscribe implements the TaskManager interface for resubscribing to task events.
func (m *mockTaskManager) OnResubscribe(
	ctx context.Context, params taskmanager.TaskIDParams,
) (<-chan taskmanager.TaskEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.SubscribeError != nil {
		return nil, m.SubscribeError
	}

	// Check if task exists
	_, exists := m.tasks[params.ID]
	if !exists {
		return nil, taskmanager.ErrTaskNotFound(params.ID)
	}

	// Create a channel and send events
	eventCh := make(chan taskmanager.TaskEvent, len(m.SubscribeEvents)+1)

	// Send configured events in background
	if len(m.SubscribeEvents) > 0 {
		go func() {
			for _, event := range m.SubscribeEvents {
				select {
				case <-ctx.Done():
					close(eventCh)
					return
				case eventCh <- event:
					// If this is the final event, close the channel
					if event.IsFinal() {
						close(eventCh)
						return
					}
				}
			}
			// If we didn't have a final event, close the channel anyway
			close(eventCh)
		}()
	} else {
		// No events configured, send a default completed status
		go func() {
			completedEvent := taskmanager.TaskStatusUpdateEvent{
				ID: params.ID,
				Status: taskmanager.TaskStatus{
					State:     taskmanager.TaskStateCompleted,
					Timestamp: getCurrentTimestamp(),
				},
				Final: true,
			}

			select {
			case <-ctx.Done():
				close(eventCh)
				return
			case eventCh <- completedEvent:
				close(eventCh)
				return
			}
		}()
	}

	return eventCh, nil
}

// ProcessTask is a helper method for tests that need to process a task directly.
func (m *mockTaskManager) ProcessTask(
	ctx context.Context, taskID string, msg taskmanager.Message,
) (*taskmanager.Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if task exists
	task, exists := m.tasks[taskID]
	if !exists {
		return nil, taskmanager.ErrTaskNotFound(taskID)
	}

	// Update task status to working
	task.Status.State = taskmanager.TaskStateWorking
	task.Status.Timestamp = getCurrentTimestamp()

	// Add message to history if it exists
	if task.History == nil {
		task.History = make([]taskmanager.Message, 0)
	}
	task.History = append(task.History, msg)

	return task, nil
}
