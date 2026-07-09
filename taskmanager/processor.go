// Tencent is pleased to support the open source community by making trpc-a2a-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-a2a-go is licensed under the Apache License Version 2.0.

package taskmanager

import (
	"context"

	"trpc.group/trpc-go/trpc-a2a-go/v2/protocol"
)

// Processor is a proposed simpler contract for agent logic, aligned with the
// official A2A executors (a2a-python's AgentExecutor.execute, a2a-go's
// AgentExecutor.Execute): the framework hands the round a TaskHandle to emit
// through, and the agent just emits and returns.
//
// Compared with MessageProcessor:
//   - The handle is passed in (framework-owned) instead of the agent building
//     one and returning a channel.
//   - There is no manual Close, no Events(), and no goroutine boilerplate: the
//     round ends when Process returns (the framework closes the stream), and a
//     terminal event (Complete/Fail/...) ends it as soon as it is emitted.
//   - Returning an error fails the round if no terminal state was emitted;
//     to report a business failure prefer emitting one via h.Fail.
//
// It coexists with MessageProcessor for evaluation. AsMessageProcessor bridges
// a Processor onto the current engine; a native implementation would have the
// engine drive Process directly.
type Processor interface {
	Process(ctx context.Context, ec *ExecContext, h *TaskHandle) error
}

// AsMessageProcessor adapts a Processor to the current MessageProcessor
// contract. This is temporary scaffolding over the existing engine: it owns the
// goroutine and the Close so the Processor never has to.
func AsMessageProcessor(p Processor) MessageProcessor {
	return processorAdapter{p: p}
}

type processorAdapter struct{ p Processor }

// ProcessMessage implements MessageProcessor by running the Processor in a
// framework-owned goroutine and closing the stream when it returns.
func (a processorAdapter) ProcessMessage(
	ctx context.Context,
	ec *ExecContext,
) (<-chan protocol.StreamEvent, error) {
	h := NewTaskHandle(ctx, ec)
	go func() {
		// "return == done": the framework closes the round when Process
		// returns; the agent never calls Close.
		defer h.Close()
		if err := a.p.Process(ctx, ec, h); err != nil {
			// A returned error with no terminal state emitted fails the round.
			// If Process already emitted a terminal state, the engine's
			// terminal-immutability discards this.
			_ = h.Fail(ReplyText("processing failed: " + err.Error()))
		}
	}()
	return h.Events(), nil
}
