// Package saga provides a small, dependency-free saga orchestrator for Go.
//
// A saga is an ordered sequence of steps, each paired with a compensating
// action. The orchestrator runs the steps in order; if any step fails, it runs
// the compensations for the already-completed steps in reverse order, leaving
// the system in a consistent state. This is the standard pattern for managing
// distributed transactions and long-running workflows where a single ACID
// transaction is impossible — for example provisioning a network, then a VM,
// then a billing record across separate services.
//
// The package is deliberately transport-agnostic. It does not import Kafka,
// HTTP, or a database. Instead it emits an [Event] for every state transition
// through an observer callback, so the caller can publish those events to a
// message bus, persist them for crash recovery, or stream them to a UI.
//
// A built [Saga] is immutable, so a single definition may be executed
// concurrently from many goroutines; each Execute call carries its own payload
// and rollback state. (The observer, if set, must itself be safe for concurrent
// use.)
package saga

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// EventType enumerates the lifecycle transitions an orchestrator emits.
type EventType string

const (
	StepStarted           EventType = "step_started"
	StepCompleted         EventType = "step_completed"
	StepFailed            EventType = "step_failed"
	CompensationStarted   EventType = "compensation_started"
	CompensationCompleted EventType = "compensation_completed"
	CompensationFailed    EventType = "compensation_failed"
	SagaCompleted         EventType = "saga_completed"
	SagaAborted           EventType = "saga_aborted"
)

// Event describes a single state transition during execution. Emit these to a
// broker, an append-only journal, or a live console via [Saga.Observe].
type Event struct {
	Saga    string
	Step    string // empty for saga-level events
	Type    EventType
	Attempt int   // 1-based action attempt; 0 for non-action events
	Err     error // populated for *Failed events
}

// Step is one unit of work in a saga. Action performs the forward operation;
// Compensate undoes it and may be nil when the action has no side effect to
// reverse (for example a terminal notification).
//
// Compensate must be idempotent: it is only invoked for steps whose Action
// returned nil, but in a crash-recovery setup it may be retried, so it should
// tolerate being called when its effect was already undone.
type Step[T any] struct {
	Name       string
	Action     func(ctx context.Context, payload T) error
	Compensate func(ctx context.Context, payload T) error
}

// RetryPolicy controls per-step retries of the forward Action. The zero value
// means "try once, no retry".
type RetryPolicy struct {
	// MaxAttempts is the total number of Action attempts (>= 1). Values < 1 are
	// treated as 1.
	MaxAttempts int
	// Backoff returns how long to wait before the given 1-based retry attempt.
	// nil means no delay. The wait honours context cancellation.
	Backoff func(attempt int) time.Duration
}

// Saga is an immutable, executable workflow definition. Build it with [New]
// and the chainable Step/Observe/Retry methods, then call Execute.
type Saga[T any] struct {
	name  string
	steps []Step[T]
	retry RetryPolicy
	obs   func(Event)
}

// New returns an empty saga with the given name.
func New[T any](name string) *Saga[T] {
	return &Saga[T]{name: name}
}

// Step appends a step. compensate may be nil. Returns the saga for chaining.
func (s *Saga[T]) Step(name string, action, compensate func(ctx context.Context, payload T) error) *Saga[T] {
	s.steps = append(s.steps, Step[T]{Name: name, Action: action, Compensate: compensate})
	return s
}

// Observe registers a callback invoked for every [Event]. Returns the saga for
// chaining. Set this before executing; do not mutate the saga afterwards.
func (s *Saga[T]) Observe(fn func(Event)) *Saga[T] {
	s.obs = fn
	return s
}

// Retry sets the per-step retry policy for forward actions. Returns the saga
// for chaining.
func (s *Saga[T]) Retry(p RetryPolicy) *Saga[T] {
	s.retry = p
	return s
}

// Result reports what happened during an execution. Completed lists steps whose
// Action succeeded; on abort, Failed names the step that triggered rollback and
// Compensated lists the steps that were successfully undone.
type Result struct {
	Completed   []string
	Failed      string
	Compensated []string
}

// AbortError is returned when a step fails but every compensation succeeds. It
// unwraps to the originating step error.
type AbortError struct {
	Saga string
	Step string
	Err  error
}

func (e *AbortError) Error() string {
	return fmt.Sprintf("saga %q aborted at step %q: %v", e.Saga, e.Step, e.Err)
}

func (e *AbortError) Unwrap() error { return e.Err }

// CompensationError reports one or more compensations that themselves failed
// during rollback — a state requiring operator attention.
type CompensationError struct {
	Failures []StepError
}

func (e *CompensationError) Error() string {
	parts := make([]string, len(e.Failures))
	for i, f := range e.Failures {
		parts[i] = fmt.Sprintf("%s: %v", f.Step, f.Err)
	}
	return "compensation failed for [" + strings.Join(parts, "; ") + "]"
}

// StepError pairs a step name with the error it produced.
type StepError struct {
	Step string
	Err  error
}

// Execute runs the saga to completion or rolls it back on the first failure.
//
// On success it returns a Result with all steps in Completed and a nil error.
// On a step failure it compensates the completed steps in reverse order and
// returns:
//   - *AbortError, if every compensation succeeded; or
//   - an error wrapping both the *AbortError and a *CompensationError, if any
//     compensation failed.
//
// Context cancellation is treated as a step failure and triggers rollback.
func (s *Saga[T]) Execute(ctx context.Context, payload T) (Result, error) {
	var res Result
	completed := make([]int, 0, len(s.steps))

	for i := range s.steps {
		step := s.steps[i]
		if err := s.runAction(ctx, step, payload); err != nil {
			res.Failed = step.Name
			compErr := s.compensate(ctx, payload, completed, &res)
			s.emit(Event{Saga: s.name, Step: step.Name, Type: SagaAborted, Err: err})

			abort := &AbortError{Saga: s.name, Step: step.Name, Err: err}
			if compErr != nil {
				return res, fmt.Errorf("%w; rollback incomplete: %w", abort, compErr)
			}
			return res, abort
		}
		res.Completed = append(res.Completed, step.Name)
		completed = append(completed, i)
	}

	s.emit(Event{Saga: s.name, Type: SagaCompleted})
	return res, nil
}

// runAction executes a single step's Action, applying the retry policy and
// honouring context cancellation between attempts.
func (s *Saga[T]) runAction(ctx context.Context, step Step[T], payload T) error {
	attempts := s.retry.MaxAttempts
	if attempts < 1 {
		attempts = 1
	}

	var err error
	for attempt := 1; attempt <= attempts; attempt++ {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}

		s.emit(Event{Saga: s.name, Step: step.Name, Type: StepStarted, Attempt: attempt})
		err = step.Action(ctx, payload)
		if err == nil {
			s.emit(Event{Saga: s.name, Step: step.Name, Type: StepCompleted, Attempt: attempt})
			return nil
		}
		s.emit(Event{Saga: s.name, Step: step.Name, Type: StepFailed, Attempt: attempt, Err: err})

		if attempt < attempts && s.retry.Backoff != nil {
			select {
			case <-time.After(s.retry.Backoff(attempt)):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return err
}

// compensate rolls back the completed steps in reverse order. It is best-effort:
// a failing compensation is recorded but does not stop the remaining ones, so a
// single broken rollback cannot strand the other steps. Steps with a nil
// Compensate are skipped.
func (s *Saga[T]) compensate(ctx context.Context, payload T, completed []int, res *Result) error {
	var failures []StepError

	for i := len(completed) - 1; i >= 0; i-- {
		step := s.steps[completed[i]]
		if step.Compensate == nil {
			continue
		}

		s.emit(Event{Saga: s.name, Step: step.Name, Type: CompensationStarted})
		if err := step.Compensate(ctx, payload); err != nil {
			s.emit(Event{Saga: s.name, Step: step.Name, Type: CompensationFailed, Err: err})
			failures = append(failures, StepError{Step: step.Name, Err: err})
			continue
		}
		s.emit(Event{Saga: s.name, Step: step.Name, Type: CompensationCompleted})
		res.Compensated = append(res.Compensated, step.Name)
	}

	if len(failures) > 0 {
		return &CompensationError{Failures: failures}
	}
	return nil
}

func (s *Saga[T]) emit(e Event) {
	if s.obs != nil {
		s.obs(e)
	}
}

// AsAbort reports whether err is or wraps an *AbortError, returning it for
// inspection. A convenience for callers that branch on rollback outcomes.
func AsAbort(err error) (*AbortError, bool) {
	var ae *AbortError
	if errors.As(err, &ae) {
		return ae, true
	}
	return nil, false
}
