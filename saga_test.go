package saga_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	saga "github.com/mrchcoin/go-saga-orchestrator"
)

// trace records the order in which actions and compensations run.
type trace struct {
	mu  sync.Mutex
	log []string
}

func (t *trace) add(s string) {
	t.mu.Lock()
	t.log = append(t.log, s)
	t.mu.Unlock()
}

func (t *trace) snapshot() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]string, len(t.log))
	copy(out, t.log)
	return out
}

// act/comp build action and compensation funcs that record into the trace and
// optionally fail.
func act(t *trace, name string, fail error) func(context.Context, *trace) error {
	return func(context.Context, *trace) error {
		if fail != nil {
			return fail
		}
		t.add("do:" + name)
		return nil
	}
}

func comp(name string, fail error) func(context.Context, *trace) error {
	return func(_ context.Context, t *trace) error {
		if fail != nil {
			return fail
		}
		t.add("undo:" + name)
		return nil
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestExecuteSuccess(t *testing.T) {
	tr := &trace{}
	sg := saga.New[*trace]("happy").
		Step("a", act(tr, "a", nil), comp("a", nil)).
		Step("b", act(tr, "b", nil), comp("b", nil)).
		Step("c", act(tr, "c", nil), comp("c", nil))

	res, err := sg.Execute(context.Background(), tr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !equal(res.Completed, []string{"a", "b", "c"}) {
		t.Errorf("completed = %v, want [a b c]", res.Completed)
	}
	if len(res.Compensated) != 0 {
		t.Errorf("compensated = %v, want none", res.Compensated)
	}
	if !equal(tr.snapshot(), []string{"do:a", "do:b", "do:c"}) {
		t.Errorf("trace = %v", tr.snapshot())
	}
}

func TestCompensatesInReverseOrder(t *testing.T) {
	tr := &trace{}
	boom := errors.New("boom")
	sg := saga.New[*trace]("rollback").
		Step("a", act(tr, "a", nil), comp("a", nil)).
		Step("b", act(tr, "b", nil), comp("b", nil)).
		Step("c", act(tr, "c", boom), comp("c", nil)). // fails
		Step("d", act(tr, "d", nil), comp("d", nil))   // never reached

	res, err := sg.Execute(context.Background(), tr)

	ae, ok := saga.AsAbort(err)
	if !ok {
		t.Fatalf("want *AbortError, got %T: %v", err, err)
	}
	if ae.Step != "c" {
		t.Errorf("aborted step = %q, want c", ae.Step)
	}
	if !errors.Is(err, boom) {
		t.Errorf("error should unwrap to boom, got %v", err)
	}
	if res.Failed != "c" {
		t.Errorf("res.Failed = %q, want c", res.Failed)
	}
	// Actions a,b ran; then b,a compensated in reverse. c's action failed so it
	// was never completed and is not compensated.
	want := []string{"do:a", "do:b", "undo:b", "undo:a"}
	if !equal(tr.snapshot(), want) {
		t.Errorf("trace = %v, want %v", tr.snapshot(), want)
	}
	if !equal(res.Compensated, []string{"b", "a"}) {
		t.Errorf("compensated = %v, want [b a]", res.Compensated)
	}
}

func TestCompensationFailureIsReported(t *testing.T) {
	tr := &trace{}
	boom := errors.New("step failed")
	rollbackErr := errors.New("undo a failed")
	sg := saga.New[*trace]("broken-rollback").
		Step("a", act(tr, "a", nil), comp("a", rollbackErr)). // compensation fails
		Step("b", act(tr, "b", nil), comp("b", nil)).
		Step("c", act(tr, "c", boom), comp("c", nil))

	_, err := sg.Execute(context.Background(), tr)
	if err == nil {
		t.Fatal("expected error")
	}
	// Must still surface the original abort...
	if !errors.Is(err, boom) {
		t.Errorf("error should unwrap to original step error, got %v", err)
	}
	// ...and report the compensation failure.
	var ce *saga.CompensationError
	if !errors.As(err, &ce) {
		t.Fatalf("want wrapped *CompensationError, got %v", err)
	}
	if len(ce.Failures) != 1 || ce.Failures[0].Step != "a" {
		t.Errorf("compensation failures = %+v, want one for step a", ce.Failures)
	}
}

func TestNilCompensateSkipped(t *testing.T) {
	tr := &trace{}
	boom := errors.New("boom")
	sg := saga.New[*trace]("nil-comp").
		Step("notify", act(tr, "notify", nil), nil). // no compensation
		Step("charge", act(tr, "charge", boom), comp("charge", nil))

	res, err := sg.Execute(context.Background(), tr)
	if _, ok := saga.AsAbort(err); !ok {
		t.Fatalf("want *AbortError, got %v", err)
	}
	// notify completed but has no compensation; must not panic or appear undone.
	if !equal(tr.snapshot(), []string{"do:notify"}) {
		t.Errorf("trace = %v, want [do:notify]", tr.snapshot())
	}
	if len(res.Compensated) != 0 {
		t.Errorf("compensated = %v, want none", res.Compensated)
	}
}

func TestRetrySucceedsAfterTransientFailures(t *testing.T) {
	tr := &trace{}
	var calls atomic.Int32
	flaky := func(context.Context, *trace) error {
		if calls.Add(1) < 3 { // fail twice, succeed on the 3rd
			return errors.New("transient")
		}
		tr.add("do:flaky")
		return nil
	}
	sg := saga.New[*trace]("retry").
		Retry(saga.RetryPolicy{MaxAttempts: 3}). // no backoff -> fast test
		Step("flaky", flaky, comp("flaky", nil))

	res, err := sg.Execute(context.Background(), tr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls.Load() != 3 {
		t.Errorf("calls = %d, want 3", calls.Load())
	}
	if !equal(res.Completed, []string{"flaky"}) {
		t.Errorf("completed = %v, want [flaky]", res.Completed)
	}
}

func TestRetryExhaustedAborts(t *testing.T) {
	tr := &trace{}
	var calls atomic.Int32
	always := func(context.Context, *trace) error {
		calls.Add(1)
		return errors.New("permanent")
	}
	sg := saga.New[*trace]("retry-fail").
		Retry(saga.RetryPolicy{MaxAttempts: 4}).
		Step("a", act(tr, "a", nil), comp("a", nil)).
		Step("b", always, comp("b", nil))

	_, err := sg.Execute(context.Background(), tr)
	if _, ok := saga.AsAbort(err); !ok {
		t.Fatalf("want *AbortError, got %v", err)
	}
	if calls.Load() != 4 {
		t.Errorf("calls = %d, want 4 attempts", calls.Load())
	}
	if !equal(tr.snapshot(), []string{"do:a", "undo:a"}) {
		t.Errorf("trace = %v, want [do:a undo:a]", tr.snapshot())
	}
}

func TestContextCancellationTriggersRollback(t *testing.T) {
	tr := &trace{}
	ctx, cancel := context.WithCancel(context.Background())
	sg := saga.New[*trace]("cancel").
		Step("a", act(tr, "a", nil), comp("a", nil)).
		Step("b", func(context.Context, *trace) error {
			cancel() // cancel mid-flight; next step must not run
			return nil
		}, comp("b", nil)).
		Step("c", act(tr, "c", nil), comp("c", nil))

	_, err := sg.Execute(ctx, tr)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	// c never ran; b and a rolled back in reverse.
	want := []string{"do:a", "undo:b", "undo:a"}
	if !equal(tr.snapshot(), want) {
		t.Errorf("trace = %v, want %v", tr.snapshot(), want)
	}
}

func TestObserverSeesLifecycle(t *testing.T) {
	var mu sync.Mutex
	var types []saga.EventType
	tr := &trace{}
	boom := errors.New("boom")

	sg := saga.New[*trace]("observed").
		Observe(func(e saga.Event) {
			mu.Lock()
			types = append(types, e.Type)
			mu.Unlock()
		}).
		Step("a", act(tr, "a", nil), comp("a", nil)).
		Step("b", act(tr, "b", boom), comp("b", nil))

	_, _ = sg.Execute(context.Background(), tr)

	want := []saga.EventType{
		saga.StepStarted, saga.StepCompleted, // a
		saga.StepStarted, saga.StepFailed, // b
		saga.CompensationStarted, saga.CompensationCompleted, // undo a
		saga.SagaAborted,
	}
	mu.Lock()
	got := types
	mu.Unlock()
	if len(got) != len(want) {
		t.Fatalf("event types = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("event[%d] = %q, want %q (full: %v)", i, got[i], want[i], got)
		}
	}
}

// TestConcurrentExecute runs one immutable saga definition from many goroutines
// to demonstrate that execution carries no shared mutable state. Run with -race.
func TestConcurrentExecute(t *testing.T) {
	var completed atomic.Int64
	sg := saga.New[*trace]("concurrent").
		Observe(func(saga.Event) { /* must tolerate concurrent calls */ }).
		Step("a", func(context.Context, *trace) error { return nil }, nil).
		Step("b", func(context.Context, *trace) error { return nil }, nil)

	const n = 200
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			if _, err := sg.Execute(context.Background(), &trace{}); err == nil {
				completed.Add(1)
			}
		}()
	}
	wg.Wait()
	if completed.Load() != n {
		t.Errorf("completed = %d, want %d", completed.Load(), n)
	}
}

func TestRetryBackoffHonoursContext(t *testing.T) {
	// A long backoff plus an already-cancelled-soon context must abort quickly
	// rather than sleeping for the full delay.
	tr := &trace{}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	sg := saga.New[*trace]("backoff").
		Retry(saga.RetryPolicy{
			MaxAttempts: 5,
			Backoff:     func(int) time.Duration { return time.Hour }, // would hang without ctx
		}).
		Step("x", func(context.Context, *trace) error { return errors.New("fail") }, nil)

	start := time.Now()
	_, err := sg.Execute(ctx, tr)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("execute took %v; backoff did not honour context", elapsed)
	}
	if err == nil {
		t.Fatal("expected error")
	}
}

// Example shows the canonical order-placement saga: reserve, charge, ship.
func Example() {
	type order struct{ events []string }
	rec := func(o *order, s string) func(context.Context, *order) error {
		return func(context.Context, *order) error { o.events = append(o.events, s); return nil }
	}

	o := &order{}
	flow := saga.New[*order]("place-order").
		Step("reserve-inventory", rec(o, "reserved"), rec(o, "released")).
		Step("charge-payment", func(context.Context, *order) error { return errors.New("card declined") }, rec(o, "refunded")).
		Step("ship", rec(o, "shipped"), nil)

	_, err := flow.Execute(context.Background(), o)
	fmt.Println(o.events)
	fmt.Println(err)
	// Output:
	// [reserved released]
	// saga "place-order" aborted at step "charge-payment": card declined
}
