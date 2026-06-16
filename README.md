# go-saga-orchestrator

A small, dependency-free **saga orchestrator** for Go 1.22+. Run an ordered
sequence of steps; if any step fails, the already-completed steps are
**compensated in reverse order**, leaving the system consistent. This is the
standard way to coordinate a "distributed transaction" across services that
cannot share a single database transaction — provision a network, then a VM,
then a billing record, and unwind cleanly if any stage fails.

- **Zero dependencies** — standard library only.
- **Type-safe** via generics: the payload is your own struct, no `interface{}`.
- **Transport-agnostic** — emits lifecycle `Event`s through an observer, so you
  decide whether they go to Kafka/NATS, an append-only journal for crash
  recovery, or a live WebSocket stream. The core never imports a broker.
- **Best-effort rollback** — a single failing compensation is reported, not
  fatal; the remaining steps still unwind.
- **Per-step retries** with context-aware backoff.
- **Concurrency-safe** — a built saga is immutable and can be executed from many
  goroutines at once (verified under `-race`).

> Clean-room, original implementation of a well-known pattern — written to
> demonstrate the design, not derived from any employer's code.

## Install

```sh
go get github.com/mrchcoin/go-saga-orchestrator
```

## Quickstart

```go
type Order struct {
    Reserved, Charged, Shipped bool
}

flow := saga.New[*Order]("place-order").
    Observe(func(e saga.Event) { log.Printf("%s %s", e.Type, e.Step) }).
    Step("reserve-inventory",
        func(ctx context.Context, o *Order) error { o.Reserved = true; return nil },
        func(ctx context.Context, o *Order) error { o.Reserved = false; return nil }).
    Step("charge-payment",
        chargeCard,   // returns an error if the card is declined
        refundCard).  // compensation
    Step("ship",
        func(ctx context.Context, o *Order) error { o.Shipped = true; return nil },
        nil)          // nothing to undo

res, err := flow.Execute(ctx, &Order{})
if ae, ok := saga.AsAbort(err); ok {
    log.Printf("aborted at %s; rolled back %v", ae.Step, res.Compensated)
}
```

If `charge-payment` fails, `reserve-inventory` is compensated automatically and
`ship` never runs. See [`example/`](./example) for a runnable version:

```sh
go run ./example
```

## How it works

```
Execute(ctx, payload)
   │
   ├─ step 1  Action ─ ok ─┐
   ├─ step 2  Action ─ ok ─┤ completed = [1, 2]
   ├─ step 3  Action ─ FAIL
   │                       │
   └─ rollback ◀───────────┘  compensate 2, then 1   (reverse order)
        └─▶ return *AbortError  (unwraps to step 3's error)
```

Every transition emits an `Event` (`step_started`, `step_completed`,
`step_failed`, `compensation_*`, `saga_completed`, `saga_aborted`). Persisting
that stream is what turns this from an in-memory helper into a crash-recoverable
workflow engine: replay the journal to learn which steps completed, then resume
or compensate.

## Error model

| Outcome | Returned error |
|---|---|
| All steps succeed | `nil` |
| A step fails, rollback succeeds | `*AbortError` (unwraps to the step error) |
| A step fails, a compensation also fails | error wrapping both `*AbortError` and `*CompensationError` |

Use `errors.Is`/`errors.As` (or the `saga.AsAbort` helper) to branch on the
outcome.

## Retries

```go
saga.New[*Order]("place-order").
    Retry(saga.RetryPolicy{
        MaxAttempts: 3,
        Backoff:     func(attempt int) time.Duration { return time.Duration(attempt) * 100 * time.Millisecond },
    }).
    Step(...)
```

Backoff waits honour context cancellation, so a cancelled saga aborts promptly
instead of sleeping out the full delay.

## Testing

```sh
go test -race ./...
go vet ./...
```

The suite covers ordering, reverse compensation, compensation failure, nil
compensations, retry success/exhaustion, context cancellation, observer event
sequencing, and concurrent execution under the race detector.

## Design notes & trade-offs

- **Sequential by design.** Steps run in order because compensation order
  matters; parallel fan-out within a step is the caller's job (run it inside the
  step's `Action`).
- **Idempotent compensations.** Compensations should tolerate being run when
  their effect is already undone — important once you add durable recovery.
- **No persistence in core.** Durability is deliberately a plug-in via the event
  observer, keeping the core ~300 lines and free of infrastructure coupling.

## License

MIT
