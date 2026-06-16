// Command example runs a small order-placement saga twice: once on the happy
// path and once where payment fails, so you can watch the orchestrator
// compensate the completed steps in reverse order.
//
//	go run ./example
package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	saga "github.com/mrchcoin/go-saga-orchestrator"
)

// order is the payload threaded through the saga's steps.
type order struct {
	ID         string
	Reserved   bool
	Charged    bool
	Shipped    bool
	FailCharge bool
}

func main() {
	fmt.Println("== happy path ==")
	run(&order{ID: "ord-1"})

	fmt.Println("\n== payment fails (rolls back) ==")
	run(&order{ID: "ord-2", FailCharge: true})
}

func run(o *order) {
	flow := saga.New[*order]("place-order").
		Observe(logEvent).
		Step("reserve-inventory",
			func(_ context.Context, o *order) error { o.Reserved = true; return nil },
			func(_ context.Context, o *order) error { o.Reserved = false; return nil }).
		Step("charge-payment",
			func(_ context.Context, o *order) error {
				if o.FailCharge {
					return errors.New("card declined")
				}
				o.Charged = true
				return nil
			},
			func(_ context.Context, o *order) error { o.Charged = false; return nil }).
		Step("ship",
			func(_ context.Context, o *order) error { o.Shipped = true; return nil },
			nil)

	res, err := flow.Execute(context.Background(), o)
	if err != nil {
		fmt.Printf("result: ABORTED at %q; rolled back %v\n", res.Failed, res.Compensated)
		fmt.Printf("error:  %v\n", err)
	} else {
		fmt.Printf("result: OK; completed %v\n", res.Completed)
	}
	fmt.Printf("state:  reserved=%t charged=%t shipped=%t\n", o.Reserved, o.Charged, o.Shipped)
}

// logEvent is a trivial observer; in production this would publish to Kafka, an
// append-only journal, or a WebSocket stream.
func logEvent(e saga.Event) {
	if e.Err != nil {
		log.Printf("[%s] step=%s attempt=%d err=%v", e.Type, e.Step, e.Attempt, e.Err)
		return
	}
	log.Printf("[%s] step=%s", e.Type, e.Step)
}
