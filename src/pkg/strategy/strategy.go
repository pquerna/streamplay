package strategy

import (
	"streamplay/pipe"
	"streamplay/event"
)

type StrategyEnum int

const (
	ALL StrategyEnum = iota
	ROUND_ROBIN
)

func NewStrategy(strat StrategyEnum) func(events <-chan *event.Event, sinks []pipe.Sink, out chan<- *event.Event) {
	switch strat {
	case ALL:
		return AllStrategy
	case ROUND_ROBIN:
		return RRStrategy
	}
	return nil
}

// Send the event to all the output sinks
// Confirm after all have acked
func AllStrategy(events <-chan *event.Event, sinks []pipe.Sink, out chan<- *event.Event) {
	for ev := range events {
		// Pipe to _all_ sinks
		for _, sink := range sinks {
			//sink.
			sink.WriteEvent(ev)
		}
		out <- ev
	}
}

func RRStrategy(events <-chan *event.Event, sinks []pipe.Sink, out chan<- *event.Event) {
	var l = len(sinks)
	var i = 0

	for ev := range events {
		// Pipe to _a_ sinks
		sinks[i].WriteEvent(ev)
		out <- ev

		i++
		if i >= l {
			i = 0
		}
	}
}
