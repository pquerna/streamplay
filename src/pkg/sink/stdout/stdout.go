package sink

import (
	"log"
	"streamplay/pipe"
	"streamplay/event"
	"fmt"
)

type StdOutSink struct {
	pipe  *pipe.Pipe
	bytes int
}

func NewStdOutSink(pipe *pipe.Pipe) *StdOutSink {
	return &StdOutSink{pipe: pipe}
}

func (s *StdOutSink) Open() {
	log.Println("Open the stdout sink")

}

func (s *StdOutSink) GetName() string {
	return "StdOutSink"
}

func (s *StdOutSink) WriteEvent(ev *event.Event) {
	fmt.Printf("EVENT: %s\n", ev)
}
