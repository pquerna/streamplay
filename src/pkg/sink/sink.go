package sink

import (
	"log"
	"streamplay/pipe"
	"streamplay/sink/stdout"
)

func NewSink(name string, pipe *pipe.Pipe, config map[string]string) pipe.Sink {

	switch name {
	case "stdout":
		sink := sink.NewStdOutSink(pipe)
		return sink
		break
	default:
		log.Fatalf("Failed to load sink '%s'\n", name)
	}
	return nil
}
