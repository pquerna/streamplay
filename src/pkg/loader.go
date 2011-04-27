package streamplay

import (
	"log"
	"streamplay/pipe"
	"streamplay/processor/lineend"
	"streamplay/decorator/writeaheadlog"
	"streamplay/strategy"
	"streamplay/sink"
	"streamplay/source"
	//  "streamplay/event"
)


func Load(name string, config map[string]string) {
	log.Printf("Loading pipe for '%s'\n", name)
	p := pipe.NewPipe(name)

	// Obviously make this configurable in the future
	strat := strategy.NewStrategy(strategy.ALL)
	snk := sink.NewSink("stdout", p, config)
	src := source.NewSource("unixsock", p, config)
	src2 := source.NewSource("udp", p, config)
	proc := processor.NewLineEndProcessor(p, config)
	dec := decorator.NewWriteAheadLog(p, map[string]string{"path": "/tmp/waltest"})

	// Assign all the different parts
	p.SetStrategy(strat)
	p.AddSink(snk)
	p.SetProcessor(proc)
	p.AddSource(src)
	p.AddSource(src2)
	p.AddDecorator(dec)
	log.Printf("%s", p)

	// Start process by opening all the files
	p.Start()

	// Kick it off this consumes the main event flow
	p.Run()
}
