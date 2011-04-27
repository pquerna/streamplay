package pipe

import (
	"bytes"
	"streamplay/event"
	"fmt"
	"log"
)


const (
	SOURCE_BUFFER_SIZE	= 1
	PROCESSOR_BUFFER_SIZE	= 1
)


type Chunk struct {
	Bytes		*bytes.Buffer
	Object		interface{}
	RunInDecos	bool
	RunProcessor	bool
	RunOutDecos	bool

}

type Source interface {
	GetName() string
	Open()
	SetStream(data chan<- *Chunk)
}

type Decorator interface {
	GetName() string
	GetReady() chan bool
	Open()
	SetSourceStream(data chan<- *Chunk)
	Inbound(chk *Chunk)
	Outbound(e *event.Event)
}

type Processor interface {
	GetName() string
	Open()
	SetEventStream(ev chan<- *event.Event)
	WriteData(data *Chunk)
}

type Sink interface {
	GetName() string
	Open()
	WriteEvent(ev *event.Event)
}


type Pipe struct {
	Name       string
	Sources    []Source
	Processor  Processor
	Decorators []Decorator
	Sinks      []Sink
	Strategy   func(events <-chan *event.Event, sinks []Sink, out chan<- *event.Event)

	sourceData chan *Chunk
	procData   chan *Chunk
	eventData  chan *event.Event
}


func NewPipe(Name string) *Pipe {
	sd := make(chan *Chunk, SOURCE_BUFFER_SIZE)
	pd := make(chan *Chunk, PROCESSOR_BUFFER_SIZE)
	ed := make(chan *event.Event)
	return &Pipe{Name: Name, sourceData: sd, eventData: ed, procData: pd}
}

// Return a simple Chunk, this is for most buffers.  However
// things like the scribe reader won't use this, it will use the more advanced
// interface called the Object interface
func NewSimpleChunk(buffer []uint8) *Chunk {
	return &Chunk{Bytes: bytes.NewBuffer(buffer), RunInDecos: true,
		RunProcessor: true, RunOutDecos:true, Object: nil}
}

// Return a more bare Chunk, This is for most replay type activites.  However
// it won't be used for in and out decorators
func NewReplayChunk(buffer []uint8) *Chunk {
	return &Chunk{Bytes: bytes.NewBuffer(buffer), RunInDecos: false,
		RunProcessor: true, RunOutDecos: false, Object: nil}
}

func (chk *Chunk) Len() int {
	return chk.Bytes.Len()
}

func (pipe *Pipe) AddSource(source Source) {
	pipe.Sources = append(pipe.Sources, source)
	source.SetStream(pipe.sourceData)
}

func (pipe *Pipe) AddDecorator(decorator Decorator) {
	pipe.Decorators = append(pipe.Decorators, decorator)
	decorator.SetSourceStream(pipe.procData)
}

func (pipe *Pipe) AddSink(sink Sink) {
	pipe.Sinks = append(pipe.Sinks, sink)
}

func (pipe *Pipe) SetProcessor(processor Processor) {
	pipe.Processor = processor
	processor.SetEventStream(pipe.eventData)
}

func (pipe *Pipe) SetStrategy(strat func(events <-chan *event.Event, sinks []Sink, out chan<- *event.Event)) {
	pipe.Strategy = strat
}

// Start the various components, in reverse order, disregarding
// the middleware-like nature of the decorators
//  - Sinks
//  - Processor
//  - Decorators 
//  - Sources
func (pipe *Pipe) Start() {
	//for _, sinks := range
	pipe.Processor.Open()
	log.Printf("Opening processor, '%s', for pipe '%s'\n", pipe.Processor.GetName(), pipe.Name)
	//done := make(chan bool)
	doneEvents := make(chan *event.Event)

	/* TODO: Figure out if this really necessary, the biggest problem with
		this method is that it is one off, when the act of replaying
		isn't really one off.  Open now blocks, and there are go
		routines listening for the rest of the data coming in.  This
		works cleaner now, there is a seperate queue for the sourceData
		that comes from various sources.  That data is processed then
		added to the procData queue.  There it is the same structure
		manipulated then output on the event queue.  Once it gets to the
		event queue, it hits the strategy component.  That decides how
		to process the event.

	// Listen only for sourceData
	// until its done configuring
	go func() {
		for {
			select {
			case buf := <-pipe.sourceData:
				pipe.Processor.WriteData(buf)
			case <-done:
				log.Println("Finished running preload scripts")
				return
			}
		}
	}()

	// When all decorators are completely ready, then
	// flag the done as good
	go func() {
		for _, dec := range pipe.Decorators {
			<-dec.GetReady()
			log.Printf("Finished with preload on decorator '%s', for pipe '%s'\n", dec.GetName(), pipe.Name)
		}
		done <- true
	}()
	*/

	// Create the strategy
	go pipe.ProcessData()
	go pipe.Strategy(pipe.eventData, pipe.Sinks, doneEvents)
	go pipe.WriteEvents(doneEvents)

	// Open all the decorators
	for _, dec := range pipe.Decorators {
		dec.Open()
	}

	// Wait before on for all the decorators to say they are ready to
	// proceed
	for _, dec := range pipe.Decorators {
		<-dec.GetReady()
		log.Printf("Finished with preload on decorator '%s', for pipe '%s'\n", dec.GetName(), pipe.Name)
	}

	// Now for the input
	for _, src := range pipe.Sources {
		src.Open()
	}
}

// Write out events to the external decorators in reverse
// order.  This is to give the feel of a middleware type
// component
func (pipe *Pipe) WriteEvents(evq <-chan *event.Event) {
	var length = len(pipe.Decorators)
	var d Decorator

	// Based on the strategy confirm with the 
	for ev := range evq {
		if ev.RunOutDecos {
			for i := length - 1; i >= 0; i-- {
				d = pipe.Decorators[i]
				d.Outbound(ev)
			}
		}
	}
}

// This has to process at least one decorator once or else data won't be
// produced, and if data isn't produced this could lead to a deadlock
func (pipe *Pipe) processDecorators() {
	for data := range pipe.sourceData {
		// Data is processed blocking through the decorators
		if data.RunInDecos {
			for _, d := range pipe.Decorators {
				// Only process next if previous says okay
				if data.RunInDecos {
					d.Inbound(data)
				}
			}
		} else {
			if data.RunProcessor {
				pipe.procData <- data
			}
		}
	}
}

// This is multiplexed in such a way it is processed by a decorator and if all
// the decorators either let it pass to the processor than an event is added to
// the processor queue
func (pipe *Pipe) ProcessData() {
	for pd := range pipe.procData {
		if pd.RunProcessor {
			pipe.Processor.WriteData(pd)
		}
	}
}

func (pipe *Pipe) Run() {
	for {
		pipe.processDecorators()
	}
}

func (pipe *Pipe) StringSources() string {
	s := "[ "
	var s1 string

	for i, source := range pipe.Sources {
		s1 = fmt.Sprintf("Source[%s]", source.GetName())
		if i > 0 {
			s = s + ", " + s1
		} else {
			s = s + s1
		}
	}
	s = s + " ]"
	return s
}

func (pipe *Pipe) BeginStringDecorators() string {
	s := "[ "
	var s1 string

	for i, decorator := range pipe.Decorators {
		s1 = fmt.Sprintf("Decorator[%s,%d].Inbound()", decorator.GetName(), i)
		if i > 0 {
			s = s + " -> " + s1
		} else {
			s = s + s1
		}
	}
	s = s + " ]"
	return s
}

func (pipe *Pipe) EndStringDecorators() string {
	s := "[ "
	var s1 string

	for i := len(pipe.Decorators) - 1; i >= 0; i-- {
		s1 = fmt.Sprintf("Decorator[%s,%d].Outbound()", pipe.Decorators[i].GetName(), i)
		if i > 0 {
			s = s + " -> " + s1
		} else {
			s = s + s1
		}
	}
	s = s + " ]"
	return s
}

func (pipe *Pipe) StringProcessor() string {
	s := fmt.Sprintf(" Processor[%s] ", pipe.Processor.GetName())
	return s
}

func (pipe *Pipe) StringSinks() string {
	s := "[ "
	for i, sink := range pipe.Sinks {
		s += fmt.Sprintf("Sink[%s,%d]", sink.GetName(), i)
	}
	s += " ]"
	return s
}

func (pipe *Pipe) StringStrategy() string {
	s := "Strat"
	return s
}

func (pipe *Pipe) String() string {
	s := pipe.StringSources() + " -> " + pipe.BeginStringDecorators() + " -> " + pipe.StringProcessor() + " -> " + pipe.StringStrategy() + " -> " + pipe.StringSinks() + " -> " + pipe.EndStringDecorators()
	return s
}
