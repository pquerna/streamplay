package processor

import (
	"log"
	"bytes"
	"streamplay/event"
	"streamplay/pipe"
)

const newLine = '\n'
const name = "LineEndProcessor"

type LineEndProcessor struct {
	pipe      *pipe.Pipe
	buf       *bytes.Buffer
	bytesRead int64
	events    chan<- *event.Event
	isOpen    bool
}

func NewLineEndProcessor(pipe *pipe.Pipe, config map[string]string) *LineEndProcessor {
	log.Printf("loading the lineend module for pipe '%s'\n", pipe.Name)
	return &LineEndProcessor{pipe: pipe, buf: new(bytes.Buffer), bytesRead: 0,
		isOpen: false}
}

func (l *LineEndProcessor) GetName() string {
	return name
}

func (l *LineEndProcessor) SetEventStream(ev chan<- *event.Event) {
	l.events = ev
}

func (l *LineEndProcessor) Open() {
	l.isOpen = true
}

func (l *LineEndProcessor) WriteData(chk *pipe.Chunk) {
	if !l.isOpen {
		log.Fatal("Configured incorrectly!  Must explicitly open the processor")
		return
	}

	for _, r := range chk.Bytes.Bytes() {
		l.bytesRead++
		if r == newLine {
			l.events <- event.NewEvent(l.buf.String(), l.pipe.Name,
				l.bytesRead, chk.RunOutDecos)
			//log.Printf("Found a newline, wrote %d bytes\n", l.buf.Len())
			l.buf.Reset()
		} else {
			l.buf.WriteByte(r)
		}
	}
}
