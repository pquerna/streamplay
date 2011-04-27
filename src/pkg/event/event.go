package event

import (
	"fmt"
)

type Event struct {
	Body		string
	Category	string
	Position	int64
	RunOutDecos	bool
}

func NewEvent(body string, category string, position int64, runOutDecos bool) *Event {
	return &Event{Body: body, Category: category, Position: position,
		RunOutDecos: runOutDecos}
}

func (e *Event) String() string {
	return fmt.Sprintf("Body='%s',Category='%s',Position=%d,RunOutDecos=%b",
		e.Body, e.Category, e.Position, e.RunOutDecos)

}
