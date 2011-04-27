package udpsource

import (
	"net"
	_log "log"
	"os"
	"streamplay/pipe"
)


const (
	name = "UDP"
	udp  = "udp"
)

// Define some logging constants
var log = _log.New(os.Stderr, "[" + name + "] ", _log.Ldate|_log.Ltime|_log.Lshortfile)

type UDPSource struct {
	pipe   *pipe.Pipe		// The pipe
	config map[string]string	// The config for this module
	data   chan<- *pipe.Chunk	// This is how the info is passed around
	isOpen bool			// is the source open
}

func NewUDPSource(pipe *pipe.Pipe, config map[string]string) *UDPSource {
	source := &UDPSource{pipe: pipe, config: config}
	return source
}

func (us *UDPSource) GetName() string {
	return name
}

func (us *UDPSource) SetStream(data chan<- *pipe.Chunk) {
	us.data = data
}

func (us *UDPSource) Open() {
	if us.isOpen {
		return
	}

	p, err := net.ResolveUDPAddr(us.config["address"])
	if err != nil {
		log.Fatalf("failed to resolve udp address %s", us.config["address"])
	}

	l, err := net.ListenUDP(udp, p)
	if err != nil {
		log.Fatal(err)
	}
	// Assign this to the internal data
	us.isOpen = true
	go us.Run(l)
	log.Println("Accepting packets")
}

func (us *UDPSource) Run(l *net.UDPConn) {
	// Why doesn't this get called on ctrl + c
	var buf [1024]byte

	for {
		n, err := l.Read(buf[0:])
		if err != nil || n == 0 {
			break
		}
		us.data <- pipe.NewSimpleChunk(buf[0:n])
	}
}
