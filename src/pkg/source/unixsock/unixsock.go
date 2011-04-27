package unixsocksource

import (
	"net"
	_log "log"
	"io"
	"os"
	"streamplay/pipe"
)

const name = "UnixSock"
const unix = "unix"

// Define some logging constants
var log = _log.New(os.Stderr, "[" + name + "] ", _log.Ldate|_log.Ltime|_log.Lshortfile)


type UnixSock struct {
	pipe   *pipe.Pipe		// The pipe
	config map[string]string	// The config for this module
	data   chan<- *pipe.Chunk	// This is how the info is passed around
	isOpen bool			// is the source open
}

func NewUnixSockSource(pipe *pipe.Pipe, config map[string]string) *UnixSock {
	source := &UnixSock{pipe: pipe, config: config}
	return source
}

func (us *UnixSock) GetName() string {
	return name
}

func (us *UnixSock) SetStream(data chan<- *pipe.Chunk) {
	us.data = data
}

func (us *UnixSock) Open() {
	if us.isOpen {
		return
	}

	p, err := net.ResolveUnixAddr(unix, us.config["path"])
	if err != nil {
		log.Fatal("failed to resolve unix address %s", us.config["path"])
	}

	l, err := net.ListenUnix(unix, p)
	if err != nil {
		log.Fatal(err)
	}
	// Assign this to the internal data
	us.isOpen = true
	go us.Accept(l)
	log.Println("Accepting connections")
}

func (us *UnixSock) Accept(l *net.UnixListener) {
	// Why doesn't this get called on ctrl + c
	defer l.Close()

	for {
		fd, err := l.Accept()
		if err != nil {
			break
		}
		us.processBuffer(fd)
	}
}

func (us *UnixSock) processBuffer(fd io.ReadWriter) {
	var buf [1024]byte

	for {
		n, err := fd.Read(buf[0:])
		if err != nil || n == 0 {
			break
		}
		us.data <- pipe.NewSimpleChunk(buf[0:n])
	}
}
