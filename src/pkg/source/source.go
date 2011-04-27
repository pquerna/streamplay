package source

import (
	"log"
	"streamplay/pipe"
	"streamplay/source/unixsock"
	"streamplay/source/udp"
)

func NewSource(name string, pi *pipe.Pipe, config map[string]string) pipe.Source {
	var src pipe.Source = nil

	switch name {
	case "unixsock":
		src = unixsocksource.NewUnixSockSource(pi, config)
	case "udp":
		src = udpsource.NewUDPSource(pi, config)
	default:
		log.Fatalf("Failed to load sink '%s'\n", name)
	}
	log.Printf("Created the '%s' module for pipe '%s'\n", name, pi.Name)
	return src
}
