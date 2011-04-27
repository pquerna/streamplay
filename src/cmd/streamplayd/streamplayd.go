package main

import (
	"streamplay"
	"flag"
	"fmt"
	//  "net"
	"os"
	//  "log"
)

var (
	showVersion = flag.Bool("v", false, "print streamplay's version string")
)

func Usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s %s [OPTIONS]\n", os.Args[0], streamplay.Version)
	fmt.Fprintf(os.Stderr, "\nOptions:\n")
	flag.PrintDefaults()
}

func main() {
	flag.Usage = Usage
	flag.Parse()

	if *showVersion {
		fmt.Fprintln(os.Stdout, "streamplayd", streamplay.Version)
		return
	}

	streamplay.Main()
}
