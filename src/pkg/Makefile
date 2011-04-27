include ../Make.inc

VERSION=v0.1

TARG=streamplay
GOFILES=\
	streamplay.go\
	loader.go\
	version.go\

include $(GOROOT)/src/Make.pkg

.PHONY: version.go
version.go:
	printf 'package streamplay\nconst Version = `%s`\n' '$(VERSION)' > $@
