if test -z "$GOROOT"
then
    # figure out what GOROOT is supposed to be
    GOROOT=`printf 't:;@echo $(GOROOT)\n' | gomake -f -`
    export GOROOT
fi

PKG_REQS="
    github.com/bmizerany/assert
"

PKGS="
    event
    pipe
    sink/stdout
    sink
    strategy
    decorator/writeaheadlog
    processor/lineend
    source/unixsock
    source/udp
    source 
    status
    .
"

CMDS="
    streamplayd
"
