#!/bin/sh
set -e
if [ -f env.sh ]
then . ./env.sh
else
    echo 1>&2 "! $0 must be run from the root directory"
    exit 1
fi

xcd() {
    echo
    cd $1
    echo --- cd $1
}

mk() {
    d=$PWD
    xcd $1
    gomake clean
    gomake
    gomake install
    cd "$d"
}

rm -rf $GOROOT/pkg/${GOOS}_${GOARCH}/streamplay
rm -rf $GOROOT/pkg/${GOOS}_${GOARCH}/streamplay.a
rm -rf $GOBIN/streamplayd

for req in $PKG_REQS
do goinstall $req
done

for p in $CMD_REQS
do mk $p
done

for pkg in $PKGS
do mk pkg/$pkg
done

for cmd in $CMDS
do mk cmd/$cmd
done

#echo
#echo "--- TESTING"
#
#/bin/sh test.sh
