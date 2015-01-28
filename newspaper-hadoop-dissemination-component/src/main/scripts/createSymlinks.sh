#!/bin/bash

print_usage()
{
    echo "Usage: $(basename $0) roundtripid"
    echo
    echo "For example $(basename $0) B400026952016-RT1"
}

if [ $# -ne 1 ]; then
  print_usage
  exit 1
fi

SCRIPT_DIR=$(dirname $(readlink -f $0))

java -classpath "$SCRIPT_DIR/../conf:$SCRIPT_DIR/../lib/*" \
 dk.statsbiblioteket.medieplatform.autonomous.SymlinkCreatorApplication $1 -c $SCRIPT_DIR/../conf/config.properties
