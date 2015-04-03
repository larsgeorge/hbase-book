#!/bin/sh
#
# Runs the Google Protocol Buffer compiler.
#
# Requirements: protoc
#
# Note:
# - For MacOS with Homebrew run: $ brew install protobuf
#
set -e

PROTO_BASE_DIR="ch04/src/main/protobuf"

PROTO_ROWCOUNT="$PROTO_BASE_DIR/RowCountService.proto"
PROTO_FILTERS="$PROTO_BASE_DIR/CustomFilters.proto"
PROTO_OBSERVERSTATS="$PROTO_BASE_DIR/ObserverStatisticsService.proto"

PROTOS="$PROTO_ROWCOUNT $PROTO_FILTERS $PROTO_OBSERVERSTATS"

PATH_GENERATED_BASE="ch04/src/main/java"
PATH_GENERATED_COPROS="$PATH_GENERATED_BASE/coprocessor/generated"
PATH_GENERATED_FILTERS="$PATH_GENERATED_BASE/filters/generated"

PATHS="$PATH_GENERATED_COPROS $PATH_GENERATED_FILTERS"

# check all is well
if [ ! -d "$PROTO_BASE_DIR" ]; then
  echo "Error: this script must run in the project root directort... exiting!"
  exit -1
fi

# check if output directory exists, if not create it
for loc in $PATHS; do
  if [ ! -d "$loc" ]; then
    echo "creating directory: $loc"
    mkdir "$loc"
  fi
done

# run protocol buffer compiler
for proto in $PROTOS; do
  if [ -f "$proto" ]; then
    echo "compiling protocol: $proto"
    protoc -I$PROTO_BASE_DIR --java_out=$PATH_GENERATED_BASE $proto
  fi
done

echo "done."
