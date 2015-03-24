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
PATH_GENERATED="ch04/src/main/java/coprocessor/generated"

# check all is well
if [ ! -d "$PROTO_BASE_DIR" ]; then
  echo "Error: this script must run in the project root directort... exiting!"
  exit -1
fi

# check if output directory exists, if not create it
if [ ! -d "$PATH_GENERATED" ]; then
  echo "creating directory: $PATH_GENERATED"
  mkdir "$PATH_GENERATED"
fi

# run protocol buffer compiler
if [ -f "$PROTO_ROWCOUNT" ]; then
  echo "compiling row count protocol..."
  protoc -I$PROTO_BASE_DIR --java_out=$PATH_GENERATED $PROTO_ROWCOUNT
fi

echo "done."
