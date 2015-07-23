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

PROTO_BASE_DIR="src/main/protobuf"

PROTO_SCANCONTROL="$PROTO_BASE_DIR/ScanControlService.proto"

PROTOS="$PROTO_SCANCONTROL"

PATH_GENERATED_BASE="src/main/java"
PATH_GENERATED_COPROS="$PATH_GENERATED_BASE/coprocessor/generated"

PATHS="$PATH_GENERATED_COPROS"

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
