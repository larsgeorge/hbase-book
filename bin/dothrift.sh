#!/bin/sh
#
# Runs the Thrift compiler.
#
# Requirements: 
#   - Apache Thrift's thrift compiler
#   - HBase Thrift file
#
# Usage: $ dothrift.sh <thrift-file>
#
# Note:
# - For MacOS with Homebrew run: $ brew install thrift
#
set -e

PATH_GENERATED="ch06/src/main/java"
THRIFT_FILE="$1"

# check all is well
if [ $# -eq 0 ]; then
  echo "Missing thrift file parameter!"
  echo "Usage: $0 <thrift-file>"
  exit -1
fi
  
# run thrift compiler
echo "compiling thrift: $THRIFT_FILE"
thrift -out $PATH_GENERATED --gen java $THRIFT_FILE

echo "done."
