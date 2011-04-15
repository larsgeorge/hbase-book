#! /usr/bin/env bash
#
# run.sh: helps executing the examples by setting up the Java CLASSPATH.
#

MVN="mvn"
if [ "$MAVEN_HOME" != "" ]; then
  MVN=${MAVEN_HOME}/bin/mvn
fi

# CLASSPATH initially contains $HBASE_CONF_DIR
CLASSPATH="${HBASE_CONF_DIR}"

# Need to generate classpath from maven pom. This is costly so generate it
# and cache it. Save the file into our target dir so a mvn clean will get
# clean it up and force us create a new one.
cpfile="target/cached_classpath.txt"
if [ ! -f "${cpfile}" ]
then
  ${MVN} -f "../pom.xml" dependency:build-classpath -Dmdep.outputFile="${cpfile}" &> /dev/null
fi
CLASSPATH=${CLASSPATH}:`cat "${cpfile}"`

if [ -d "target/classes" ]; then
  CLASSPATH=${CLASSPATH}:target/classes
fi

JAVA=$JAVA_HOME/bin/java
JAVA_HEAP_MAX=-Xmx512m

"$JAVA" $JAVA_HEAP_MAX -classpath "$CLASSPATH" "$@"