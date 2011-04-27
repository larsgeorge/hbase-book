#! /usr/bin/env bash
#
# run.sh: helps executing Hush by setting up the Java CLASSPATH.
#

# get the current directory
bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

# set up Maven environment
MVN="mvn"
if [ "$MAVEN_HOME" != "" ]; then
  MVN=${MAVEN_HOME}/bin/mvn
fi

# classpath initially contains $HBASE_CONF_DIR
CLASSPATH="${HBASE_CONF_DIR}"

# add classes first, triggers log4j.properties priority
if [ -d "${bin}/../target/classes" ]; then
  CLASSPATH=${CLASSPATH}:${bin}/../target/classes
fi

# create and cache Maven classpath
cpfile="${bin}/../target/cached_classpath.txt"
if [ ! -f "${cpfile}" ]; then
  ${MVN} -f "${bin}/../pom.xml" dependency:build-classpath -Dmdep.outputFile="${cpfile}" &> /dev/null
fi
CLASSPATH=${CLASSPATH}:`cat "${cpfile}"`

JAVA=$JAVA_HOME/bin/java
JAVA_HEAP_MAX=-Xmx512m

echo "====================="
echo " Starting Hush..."
echo "====================="

cd ${bin}/..
"$JAVA" $JAVA_HEAP_MAX -classpath "$CLASSPATH" com.hbasebook.hush.HushMain