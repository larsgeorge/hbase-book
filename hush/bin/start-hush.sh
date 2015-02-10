#! /usr/bin/env bash
#
# run.sh: helps executing Hush by setting up the Java CLASSPATH.
#

# get the current directory
bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

# This will set HUSH_HOME, etc.
. "$bin"/hush-config.sh

# Detect if we are in hush sources dir
in_dev_env=false
if [ -d "${HUSH_HOME}/target" ]; then
  in_dev_env=true
  echo "Found development environment..."
fi

# set up Maven environment
MVN="mvn"
if [ "$MAVEN_HOME" != "" ]; then
  MVN=${MAVEN_HOME}/bin/mvn
fi

# check envvars which might override default args
if [ "$HUSH_HEAPSIZE" != "" ]; then
  JAVA_HEAP_MAX="-Xmx${HUSH_HEAPSIZE}m"
fi

# classpath initially contains $HUSH_CONF_DIR
CLASSPATH="${HUSH_CONF_DIR}"

add_maven_main_classes_to_classpath() {
  if [ -d "$HUSH_HOME/target/classes" ]; then
    CLASSPATH=${CLASSPATH}:$HUSH_HOME/target/classes
  fi
}

add_maven_test_classes_to_classpath() {
  if [ -d "$HUSH_HOME/target/test-classes" ]; then
    CLASSPATH=${CLASSPATH}:$HUSH_HOME/target/test-classes
  fi
}

add_maven_deps_to_classpath() {
  cpfile="${HUSH_HOME}/target/cached_classpath.txt"
  echo "Adding libraries from cached file: $cpfile"
  if [ ! -f "${cpfile}" ]; then
    ${MVN} -f "${HUSH_HOME}/pom.xml" dependency:build-classpath -Dmdep.outputFile="${cpfile}" &> /dev/null
  fi
  CLASSPATH=${CLASSPATH}:`cat "${cpfile}"`
}

# Add maven target directory
if $in_dev_env; then
  # add classes first, triggers log4j.properties priority
  add_maven_main_classes_to_classpath
  add_maven_test_classes_to_classpath
  # create and cache Maven classpath
  add_maven_deps_to_classpath
fi

# For releases, add hush jar & webapps to CLASSPATH
# Note: webapps must come first else it messes up Jetty
if [ -d "$HUSH_HOME/webapp" ]; then
  CLASSPATH=${CLASSPATH}:$HUSH_HOME
fi
for f in $HUSH_HOME/hush*.jar; do
  if [ -f $f ]; then
    CLASSPATH=${CLASSPATH}:$f;
  fi
done

# Add libs to CLASSPATH
for f in $HUSH_HOME/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

# default log directory & file
if [ "$HUSH_LOG_DIR" = "" ]; then
  HUSH_LOG_DIR="$HUSH_HOME/logs"
fi
if [ "$HUSH_LOGFILE" = "" ]; then
  HUSH_LOGFILE='hush.log'
fi

HUSH_OPTS="$HUSH_OPTS -Dhush.log.dir=$HUSH_LOG_DIR"
HUSH_OPTS="$HUSH_OPTS -Dhush.log.file=$HUSH_LOGFILE"
HUSH_OPTS="$HUSH_OPTS -Dhush.home.dir=$HUSH_HOME"
HUSH_OPTS="$HUSH_OPTS -Dhush.root.logger=${HUSH_ROOT_LOGGER:-INFO,console}"
if [ "x$JAVA_LIBRARY_PATH" != "x" ]; then
  HUSH_OPTS="$HUSH_OPTS -Djava.library.path=$JAVA_LIBRARY_PATH"
fi

JAVA="$JAVA_HOME/bin/java"
JAVA_HEAP_MAX="-Xmx512m"

echo "====================="
echo " Starting Hush..."
echo "====================="

echo "Using classpath: $CLASSPATH"

cd ${bin}/..
"$JAVA" $JAVA_HEAP_MAX $HUSH_OPTS -classpath "$CLASSPATH" com.hbasebook.hush.HushMain