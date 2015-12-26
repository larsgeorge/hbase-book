# resolve links - "${BASH_SOURCE-$0}" may be a softlink
this="${BASH_SOURCE-$0}"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

# convert relative path to absolute path
bin=`dirname "$this"`
script=`basename "$this"`
bin=`cd "$bin">/dev/null; pwd`
this="$bin/$script"

# the root of the installation
if [ -z "$HUSH_HOME" ]; then
  export HUSH_HOME=`dirname "$this"`/..
fi

#check to see if the conf dir or hush home are given as an optional arguments
while [ $# -gt 1 ]
do
  if [ "--config" = "$1" ]
  then
    shift
    confdir=$1
    shift
    HUSH_CONF_DIR=$confdir
  else
    break
  fi
done

# Allow alternate hush conf dir location.
HUSH_CONF_DIR="${HUSH_CONF_DIR:-$HUSH_HOME/conf}"

# Source the hbase-env.sh.  Will have JAVA_HOME defined.
if [ -f "${HUSH_CONF_DIR}/hush-env.sh" ]; then
  . "${HUSH_CONF_DIR}/hush-env.sh"
fi

if [ -z "$JAVA_HOME" ]; then
  for candidate in \
    /usr/lib/jvm/java-6-sun \
    /usr/lib/jvm/java-1.6.0-sun-1.6.0.* \
    /usr/lib/j2sdk1.6-sun \
    /usr/java/jre1.6* \
    /usr/java/jdk1.6* \
    /Library/Java/Home \
    /Library/Java/JavaVirtualMachines/jdk1.7*/Contents/Home \
    /Library/Java/JavaVirtualMachines/jdk1.8*/Contents/Home \
    /usr/java/latest \
    /usr/java/default ; do
    if [ -e $candidate/bin/java ]; then
      # don't break out of the loop to ensure that the latest Java version is used
      export JAVA_HOME=$candidate
    fi
  done
  # if we didn't set it
  if [ -z "$JAVA_HOME" ]; then
    cat 1>&2 <<EOF
+======================================================================+
|      Error: JAVA_HOME is not set and Java could not be found         |
+----------------------------------------------------------------------+
| Please download the latest Sun JDK from the Sun Java web site        |
|       > http://java.sun.com/javase/downloads/ <                      |
|                                                                      |
| Hush requires Java 1.6 or later.                                    |
| NOTE: This script will find Sun Java whether you install using the   |
|       binary or the RPM based installer.                             |
+======================================================================+
EOF
    exit 1
  fi
fi
