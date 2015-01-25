#!/bin/sh
# 
# Compares two revisions of HBase using JDiff.
#
# Requirements: Java, Maven, Subversion, JDiff
#
set -e

usage() {
cat << EOF
usage: $(basename $0) options <old-rev> [<new-rev>]

Run JDiff between two revisions of HBase. If the <new-rev> is not given
the end revision is set to trunk.

OPTIONS:
   -h   Show this message
   -f   Force a refresh of the local repositories
   -p   Public classes and methods only
   -e <list>  Exclude packages, e.g. "java.net:org.apache.hadoop.hbase.thrift.generated"
   -t   Exclude generated Thrift classes
   -v   Verbose - print out messages, errors and warnings
EOF
}

FORCE_REFRESH=
PUBLIC_ONLY=
EXCLUDE=
NO_THRIFT_GEN=
VERBOSE=
while getopts "hfpe:tv" OPTION; do
  case $OPTION in
  h)
    usage
    exit 0 ;;
  f)
    FORCE_REFRESH=1 ;;
  p)
    PUBLIC_ONLY=1 ;;
  e)
    EXCLUDE="$OPTARG" ;;
  t)
    NO_THRIFT_GEN=1 ;;
  v)
    VERBOSE=1 ;;
  ?)
    usage
    exit 0 ;;
  esac
done

shift $((OPTIND-1)); OPTIND=1

if [[ $# -eq 0 ]]; then
  usage
  exit 1
fi

JDIFF_HOME="/projects/external/jdiff-1.1.1"
OUT_DIR="/tmp"

OLD_REV="$1"
NEW_REV="${2:-trunk}"
OLD_NAME="hbase-$OLD_REV"
NEW_NAME="hbase-$NEW_REV"
DOCLET_PATH="$JDIFF_HOME/jdiff.jar"
DEST_NAME="changes-$OLD_REV-$NEW_REV"
JAVADOC_OPTIONS="-J-Xmx512M -encoding UTF-8 -subpackages org.apache.hadoop.hbase"
JAVADOC_OPTIONS="-firstsentence $JAVADOC_OPTIONS"
SVN_URL="http://svn.apache.org/repos/asf/hbase/trunk"
OUTFILE="/dev/null"
SOURCEPATH="@/src/main/java/:@/hbase-client/src/main/java:@/hbase-common/src/main/java:@/hbase-server/src/main/java"

if [[ $VERBOSE -eq 1 ]]; then
  OUTFILE="/dev/fd/1"
fi

if [[ $PUBLIC_ONLY -eq 1 ]]; then
  echo "Switching to public classes and methods only..."
  JAVADOC_OPTIONS="-public $JAVADOC_OPTIONS"
fi

if [[ ! -z "$EXCLUDE" ]]; then
  echo "Setting exclude list to $EXCLUDE..."
  JAVADOC_OPTIONS="-exclude $EXCLUDE"
fi

if [[ $NO_THRIFT_GEN -eq 1 ]]; then
  echo "Omitting Thrift generated classes..."
  JAVADOC_OPTIONS="-exclude org.apache.hadoop.hbase.thrift.generated $JAVADOC_OPTIONS"
fi

cd $OUT_DIR

if [[ $FORCE_REFRESH -eq 1 ]]; then
  echo "Removing directory $OUT_DIR/$OLD_NAME..."
  rm -fR $OLD_NAME
  echo "Removing directory $OUT_DIR/$NEW_NAME..."
  rm -fR $NEW_NAME
  echo "Removing directory $OUT_DIR/$DEST_NAME..."
  rm -fR $NEW_NAME

  echo "Checking out old revision $OLD_REV..."
  svn checkout ${SVN_URL}@$OLD_REV $OLD_NAME > $OUTFILE 2>&1
  echo "Building JavaDocs for old revision $OLD_REV..."
  cd $OLD_NAME
  mvn -DskipTests install javadoc:javadoc > $OUTFILE 2>&1

  cd $OUT_DIR

  echo "Checking out new revision $NEW_REV..."
  svn checkout ${SVN_URL}${2:+@$NEW_REV} $NEW_NAME > $OUTFILE 2>&1
  echo "Building JavaDocs for new revision $NEW_REV..."
  cd $NEW_NAME
  mvn -DskipTests install javadoc:javadoc > $OUTFILE 2>&1

  cd $OUT_DIR
fi

echo "Running JDiff on old revision $OLD_REV..."
javadoc -doclet jdiff.JDiff -docletpath $DOCLET_PATH \
    -apiname "$OLD_NAME" -sourcepath ${SOURCEPATH//@//$OUT_DIR/$OLD_NAME} \
    $JAVADOC_OPTIONS > $OUTFILE 2>&1

echo "Running JDiff on new revision $NEW_REV..."
javadoc -doclet jdiff.JDiff -docletpath $DOCLET_PATH \
    -apiname "$NEW_NAME" -sourcepath ${SOURCEPATH//@//$OUT_DIR/$NEW_NAME} \
    $JAVADOC_OPTIONS > $OUTFILE 2>&1

echo "Running JDiff to compare $OLD_REV with $NEW_REV..."
mkdir -p $DEST_NAME
javadoc -doclet jdiff.JDiff -docletpath $DOCLET_PATH \
    -J-Dorg.xml.sax.driver=com.sun.org.apache.xerces.internal.parsers.SAXParser \
    -oldapi "$OLD_NAME" -javadocold "$OLD_NAME/target/site/apidocs" \
    -newapi "$NEW_NAME" -javadocnew "$NEW_NAME/target/site/apidocs" \
    -d $DEST_NAME -J-Xmx512M -stats -quiet -encoding UTF-8 \
    $JDIFF_HOME/Null.java > $OUTFILE 2>&1

echo "Report saved in $DEST_NAME."
echo "Done."
