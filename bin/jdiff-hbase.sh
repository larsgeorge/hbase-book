#!/bin/sh
# 
# Compares two revisions of HBase using JDiff.
#
# Requirements: Java, Maven, Subversion 
#
set -e

if [[ $# -eq 0 ]]; then
        echo "Usage: $(basename $0) <old-rev> [<new-rev>]"
	echo "	If <new-rev> is omitted then trunk is used instead"
        exit 1
fi

OLD_REV="$1"
NEW_REV="${2:-trunk}"
OLD_NAME="hbase-$OLD_REV"
NEW_NAME="hbase-$NEW_REV"
JDIFF_HOME="/projects/external/jdiff-1.1.1"
DOCLET_PATH="$JDIFF_HOME/jdiff.jar"
OUT_DIR="/tmp"
DEST_NAME="changes-$OLD_REV-$NEW_REV"
JAVADOC_OPTIONS="-J-Xmx512M -encoding UTF-8 -subpackages org.apache.hadoop.hbase"
JAVADOC_OPTIONS="-firstsentence $JAVADOC_OPTIONS"
SVN_URL="http://svn.apache.org/repos/asf/hbase/trunk"

cd $OUT_DIR

echo "Removing directory $OUT_DIR/$OLD_NAME..."
rm -fR $OLD_NAME
echo "Removing directory $OUT_DIR/$NEW_NAME..."
rm -fR $NEW_NAME
echo "Removing directory $OUT_DIR/$DEST_NAME..."
rm -fR $NEW_NAME

echo "Checking out old revision $OLD_REV..."
svn checkout ${SVN_URL}@$OLD_REV $OLD_NAME > /dev/null
echo "Building JavaDocs for old revision $OLD_REV..."
cd $OLD_NAME
mvn javadoc:javadoc > /dev/null

cd $OUT_DIR

echo "Checking out new revision $NEW_REV..."
svn checkout ${SVN_URL}${2:+@$NEW_REV} $NEW_NAME > /dev/null
echo "Building JavaDocs for new revision $NEW_REV..."
cd $NEW_NAME
mvn javadoc:javadoc > /dev/null

cd $OUT_DIR

echo "Running JDiff on old revision $OLD_REV..."
javadoc -doclet jdiff.JDiff -docletpath $DOCLET_PATH \
    -apiname "$OLD_NAME" -sourcepath $OLD_NAME/src/main/java/ $JAVADOC_OPTIONS

echo "Running JDiff on new revision $NEW_REV..."
javadoc -doclet jdiff.JDiff -docletpath $DOCLET_PATH \
    -apiname "$NEW_NAME" -sourcepath $NEW_NAME/src/main/java/ $JAVADOC_OPTIONS

echo "Running JDiff to compare $OLD_REV with $NEW_REV..."
mkdir -p $DEST_NAME
javadoc -doclet jdiff.JDiff -docletpath $DOCLET_PATH \
    -J-Dorg.xml.sax.driver=com.sun.org.apache.xerces.internal.parsers.SAXParser \
    -oldapi "$OLD_NAME" -javadocold "$OLD_NAME/target/site/apidocs" \
    -newapi "$NEW_NAME" -javadocnew "$NEW_NAME/target/site/apidocs" \
    -d $DEST_NAME -J-Xmx512M -stats -encoding UTF-8 $JDIFF_HOME/Null.java

echo "Report saved in $DEST_NAME."
echo "Done."
