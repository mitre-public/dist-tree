#!/bin/bash

# This Script facilitates MANUALLY releasing software artifacts to Maven Central
# This Script generates a .zip file we can upload to Maven Central to publish our open-source artifact
# After running this scrip MANUALLY upload the .zip to: https://central.sonatype.com/

# This script replaces various Gradle or CI/CD plugins that can publish to Maven Central
# This tradeoff may or may not make sense for your release cadence

# This Script:
# 1 -- Uses gradle to clean & build the project
# 2 -- Gathers the artifacts we need for a Maven Central publication (e.g., jar, sources, javadocs, pom) into a dir
# 3 -- GPG signs the project artifacts
# 4 -- Computes MD5 checksums for the project artifacts
# 5 -- Computes SHA1 checksums for the project artifacts
# 6 -- Compresses these artifacts into "MC-publishable-VERSION.zip"
# 7 -- Deletes the dir where artifacts were gathered

# PRE-REQUISITES
# -- Your project's build must produce artifacts Maven Central will accept (e.g. complete POM, sources, docs, etc)
# -- GPG must be configured on the host machine to sign an artifact with a credential Maven Central recognizes
# -- The "md5sum" command must be available
# -- The "sha1sum" command must be available


GROUP_TOP_LEVEL="org";
GROUP_SECOND_LEVEL="mitre";
ARTIFACT_ID="dist-tree";
VERSION="0.0.1";

# Maven Central expects a .zip that contains a directory structure similar to our .m2 repository
TARGET_DIR="$GROUP_TOP_LEVEL/$GROUP_SECOND_LEVEL/$ARTIFACT_ID/$VERSION";

#save the current working directory for later..
CWD=$(pwd)


# Step 1 -- BUILD THE PROJECT

./gradlew clean
./gradlew build
./gradlew publishToMavenLocal


# STEP 2 -- GATHER OUR ARTIFACTS

mkdir -p $TARGET_DIR

# Gathers: XYZ.jar, XYZ-sources.jar, and XYZ-javadoc.jar
cp build/libs/* $TARGET_DIR
# Gathers: pom-default.xml (and renames it too!)
cp build/publications/mavenJava/pom-default.xml $TARGET_DIR/$ARTIFACT_ID-$VERSION.pom


# STEPS 3, 4, and 5 -- SIGN ARTIFACTS & COMPUTE CHECKSUMS

cd $TARGET_DIR || exit
files=(*)  # store filenames in an array
for file in "${files[@]}"; do
  echo "GPG Signing $file"
  gpg -ab $file

  echo "Computing MD5 checksum of $file"
  md5sum $file | cut -d " " -f 1 > $file.md5

  echo "Computing SHA1 checksum of $file"
  sha1sum $file | cut -d " " -f 1 > $file.sha1
done

cd $CWD || exit

# STEP 6 -- CREATE THE ZIP
zip -r MC-publishable-$VERSION.zip $GROUP_TOP_LEVEL

# STEP 7 -- CLEAN UP
rm -r $GROUP_TOP_LEVEL





