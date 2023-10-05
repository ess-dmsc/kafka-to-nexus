#!/bin/bash

# Configure build with release options.
#
# Usage: configure-release.sh <source_path> <build_path>

SOURCE_PATH="$1"
BUILD_PATH="$2"

if [ -z $SOURCE_PATH ] ; then
  echo "Error: missing source path argument"
  exit 1
fi

if [ -z $BUILD_PATH ] ; then
  echo "Error: missing build path argument"
  exit 1
fi

set -x

cmake \
  -G Ninja \
  -D CMAKE_BUILD_TYPE=Release \
  -D CONAN=MANUAL \
  -D CMAKE_SKIP_RPATH=FALSE \
  -D CMAKE_INSTALL_RPATH='$ORIGIN/../lib' \
  -D CMAKE_BUILD_WITH_INSTALL_RPATH=TRUE \
  -S ${SOURCE_PATH} \
  -B ${BUILD_PATH}
