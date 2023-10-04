#!/bin/bash

PROJECT_PATH="$1"

if [ -z $PROJECT_PATH ] ; then
  echo "Error: missing project path argument"
  exit 1
fi

set -x

cmake \
  -GNinja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCONAN=MANUAL \
  -DCMAKE_SKIP_RPATH=FALSE \
  -DCMAKE_INSTALL_RPATH='\$ORIGIN/../lib' \
  -DCMAKE_BUILD_WITH_INSTALL_RPATH=TRUE \
  ${PROJECT_PATH}
