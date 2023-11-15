#!/bin/bash

# Run from the project's root directory to check for formatting issues with
# clang-format.

clang-format --version

find . \( \
    -name '*.cpp' \
    -or \
    -name '*.cxx' \
    -or \
    -name '*.h' \
    -or \
    -name '*.hpp' \
  \) -exec clang-format --dry-run -Werror {} +
