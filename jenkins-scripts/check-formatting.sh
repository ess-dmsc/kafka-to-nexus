#!/bin/bash

clang-format --version

find . \(  -name '*.cpp' -or -name '*.cxx' -or -name '*.h' -or -name '*.hpp' \) -exec clang-format --dry-run -Werror {} +
