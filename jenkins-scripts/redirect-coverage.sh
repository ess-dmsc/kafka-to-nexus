#!/bin/bash

# Process test coverage results for publishing by Jenkins, replacing path
# to source code from container with path in build node.
#
# Usage: redirect-coverage.sh <coverage_file> <source_root>

coverage_results_file=$1
project_dir=$2

new_text=$(echo "${project_dir}" | sed 's|\/|\\\/|g')
line_num="$(grep -n "<source>" ${coverage_results_file} | head -n 1 | cut -d: -f1)"
the_line="${line_num}s|.*|<source>${new_text}<\/source>|"

sed -i ${the_line} ${coverage_results_file}
