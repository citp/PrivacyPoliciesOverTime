#!/usr/bin/env bash
#
# Extract requested fields from pcaps and write unique lines to stdout.
#
# Usage:
#
#   html2text.sh /path/to/hmtls output_dir
#

HTML_DIR=$1
OUTPUT_DIR=$2
mkdir -p $OUTPUT_DIR

MAX_PROCESS=`nproc --all`
i=0
for f in $HTML_DIR/*.html; do
  html2text --unicode-snob --ignore-emphasis --ignore-images --body-width=0 --ignore-links "$f" > $OUTPUT_DIR/"$(basename "$f" .html).txt" &
  ((i=i%MAX_PROCESS)); ((i++==0)) && wait
done
wait

