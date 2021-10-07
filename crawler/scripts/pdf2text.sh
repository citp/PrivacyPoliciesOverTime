set -x
#!/usr/bin/env bash
#
# Extract text from PDFs
#
# Usage:
#
#   pdf2text.sh /path/to/pdfs output_dir
#

PDF_DIR=$1
OUTPUT_DIR=$2
mkdir -p $OUTPUT_DIR
MAX_PROCESS=8
i=0

cd ..

for f in $PDF_DIR/*.pdf; do
  if [ -f $f ]
  then
    python3 pdf_extract.py $f $OUTPUT_DIR/"$(basename "$f" .pdf).txt" &
    ((i=i%MAX_PROCESS)); ((i++==0)) && wait
  fi
done
wait

