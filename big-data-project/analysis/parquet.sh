#!/bin/bash

input_files=$(find ../db/**/ -type f -name "*.csv.gz")

for input_file in $input_files; do
  echo "processing $input_file"
  python parquet.py -f -i $input_file
done
