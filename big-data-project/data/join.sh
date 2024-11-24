#!/bin/bash
set -e

# get yesterday date in various os.
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
  yesterday="$(date --date='yesterday' '+%Y%m%d')"
elif [[ "$OSTYPE" == "darwin"* ]]; then
  yesterday="$(date -v -1d '+%Y%m%d')"
else
  echo "Unknown platform"
fi

# Ensure ../db/days directory exists
[ ! -d "../db/days" ] && mkdir "../db/days"

input_files=$(find "../db/hours" -type f -name "$yesterday*.csv.gz")
output_file="../db/days/$yesterday.csv.gz"

# create a command for merge yesterday file.
cmd="sort --merge --unique"
for input in $input_files; do
  cmd="$cmd <(gunzip -c '$input')"
done

# execute command and save result in output_file.
eval "$cmd" | gzip -c > "$output_file"

# remove hours csv file keeping only days csv.
[[ -s "$output_file" ]] && rm $input_files
