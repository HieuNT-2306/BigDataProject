#!/bin/bash
set -e

# Check current location.
if [[ "$PWD" != */cr-analysis/data ]]; then
  echo "You are not in the right directory."
  echo "Follow instructions in README"
  exit 1
fi

# get yesterday date in various os.
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
  yesterday="$(date --date='yesterday' '+%Y%m%d')"
elif [[ "$OSTYPE" == "darwin"* ]]; then
  yesterday="$(date -v -1d '+%Y%m%d')"
else
  echo "Unknown platform"
fi

for ((i = 1; i < 3; i++)); do
  echo -e "\nCollect battles from 100 players [$i/2]"
  csv_path="../db/test/${yesterday}T$(date '+%H%M%S').csv"
  python collect.py -p 50 -o $csv_path
  sort --unique "$csv_path" | gzip > "$csv_path.gz"
  [[ -f "$csv_path.gz" ]] && rm "$csv_path"
done

echo -e "\nJoin multiple .csv.gz into a single one"

input_files=$(find "../db/test" -type f -name "$yesterday*.csv.gz")
output_file="../db/test/$yesterday.csv.gz"

# create a command for merge yesterday file.
cmd="sort --merge --unique"
for input in $input_files; do
  cmd="$cmd <(gunzip -c '$input')"
done

# execute command and save result in output_file.
eval "$cmd" | gzip -c > "$output_file"

if [ -s "$output_file" ]; then
  echo -e "\nAll tests pass. You're ready to collect."
  rm $input_files
  rm $output_file
  exit 0
else
  echo -e "\nUnknown error occured."
  exit 1
fi
