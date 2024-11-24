#!/bin/bash

# This script only works on FreeBSD or macOS
if [[ "$OSTYPE" != "darwin"* ]]; then
  echo "This script can be run only on macOS"
  exit 1
fi

output_file="db.sqlite"

rm $output_file || touch $output_file

echo "CREATE TABLE battles (
  datetime TEXT, game_mode INT,
  tag1 TEXT, trophies1 INT, crowns1 INT,
  card11 INT, card12 INT, card13 INT, card14 INT,
  card15 INT, card16 INT, card17 INT, card18 INT,
  tag2 TEXT, trophies2 INT, crowns2 INT,
  card21 INT, card22 INT, card23 INT, card24 INT,
  card25 INT, card26 INT, card27 INT, card28 INT
)" | sqlite3 $output_file

input_files=$(find *-* -type f -name '*.csv.gz' | sort)

gzcat $input_files | sqlite3 -csv $output_file '.import /dev/stdin battles'
