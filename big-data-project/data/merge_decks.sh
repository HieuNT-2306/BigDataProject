#!/bin/bash

season="$1"
path_decks="../db/decks/${season}"

csv_decks=($(find ${path_decks} -name "decks-????????.csv" | sort -V))

csv_all_decks="${path_decks}/decks-${season}.csv"
rm ${csv_all_decks} >/dev/null 2>&1
touch ${csv_all_decks}

for csv_deck in "${csv_decks[@]}"; do
	csv_all_decks_prev="${path_decks}/decks-${season}-prev.csv"
	csv_all_decks="${path_decks}/decks-${season}.csv"
	mv ${csv_all_decks} ${csv_all_decks_prev}

	echo "Merging ${csv_deck}..."
	./merge_decks ${csv_all_decks_prev} ${csv_deck} ${csv_all_decks}
done

rm ${csv_all_decks_prev}
