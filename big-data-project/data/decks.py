import json
import argparse
import csv
from pathlib import Path

import numpy as np
import pandas as pd
from rich.progress import track

# Paths
path_root = Path(__file__).parent.parent
path_db = path_root / "db"
path_assets = path_root / "analysis" / "assets"
path_cards = path_assets / "cards.json"
path_gamemodes = path_assets / "gamemodes.json"


# json assets
with open(path_gamemodes) as f:
    gamemodes = {str(gm["id"]): gm for gm in json.load(f)}
with open(path_cards) as f:
    cards = json.load(f)

# conversion dictionaries
idtoi = {str(card["id"]): i for i, card in enumerate(cards)}
itoid = {i: str(card["id"]) for i, card in enumerate(cards)}
itos = {i: card["name"] for i, card in enumerate(cards)}

# conversion functions
encode = lambda deck: [idtoi[card] for card in deck]  # noqa: E731
decode = lambda deck: [itos[card] for card in deck]  # noqa: E731
stringify = lambda deck: "".join(map(lambda x: str(x).zfill(3), deck))  # noqa: E731

P1_DECK = [f"p1_card{i}" for i in range(1, 9)]
P2_DECK = [f"p2_card{i}" for i in range(1, 9)]
CONVERTERS = {
    "datetime": pd.to_datetime,
    "gamemode": int,
    "p1_tag": str,
    "p1_trophies": int,
    "p1_crowns": int,
    **{c: lambda id: idtoi[id] for c in P1_DECK},
    "p2_tag": str,
    "p2_trophies": int,
    "p2_crowns": int,
    **{c: lambda id: idtoi[id] for c in P2_DECK},
}

# Converting datetime to datetime object is slow, use only if needed
USECOLS = ["gamemode", "p1_crowns", *P1_DECK, "p2_crowns", *P2_DECK]


def process_csv_file(csv_path_in, csv_path_out, max_rows=None):
    df = pd.read_csv(
        csv_path_in,
        nrows=max_rows,
        names=list(CONVERTERS.keys()),
        usecols=USECOLS,
        converters=CONVERTERS,  # type: ignore
    )

    # remove mirror matches and draws
    df = df[(df[P1_DECK].values != df[P2_DECK].values).any(axis=1)]
    df = df[df["p1_crowns"] != df["p2_crowns"]]

    # numpy arrays
    decks = df[P1_DECK + P2_DECK].values.reshape(-1, 2, 8)
    targets = np.eye(2, 2, dtype=int)[(df["p1_crowns"] < df["p2_crowns"]).astype(int)]

    # sort cards in decks
    decks.sort()

    # sort decks in battles
    idx = np.apply_along_axis(stringify, axis=2, arr=decks).argsort(axis=1)
    idx_decks = idx[:, :, np.newaxis]
    idx_targets = idx[:, 0].astype(bool)
    decks = np.take_along_axis(decks, idx_decks, axis=1).reshape(-1, 16)
    targets[idx_targets] = (targets[idx_targets] + 1) % 2

    # unique decks and total wins
    decks, inv = np.unique(decks, axis=0, return_inverse=True)
    wins = np.zeros((len(decks), 2), dtype=int)
    np.add.at(wins, inv, targets)

    # save to CSV
    with open(csv_path_out, "w", newline="") as f:
        writer = csv.writer(f)
        for deck, win in zip(decks, wins):
            writer.writerow([*deck, *win])


def next_row(reader):
    row = next(reader, None)
    if row is None:
        return None
    return row, stringify(row[:16]), int(row[16]), int(row[17])


def merge_csv_files(csv_path_in_1, csv_path_in_2, csv_path_out):
    with (
        open(csv_path_in_1, "r") as f1,
        open(csv_path_in_2, "r") as f2,
        open(csv_path_out, "w") as out,
    ):
        reader1 = csv.reader(f1)
        reader2 = csv.reader(f2)
        writer = csv.writer(out)

        ROW = 0
        DECKS = 1
        WINS = 2
        LOSSES = 3

        # Read the first line from each file
        row1 = next_row(reader1)
        row2 = next_row(reader2)

        while row1 and row2:
            if row1[DECKS] == row2[DECKS]:
                # Merge the rows and sum the values
                merged_row = row1[ROW][:-2] + [
                    row1[WINS] + row2[WINS],
                    row1[LOSSES] + row2[LOSSES],
                ]
                writer.writerow(merged_row)
                row1 = next_row(reader1)
                row2 = next_row(reader2)
            elif row1[DECKS] < row2[DECKS]:
                writer.writerow(row1[ROW])
                row1 = next_row(reader1)
            else:
                writer.writerow(row2[ROW])
                row2 = next_row(reader2)

        # Write the remaining rows from each file
        while row1:
            writer.writerow(row1[ROW])
            row1 = next_row(reader1)

        while row2:
            writer.writerow(row2[ROW])
            row2 = next_row(reader2)


def main(path_battles, path_decks, args):
    # process battles
    csv_battles = sorted(list(path_battles.glob("????????.csv")))
    assert len(csv_battles) > 0, "No battles CSV files found"
    for csv_battle in track(
        csv_battles,
        disable=not args.verbose,
        description="Processing...",
    ):
        csv_deck = path_decks / f"decks-{csv_battle.name}"
        if not csv_deck.exists():
            process_csv_file(csv_battle, csv_deck, args.max_rows)

    # merge decks (slower version than merge_decks.sh)
    if args.merge:
        csv_decks = sorted(list(path_decks.glob("decks-????????.csv")))
        csv_all_decks = path_decks / f"decks-{args.season}.csv"
        assert len(csv_battles) > 0, "No decks CSV files found"
        if csv_all_decks.exists():
            csv_all_decks.unlink()
        csv_all_decks.touch()
        for csv_deck in track(
            csv_decks,
            disable=not args.verbose,
            description="Merging...",
        ):
            csv_all_decks_prev = path_decks / f"decks-{args.season}-prev.csv"
            csv_all_decks = path_decks / f"decks-{args.season}.csv"
            csv_all_decks.rename(csv_all_decks_prev)
            merge_csv_files(csv_all_decks_prev, csv_deck, csv_all_decks)
        csv_all_decks_prev.unlink()  # type: ignore


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--season", type=str, required=True)
    parser.add_argument("-r", "--max_rows", type=int, default=None)
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-m", "--merge", action="store_true", default=False)
    args = parser.parse_args()

    path_battles = path_db / "kaggle" / args.season
    path_decks = path_db / "decks" / args.season
    path_decks.mkdir(exist_ok=True, parents=True)

    main(path_battles, path_decks, args)
