import argparse
import json
import pathlib

import numpy as np
import pandas as pd

# PATHS -------------------------------------------------------------------------------

here = pathlib.Path(__file__).parent
db = here.parent / "db"
assets = here / "assets"

# ARGPARSE ----------------------------------------------------------------------------

parser = argparse.ArgumentParser(
    description="Convert csv battles file into parquet.",
)
parser.add_argument(
    "-i",
    "--input",
    action="store",
    type=pathlib.Path,
    required=True,
    help="Input file: csv or csv.gz.",
)
parser.add_argument(
    "-o",
    "--output",
    action="store",
    type=pathlib.Path,
    help="Output path for .parquet.",
)
parser.add_argument(
    "-f",
    "--force",
    action="store_true",
    help="Overwrite output file.",
)

args = parser.parse_args()

if args.output is None:
    name: str = args.input.name.split(".")[0]
    args.output = args.input.with_name(f"{name}.parquet")

# CHECKS ------------------------------------------------------------------------------

args.output.parent.mkdir(parents=True, exist_ok=True)

csv_path: pathlib.Path = args.input
parquet_path: pathlib.Path = args.output

assert (
    not parquet_path.exists() or args.force
), f"{parquet_path} already exists. Use -f for overwrite it."
assert (
    parquet_path.suffix == ".parquet"
), "Output file will be a parquet file. Use .parquet as suffix for output."


# CONSTANTS ---------------------------------------------------------------------------

INFO = [0, 1]
INFO_TEAM = [2, 3, 4]
DECK_TEAM = [5, 6, 7, 8, 9, 10, 11, 12]
INFO_OPPONENT = [13, 14, 15]
DECK_OPPONENT = [16, 17, 18, 19, 20, 21, 22, 23]

with open(assets / "cards.json") as f:
    CARDS_INDEX = {str(card["id"]): i for i, card in enumerate(json.load(f))}


# READ AND PARSE CSV ------------------------------------------------------------------

converters = {
    # datetime parsed by parse_date = [0]
    1: np.int32,  # game_mode
    2: str,  # team tag
    3: np.int16,  # team trophies
    4: np.int8,  # team crowns
    **{i: CARDS_INDEX.get for i in DECK_TEAM},  # team deck
    13: str,  # opponent tag
    14: np.int16,  # opponent trophies
    15: np.int8,  # opponent crowns
    **{i: CARDS_INDEX.get for i in DECK_OPPONENT},  # opponent deck
}

battles = pd.read_csv(csv_path, header=None, converters=converters)
print(battles)
battles.iloc[:, DECK_TEAM] = battles.iloc[:, DECK_TEAM].fillna(0)
battles.iloc[:, DECK_OPPONENT] = battles.iloc[:, DECK_OPPONENT].fillna(0)



# Ensure the values are valid indices

# ENCODE DECKS ------------------------------------------------------------------------

decks_team = np.eye(256, dtype=bool)[battles.iloc[:, DECK_TEAM].astype(int)]
decks_team = np.logical_or.reduce(decks_team, axis=1)
decks_team = pd.DataFrame(decks_team)

print(decks_team)

decks_opponent = np.eye(128, dtype=bool)[battles.iloc[:, DECK_OPPONENT].astype(int)]
decks_opponent = np.logical_or.reduce(decks_opponent, axis=1)
decks_opponent = pd.DataFrame(decks_opponent)


# CREATE NEW DATAFRAME ----------------------------------------------------------------

battles = pd.concat(
    [
        battles.iloc[:, INFO],
        battles.iloc[:, INFO_TEAM],
        decks_team,
        battles.iloc[:, INFO_OPPONENT],
        decks_opponent,
    ],
    axis=1,
)

# columns = [
#     ("info", "datetime"),
#     ("info", "game_mode"),
#     ("team", "tag"),
#     ("team", "trophies"),
#     ("team", "crowns"),
#     *[("team", f"c{i}") for i in np.arange(128)],
#     ("opponent", "tag"),
#     ("opponent", "trophies"),
#     ("opponent", "crowns"),
#     *[("opponent", f"c{i}") for i in np.arange(128)],
# ]


columns = [
    ("info", "datetime"),
    ("info", "game_mode"),
    ("team", "tag"),
    ("team", "trophies"),
    ("team", "crowns"),
] + [("team", f"c{i}") for i in range(decks_team.shape[1])] + [
    ("opponent", "tag"),
    ("opponent", "trophies"),
    ("opponent", "crowns"),
] + [("opponent", f"c{i}") for i in range(decks_opponent.shape[1])]
print(battles.shape)
print(battles.columns)
battles.columns = pd.MultiIndex.from_tuples(columns)

# Print to verify the shape and column names

# SAVE TO PARQUET ---------------------------------------------------------------------

battles.to_parquet(parquet_path, engine="pyarrow", index=False)
