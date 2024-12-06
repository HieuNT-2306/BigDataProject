import argparse
import asyncio
import csv
import logging
import os
import pathlib
from datetime import datetime
from kafka import KafkaProducer
import json
import httpx
import tqdm
from crawler import Crawler
from dotenv import load_dotenv
from urllib.request import urlopen
import re as r
from urllib.request import urlopen
import re as r
# PATHS -------------------------------------------------------------------------------

here = pathlib.Path(__file__).parent
now = datetime.now().strftime("%Y%m%dT%H%M%S")

# ARGPARSE ----------------------------------------------------------------------------

parser = argparse.ArgumentParser(
    description="Collect Clash Royale battles from top ladder.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    "-q",
    "--quiet",
    action="store_true",
    default=False,
    help="Disable progress bar.",
)
parser.add_argument(
    "-p",
    "--players",
    action="store",
    metavar="X",
    type=int,
    default=float("inf"),
    help="Stop crawler after X players.",
)
parser.add_argument(
    "-rp",
    "--root-players",
    type=str,
    default=["9Y9RRPVC2", "Y9R22RQ2", "R90PRV0PY", "UYL8820RJ", "2L0UCYJC", "89G8JV8J", "L8QCPR8QY", "VC00GP0Y", "RRCC8G90R", "G9YV9GR8R"], 
    nargs="+",
    help="List of root players",
)
parser.add_argument(
    "-r",
    "--requests",
    action="store",
    metavar="Y",
    type=int,
    default=10,
    help="Perform up to Y http requests concurently.",
)
parser.add_argument(
    "-o",
    "--output",
    action="store",
    type=pathlib.Path,
    default=here.parent / "db" / "test" / f"{now}.csv",
    help="Output path for .csv.",
)
parser.add_argument(
    "-f",
    "--force",
    action="store_true",
    help="Overwrite output file.",
)
parser.add_argument(
    "-v",
    "--verbose",
    action="count",
    default=1,
    help="Increase console log level.",
)

args = parser.parse_args()

# convert v counts into logging level https://gist.github.com/ms5/9f6df9c42a5f5435be0e
args.verbose = 40 - (10 * args.verbose) if args.verbose > 0 else 0


# CONFIGS -----------------------------------------------------------------------------


args.output.parent.mkdir(parents=True, exist_ok=True)

csv_path: pathlib.Path = args.output
log_path: pathlib.Path = args.output.parent / "collect.log"

load_dotenv()
email = os.getenv("API_CLASH_ROYALE_EMAIL")
password = os.getenv("API_CLASH_ROYALE_PASSWORD")

assert (
    not csv_path.exists() or args.force
), f"{csv_path} already exists. Use -f for overwrite it."
assert (
    csv_path.suffix == ".csv"
), "Output file will be a csv file. Use .csv as suffix for output."
assert (
    email := email
), "API_CLASH_ROYALE_EMAIL env variable is not define"
assert (
    password := password
), "API_CLASH_ROYALE_PASSWORD env variable is not define"

def getIP():
    d = str(urlopen('http://checkip.dyndns.com/')
            .read())

    return r.compile(r'Address: (\d+\.\d+\.\d+\.\d+)').search(d).group(1)

ip = getIP()

def getIP():
    d = str(urlopen('http://checkip.dyndns.com/')
            .read())

    return r.compile(r'Address: (\d+\.\d+\.\d+\.\d+)').search(d).group(1)

ip = getIP()

credentials = {"email": email, "password": password}
api_key = {
    "name": "cr-analysis",
    "description": f"API key automatically generated at {now}",
    "cidrRanges": [ip],
    "scope": None,
}

with httpx.Client(base_url="https://developer.clashroyale.com") as client:
    client.post("/api/login", json=credentials)
    keys = client.post("/api/apikey/list", json={}).json().get("keys", [])
    if len(keys) == 10:
        client.post("/api/apikey/revoke", json={"id": keys[-1]["id"]})
    tmp = client.post("/api/apikey/create", json=api_key).json()
    api_token = tmp["key"]["key"]
    # api_token = client.post("/api/apikey/create", json=api_key).json()["key"]["key"]

battlelogs = Crawler(
    api_token=api_token,
    trophies_ranked_target=10_000,
    trophies_ladder_target=10_000,
    root_players=args.root_players,
    battlelogs_limit=args.players,
    concurrent_requests=args.requests,
    log_level_console=args.verbose,
    log_level_file=logging.INFO,
    log_file_path=log_path,
)


# MAIN --------------------------------------------------------------------------------


async def csv():
    battles_saved = set()

    progress_bar = tqdm.tqdm(
        total=args.players,
        disable=args.quiet,
        ncols=0,
        unit=" players",
        smoothing=0.05,
    )

    battlelogs.log.info("Start collecting ...")

    with open(csv_path, "a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        async for battles in battlelogs:
            for b in battles:
                if b.player1.tag > b.player2.tag:
                    battle = (b.battle_time, b.game_mode, *b.player1, *b.player2)
                else:
                    battle = (b.battle_time, b.game_mode, *b.player2, *b.player1)
                if (hb := hash(battle)) not in battles_saved:
                    writer.writerow(battle)
                    battles_saved.add(hb)
            progress_bar.update()
    progress_bar.close()

    battlelogs.log.info("End collecting.")


async def kafka():
    battles_saved = set()

    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize data to JSON
    )

    progress_bar = tqdm.tqdm(
        total=args.players,
        disable=args.quiet,
        ncols=0,
        unit=" players",
        smoothing=0.05,
    )

    battlelogs.log.info("Start collecting ...")

    async for battles in battlelogs:
        for b in battles:
            print(b)
            
            # Determine player order based on tags
            if b.player1.tag > b.player2.tag:
                battle = {
                    "battle_time": b.battle_time,
                    "game_mode": b.game_mode,
                    "player1": {
                        "tag": b.player1.tag,
                        "trophies": b.player1.trophies,
                        "crowns": b.player1.crowns,
                        "deck": [b.player1.card1, b.player1.card2, b.player1.card3, b.player1.card4,
                                b.player1.card5, b.player1.card6, b.player1.card7, b.player1.card8]
                    },
                    "player2": {
                        "tag": b.player2.tag,
                        "trophies": b.player2.trophies,
                        "crowns": b.player2.crowns,
                        "deck": [b.player2.card1, b.player2.card2, b.player2.card3, b.player2.card4,
                                b.player2.card5, b.player2.card6, b.player2.card7, b.player2.card8]
                    }
                }
            else:
                battle = {
                    "battle_time": b.battle_time,
                    "game_mode": b.game_mode,
                    "player1": {
                        "tag": b.player2.tag,
                        "trophies": b.player2.trophies,
                        "crowns": b.player2.crowns,
                        "deck": [b.player2.card1, b.player2.card2, b.player2.card3, b.player2.card4,
                                b.player2.card5, b.player2.card6, b.player2.card7, b.player2.card8]
                    },
                    "player2": {
                        "tag": b.player1.tag,
                        "trophies": b.player1.trophies,
                        "crowns": b.player1.crowns,
                        "deck": [b.player1.card1, b.player1.card2, b.player1.card3, b.player1.card4,
                                b.player1.card5, b.player1.card6, b.player1.card7, b.player1.card8]
                    }
                }

            # Check for duplicates
            if (hb := hash(json.dumps(battle))) not in battles_saved:
                # Send the battle data to Kafka
                producer.send('big_data_topic', battle)
                battles_saved.add(hb)
        
        progress_bar.update()

    progress_bar.close()
    producer.flush()  # Ensure all messages are sent
    producer.close()  # Close the producer

    battlelogs.log.info("End collecting.")

asyncio.run(kafka())


