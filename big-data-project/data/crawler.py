import asyncio
import logging
import pathlib
import sys
import time
from collections import namedtuple
from typing import Union

import aiohttp
import orjson
from heapdict import heapdict

"""
# Example: how to use Crawler

import asyncio
from crawler import Crawler

async def main():
    battlelogs = Crawler()
    async for battlelog in battlelogs:
        ...  # do your stuff with battlelog (e.g. save in a database)


asyncio.run(main())
"""

# Trophy Road
GAME_MODE_LADDER = {
    72000006: "Ladder",
    72000044: "Ladder_GoldRush",
    72000201: "Ladder_CrownRush",
}

# Path of Legends
GAME_MODE_RANKED = {
    72000323: "Ranked1v1",
    72000327: "Ranked1v1_GoldRush",
    72000328: "Ranked1v1_CrownRush",
    72000333: "PickMode_Ladder",  # MegaDraft in ranked
    72000345: "PickMode_Tournament_GoldRush",
    72000346: "PickMode_Tournament_CrownRush",
}

GAME_MODE_1V1 = {
    72000009: "Tournament",
    72000010: "Challenge",
    72000066: "Showdown_Ladder",
    72000007: "Friendly",
    72000291: "Challenge_AllCards_EventDeck",  # CC and GC
    **GAME_MODE_LADDER,
    **GAME_MODE_RANKED,
}


Priority = namedtuple(
    "Priority",
    ("ranked_trophies", "ladder_trophies", "last_ranked_battle", "last_ladder_battle"),
    defaults=[0, 0, "", ""],
)
Battle = namedtuple(
    "Battle",
    ("battle_time", "game_mode", "player1", "player2"),
)
Player = namedtuple(
    "Player",
    ("tag", "trophies", "crowns", *[f"card{_}" for _ in range(1, 9)]),
)


class Crawler:
    def __init__(
        self,
        api_token: str,
        # root_players: list[str] = ["G9YV9GR8R", "Y9R22RQ2", "R90PRV0PY", "RVCQ2CQGJ"],
        root_players: list[str] = ["G9YV9GR8R"],
        trophies_ranked_target: int = 10_000,
        trophies_ladder_target: int = 10_000,
        battlelogs_limit: Union[int, float] = float("inf"),
        battles_limit: Union[int, float] = float("inf"),
        concurrent_requests: int = 10,
        royaleapi_proxy: bool = False,
        log_level_console: int = logging.INFO,
        log_level_file: int = logging.ERROR,
        log_file_path: Union[pathlib.Path, None] = None,
    ) -> None:
        # Keep track of api requests
        self.players_queue = heapdict()
        self.root_players = root_players
        self.players_requested = set()
        self.pending_requests = dict()
        for root_player in self.root_players:
            self.players_queue[root_player] = Priority()

        # Set a limit on the numeber of iterations
        self.battlelogs_limit = battlelogs_limit
        self.battles_limit = battles_limit
        self.battlelog_counter = 0
        self.battles_counter = 0
        self.trophies_ranked_target = trophies_ranked_target
        self.trophies_ladder_target = trophies_ladder_target

        # Authentication and http client
        self.api_token = api_token
        self.headers = {"Authorization": f"Bearer {api_token}"}
        self.concurrent_requests = concurrent_requests
        self.royaleapi_proxy = royaleapi_proxy
        self.base_url = (
            "https://proxy.royaleapi.dev"
            if royaleapi_proxy
            else "https://api.clashroyale.com"
        )
        self.api_in_maintenance = False

        # Logger
        self.log_level_console = log_level_console
        self.log_level_file = log_level_file
        self.log_file_path = log_file_path
        self.log = self._setup_logger()

        # Ensure connection with clashroyale api
        asyncio.run(self._test_connection())

    def _setup_logger(self) -> logging.Logger:
        log = logging.getLogger(__name__)
        log.setLevel(logging.DEBUG)

        fmt = logging.Formatter(
            fmt="%(levelname)8s - %(asctime)s - %(message)s",
            datefmt="%x %X",
        )

        if self.log_file_path is not None:
            fh = logging.FileHandler(self.log_file_path)
            fh.setFormatter(fmt)
            fh.setLevel(self.log_level_file)
            log.addHandler(fh)

        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        sh.setLevel(self.log_level_console)
        log.addHandler(sh)

        return log

    async def _test_connection(self) -> None:
        async with aiohttp.ClientSession(
            self.base_url, headers=self.headers
        ) as session:
            async with session.get("/v1/cards") as resp:
                reason = await resp.json()
                if not resp.ok:
                    self.log.critical(f"Error code {resp.status} - {reason}")
                    sys.exit(f"Error code {resp.status} - {reason}")
                else:
                    self.log.info("Connection is ok, ready to collect.")

    async def _request_battlelog(self, player_tag: str) -> dict:
        url = f"/v1/players/%23{player_tag}/battlelog"
        try:
            async with self.session.get(url) as resp:
                if resp.status == 200:
                    return await resp.json(content_type=None, loads=orjson.loads)
                elif resp.status == 429:  # requestThrottled
                    msg = await resp.json()
                    self.log.error(f"{msg['reason']} - {msg['message']}")
                    # pause this requests for 5 seconds
                    await asyncio.sleep(5)
                    return {}
                elif resp.status == 503:
                    msg = await resp.json()
                    self.log.critical(f"{msg['reason']} - {msg['message']}")
                    self.api_in_maintenance = True
                    return {}
                else:
                    msg = await resp.json()
                    self.log.error(f"{msg['reason']} - {msg['message']}")
                    breakpoint()
                    return {}
        except aiohttp.ClientError as exc:
            self.log.error(exc)
            return {}

    def _parse_battle(self, battle: dict) -> Battle:
        try:
            battle_time = battle["battleTime"]
            game_mode = battle["gameMode"]["id"]
            p1, p2 = battle["team"][0], battle["opponent"][0]
            tag1, tag2 = p1["tag"][1:], p2["tag"][1:]
            # If startingTrophies or trophyChange is not present in json, their vaule
            # is assume to be 0 (hence .get with 0 as default).
            trophies1 = p1.get("startingTrophies", 0) + p1.get("trophyChange", 0)
            trophies2 = p2.get("startingTrophies", 0) + p2.get("trophyChange", 0)
            crowns1, crowns2 = p1["crowns"], p2["crowns"]
            deck1 = [card["id"] for card in p1["cards"]]
            deck2 = [card["id"] for card in p2["cards"]]
            return Battle(
                battle_time,
                game_mode,
                Player(tag1, trophies1, crowns1, *deck1),
                Player(tag2, trophies2, crowns2, *deck2),
            )
        except KeyError:
            self.log.error(f"KeyError while parsing {battle}")
            breakpoint()
            return Battle(None, None, None, None)

    def _update_players_queue(self, battles: list[Battle]) -> None:
        # Add player1 to players_requested
        _, _, p1, _ = battles[0]
        self.players_requested.add(p1.tag)

        # Update/Create priority for player2 in players_queue
        for battle in reversed(battles):
            battle_time, game_mode, _, p2 = battle

            if p2.tag in self.players_requested:
                continue

            if game_mode in GAME_MODE_RANKED and p2.trophies < 31:
                # filter out battles between top player and mediocre players
                continue

            if p2.tag in self.players_queue:
                p = self.players_queue[p2.tag]

                if game_mode in GAME_MODE_RANKED:
                    if p.last_ranked_battle < battle_time:
                        self.players_queue[p2.tag] = Priority(
                            abs(p2.trophies - self.trophies_ranked_target),
                            p.ladder_trophies,
                            battle_time,
                            p.last_ladder_battle,
                        )
                    self.log.debug(
                        f"Update player {p2.tag} ranked trophies ({p2.trophies})"
                    )

                elif game_mode in GAME_MODE_LADDER:
                    if p.last_ladder_battle < battle_time:
                        self.players_queue[p2.tag] = Priority(
                            p.ranked_trophies,
                            abs(p2.trophies - self.trophies_ladder_target),
                            p.last_ranked_battle,
                            battle_time,
                        )
                    self.log.debug(
                        f"Update player {p2.tag} ladder trophies ({p2.trophies})"
                    )
            else:
                if game_mode in GAME_MODE_RANKED:
                    self.players_queue[p2.tag] = Priority(
                        abs(p2.trophies - self.trophies_ranked_target),
                        0,
                        battle_time,
                        "",
                    )
                    self.log.debug(
                        f"Add player {p2.tag} with ranked trophies ({p2.trophies})"
                    )

                elif game_mode in GAME_MODE_LADDER:
                    self.players_queue[p2.tag] = Priority(
                        0,
                        abs(p2.trophies - self.trophies_ladder_target),
                        "",
                        battle_time,
                    )
                    self.log.debug(
                        f"Add player {p2.tag} with ladder trophies ({p2.trophies})"
                    )

    def __aiter__(self):
        # Move creation of aiohttp.ClientSession inside__aiter__ to avoid
        # RuntimeError: Timeout context manager should be used inside a task
        self.session = aiohttp.ClientSession(self.base_url, headers=self.headers)
        return self

    async def __anext__(self) -> list[Battle]:
        while (
            self.battlelog_counter < self.battlelogs_limit
            and self.battles_counter < self.battles_limit
            and not self.api_in_maintenance
        ):
            if (
                len(self.pending_requests) < self.concurrent_requests
                and len(self.players_queue) > 0
            ):
                player_tag, priority = self.players_queue.popitem()
                task = asyncio.create_task(self._request_battlelog(player_tag))
                self.pending_requests[task] = (player_tag, priority)
            else:
                done, _ = await asyncio.wait(
                    self.pending_requests.keys(),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in done:
                    if (
                        self.battlelog_counter >= self.battlelogs_limit
                        or self.battles_counter >= self.battles_limit
                    ):
                        break
                    player_tag, priority = self.pending_requests.pop(task)
                    battles = [
                        self._parse_battle(battle)
                        for battle in task.result()
                        if battle["gameMode"]["id"] in GAME_MODE_1V1
                    ]
                    if battles:
                        self._update_players_queue(battles)
                        self.log.debug(f"Found {len(battles)} battles for {player_tag}")
                        self.battles_counter += len(battles)
                        self.battlelog_counter += 1
                        return battles
                    else:
                        # ladder battlelog is empty hence does not update priority
                        self.log.debug(f"Empty battles for {player_tag}")
                        self.players_requested.add(player_tag)

        self.log.info(f"Requested {self.battlelog_counter} players")
        self.log.info(f"Found {self.battles_counter} battles")
        # stop gracefully: wait for pending http requests and close http session
        if self.pending_requests:
            await asyncio.wait(self.pending_requests.keys())
        await self.session.close()
        raise StopAsyncIteration


# Timers
def timer(function):
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = function(*args, **kwargs)
        end = time.perf_counter()
        print(f"{function.__name__:16s} tooks {end - start:.3f} sec")
        return result

    return wrapper


def atimer(function):
    async def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = await function(*args, **kwargs)
        end = time.perf_counter()
        print(f"{function.__name__:16s} tooks {end - start:.3f} sec")
        return result

    return wrapper
