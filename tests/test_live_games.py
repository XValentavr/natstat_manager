import json
import pytest
from datetime import datetime, timedelta

from app.common.utils import get_utc_now
from app.db.database import Game
from app.db.db_client import DbClient
from app.common.clients import NatstatClient
from app.background.live_games import (
    FutureGamesUpdateRuntime,
    GameUpdateManager,
    GameChangeConvertor,
    InmemoryStorage,
)
from app.common.types import GameChanges, GameType, GameShort, GameLastInfo


# gamedatetime in fixture in NY timezone
gamedatetime = datetime.strptime("2023-10-24 23:30", "%Y-%m-%d %H:%M")
dummy_gshort = GameShort(game_id=1, sport_code="NBA")


def get_file_content(filename: str) -> dict:
    with open(f"tests/fixture/{filename}", "r") as file:
        return json.load(file)


@pytest.fixture
def valid_games_response():
    return get_file_content("nba_future_games_response.json")


@pytest.fixture
def no_data_games_response():
    return get_file_content("games_list_no_data.json")


@pytest.fixture
def game_data_response():
    return get_file_content("nba_1077408.json")


@pytest.fixture
def storage():
    return InmemoryStorage()


@pytest.fixture
def game():
    return Game(
        id=1,
        sport_code="NBA",
        gamedatetime=datetime.now(),
        status="Scheduled",
        score_visitor=100,
        score_home=120,
        score_overtime=None,
    )


@pytest.fixture
def old_game():
    return Game(
        id=2,
        sport_code="NBA",
        gamedatetime=datetime.now() - timedelta(hours=6),
        status="Scheduled",
        score_visitor=90,
        score_home=110,
        score_overtime=None,
    )


def get_game_update_manager() -> GameUpdateManager:
    return GameUpdateManager(
        storage=InmemoryStorage(),
        db_client=DbClient(),
        client=NatstatClient(),
        game_change_convertor=GameChangeConvertor(),
    )


###############################
## Future games list update
###############################


def test_valid_future_games_response(valid_games_response):
    obj = FutureGamesUpdateRuntime()
    _, data = obj.parse_games_response(valid_games_response)
    assert len(data) == 12
    assert data[0]["id"] == "1079271"


def test_no_data_future_games_response(no_data_games_response):
    obj = FutureGamesUpdateRuntime()
    _, data = obj.parse_games_response(no_data_games_response)
    assert len(data) == 0


def test_none_future_games_response():
    obj = FutureGamesUpdateRuntime()
    _, data = obj.parse_games_response(None)
    assert len(data) == 0


###############################
## Storage
###############################


def test_fill_inmemory_storage(storage, game):
    storage.fill_inmemory_storage([game])
    key = f"{game.sport_code}_{game.id}"
    assert key in storage._storage
    assert storage._storage[key].game_short.game_id == game.id
    assert storage._storage[key].game_short.sport_code == game.sport_code


def test_clean_old_records(storage, game, old_game):
    storage.fill_inmemory_storage([game, old_game])
    storage.clean_old_records()
    key_old = f"{old_game.sport_code}_{old_game.id}"
    key_new = f"{game.sport_code}_{game.id}"
    assert key_old not in storage._storage
    assert key_new in storage._storage


def test_get_games_by_type(storage, game):
    storage.fill_inmemory_storage([game])
    game_type = GameType.live
    games_short = storage.get_games_by_type(game_type)
    assert len(games_short) == 1
    assert games_short[0].game_id == game.id
    assert games_short[0].sport_code == game.sport_code


def test_get_last_info(storage, game):
    storage.fill_inmemory_storage([game])
    key = f"{game.sport_code}_{game.id}"
    game_last_info = storage.get_last_info(key)
    assert game_last_info is not None
    assert game_last_info.game_short.game_id == game.id


def test_update_games_list(storage, game):
    storage.fill_inmemory_storage([game])
    game_changes = GameChanges(
        game_short=GameShort(game_id=game.id, sport_code=game.sport_code),
        game_data={},
        status="Final",
        gamedatetime=None,
        score_visitor=None,
        score_home=None,
        score_overtime=None,
        playbyplay_changes=None,
    )
    storage.update_games_list([game_changes])
    key = f"{game.sport_code}_{game.id}"
    assert storage._storage[key].status == "Final"


def test_get_game_current_status(storage, game):
    storage.fill_inmemory_storage([game])
    key = f"{game.sport_code}_{game.id}"
    status = storage.get_game_current_status(key)
    assert status == game.status


###############################
## Game Details Update
###############################


def test_game_update_response_to_game_data(game_data_response):
    obj = get_game_update_manager()
    game_data = obj.response_to_game_data(game_data_response)
    assert game_data is not None
    assert game_data["id"] == "1077408"
    assert len(game_data["stats"]["playbyplay"]) == 450


@pytest.mark.parametrize(
    "case",
    [
        # All changed
        {
            "last_info": GameLastInfo(
                game_short=dummy_gshort, gamedatetime=get_utc_now(), status="Scheduled"
            ),
            "expected_status": "Final",
            "expected_gamedatetime": gamedatetime,
            "expected_score_home": 119,
            "expected_score_visitor": 107,
            "expected_score_overtime": "N",
        },
        # last info is None (all info is changed)
        {
            "last_info": None,
            "expected_status": "Final",
            "expected_gamedatetime": gamedatetime,
            "expected_score_home": 119,
            "expected_score_visitor": 107,
            "expected_score_overtime": "N",
        },
        # no change
        {
            "last_info": GameLastInfo(
                game_short=dummy_gshort,
                gamedatetime=gamedatetime,
                status="Final",
                score_home=119,
                score_visitor=107,
                score_overtime="N",
            ),
            "expected_status": None,
            "expected_gamedatetime": None,
            "expected_score_home": None,
            "expected_score_visitor": None,
            "expected_score_overtime": None,
        },
    ],
)
def test_game_change_convertor(game_data_response, case):
    obj = get_game_update_manager()
    game_data = obj.response_to_game_data(game_data_response)

    assert game_data is not None

    game_change_convertor = GameChangeConvertor()
    game_change = game_change_convertor.get_game_changes(dummy_gshort, game_data, case["last_info"])

    assert game_change.status == case["expected_status"]
    assert game_change.gamedatetime == case["expected_gamedatetime"]
    assert game_change.score_home == case["expected_score_home"]
    assert game_change.score_visitor == case["expected_score_visitor"]
    assert game_change.score_overtime == case["expected_score_overtime"]


@pytest.mark.parametrize(
    "case",
    [
        {
            "name": "playbyplay not in game_data",
            "last_info": GameLastInfo(
                game_short=dummy_gshort, gamedatetime=get_utc_now(), status="Final"
            ),
            "remove_pbp_from_data": True,
            "expect_pbp": False,
            "expected_pbp": None,
        },
        {
            "name": "last_info is None",
            "last_info": None,
            "expect_pbp": True,
            "expected_pbp_len": 450,
        },
        {
            "name": "last_info playbyplays field is None",
            "last_info": GameLastInfo(
                game_short=dummy_gshort,
                gamedatetime=get_utc_now(),
                status="Final",
                playbyplay_ids=None,
            ),
            "expect_pbp": True,
            "expected_pbp_len": 450,
        },
        {
            "name": "change in playbyplay",
            "last_info": GameLastInfo(
                game_short=dummy_gshort,
                gamedatetime=get_utc_now(),
                status="Final",
                playbyplay_ids={
                    40053045,
                },
            ),
            "expect_pbp": True,
            "expected_pbp_len": 449,
        },
    ],
)
def test_get_play_by_play_changes(game_data_response, case):
    obj = get_game_update_manager()

    game_data = obj.response_to_game_data(game_data_response)
    assert game_data is not None

    if case.get("remove_pbp_from_data", False):
        del game_data["stats"]["playbyplay"]

    convertor = GameChangeConvertor()
    last_info = case["last_info"]
    result = convertor.get_play_by_play_changes(last_info, game_data)

    if case["expect_pbp"]:
        assert result is not None
        assert len(result) == case["expected_pbp_len"]
        if last_info is not None and last_info.playbyplay_ids:
            assert all(int(item["id"]) not in last_info.playbyplay_ids for item in result)
    else:
        assert result is None
