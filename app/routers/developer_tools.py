import aiohttp
import aiohttp.client_exceptions
from fastapi import Response, APIRouter
from starlette.concurrency import run_in_threadpool

from app.common.config import proxy, proxy_auth
from app.db.db_client import DbClient
from app.common.types import GameType
from app.background.live_games import (
    FutureGamesUpdateRuntime,
)
from app.common.types import GameShort
from app.globals import storage, game_update_manager


router = APIRouter()


@router.get("/tests/future-games/{sport_code}/")
async def get_future_games_test(sport_code: str):
    obj = FutureGamesUpdateRuntime()
    return {"data": await obj.get_future_games_by_sport(sport_code)}


@router.get("/tests/storage/")
async def get_storage_test():
    return storage._storage


@router.get("/tests/storage/{game_type:str}")
async def get_storage_games_by_type(game_type: GameType):
    return storage.get_games_by_type(game_type)


@router.get("/tests/game-changes/{sport_code}/{game_id}/")
async def get_game_changes_test(sport_code: str, game_id: int):
    game_short = GameShort(game_id=game_id, sport_code=sport_code.upper())
    change = await game_update_manager.get_game_changes(game_short)
    return change


@router.get("/games/future/", response_model=list[str])
async def get_future_games():
    db_client = DbClient()
    future_games = await run_in_threadpool(db_client.get_future_games)
    games_list = [
        f"Game ID: {game.id}, Sport Code: {game.sport_code}, "
        f"DateTime: {game.gamedatetime}, Status: {game.status}, "
        f"Visitor: {game.visitor_code} vs Home: {game.home_code}"
        for game in future_games
    ]
    return games_list


@router.get("/natstat/{path:path}")
async def proxy_get(path: str):
    target_url = f"https://interst.at/{path}"

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(
                target_url, timeout=60, proxy=proxy, proxy_auth=proxy_auth
            ) as response:
                content = await response.read()
                result = {
                    "status": response.status,
                    "content": content,
                    "headers": response.headers,
                }
        except aiohttp.ClientError as e:
            result = {"status": 500, "content": str(e).encode("utf-8"), "headers": {}}

    return Response(
        content=result["content"], status_code=result["status"], headers=result["headers"]
    )
