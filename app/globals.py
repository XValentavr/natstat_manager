from app.db.db_client import DbClient
from app.common.clients import NatstatClient
from app.background.live_games import (
    InmemoryStorage,
    GameUpdateManager,
    GameChangeConvertor,
)


storage = InmemoryStorage()
game_update_manager = GameUpdateManager(
    storage=storage,
    db_client=DbClient(),
    client=NatstatClient(),
    game_change_convertor=GameChangeConvertor(),
)
