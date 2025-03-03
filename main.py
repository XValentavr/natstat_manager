import asyncio
from contextlib import asynccontextmanager

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI
from hypercorn.asyncio import serve
from starlette.middleware.cors import CORSMiddleware

from app.common.config import hypercorn_config, settings
from app.common.logging import get_logger
from app.common.prometheus import init_prometheus
from app.routers.common import router as common_router
from app.routers.ready import router as ready_router
from app.routers.developer_tools import router as dev_tools_router
from app.background.live_games import (
    FutureGamesUpdateRuntime,
    FillInmemoryRuntime,
    CleanOldInmemoryRecordsRuntime,
    GameDetailsUpdateRuntime,
)
from app.globals import storage, game_update_manager
from app.background.populate_db.runtime import run_fetch_v3_games_data


@asynccontextmanager
async def lifespan(app: FastAPI):
    await launch_app()
    yield


async def init():
    settings.SERVICE_READY = True
    get_logger().info("Service is ready")


async def launch_app():
    asyncio.create_task(init())


app = FastAPI(lifespan=lifespan, title="Boilerplate fastapi project")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
init_prometheus(app, "natstat")


app.include_router(ready_router)
app.include_router(common_router)
app.include_router(dev_tools_router)


async def main():
    # be aware that uncaught exception is silenced until all tasks are finished (until app shutdown)
    tasks = [
        serve(app, hypercorn_config),
    ]
    await asyncio.gather(*tasks)


scheduler = BackgroundScheduler()

# scheduler.add_job(run_missing_games, trigger="date")
# scheduler.add_job(run_populate_data, trigger="date")
scheduler.add_job(run_fetch_v3_games_data, trigger="date")
scheduler.add_job(
    FutureGamesUpdateRuntime.run,
    trigger="date",
)
scheduler.add_job(
    FutureGamesUpdateRuntime.run,
    trigger="cron",
    hour=12,
)
scheduler.add_job(
    FillInmemoryRuntime.run,
    kwargs={"storage": storage},
    trigger="date",
)
scheduler.add_job(FillInmemoryRuntime.run, kwargs={"storage": storage}, trigger="cron", minute=10)
scheduler.add_job(CleanOldInmemoryRecordsRuntime.run, kwargs={"storage": storage}, trigger="date")
scheduler.add_job(
    GameDetailsUpdateRuntime.run,
    kwargs={"game_update_manager": game_update_manager},
    trigger="date",
)
scheduler.start()


@app.on_event("shutdown")
def shutdown_event():
    scheduler.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
