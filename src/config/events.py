import json
import typing
import fastapi
from utils.logger import logger
from memory.metadata import metadata_map
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from router.events import (
    initialize_correlation,
    initialize_metadata,
    initialize_skew_benchmark,
    initialize_vol,
    initialize_atm_iv,
    initialize_strike_ls,
    initialize_fwd,
    initialize_intra_long_short,
    initialize_long_short,
    update_atm_iv_data,
    initialize_atr,
    initialize_surface_iv
    
)
from config.manager import settings

scheduler = BackgroundScheduler()


def start_scheduler() -> None:
    logger.info("Starting Scheduler")
    scheduler.add_job(
        update_atm_iv_data,
        IntervalTrigger(seconds=settings.ATM_UPDATE_INTERVAL / 1000),
        id="atm_iv_update",
    )
    scheduler.start()


def stop_scheduler() -> None:
    logger.info("Stopping Scheduler")
    scheduler.shutdown()


def execute_backend_server_event_handler(backend_app: fastapi.FastAPI) -> typing.Any:
    async def launch_backend_server_events() -> None:
        logger.info("Initializing metadata...")
        metadata = json.loads(initialize_metadata())
        for i in metadata:
            metadata_map.set_metadata(i["ticker"], i)

        logger.info("Initializing ATM Screener...")
        initialize_atm_iv()
        
        logger.info("Initializing Vol Screener...")
        initialize_vol()

        logger.info("Initializing Correlation Screener...")
        initialize_correlation()

        logger.info("Initializing fwd Screener...")
        initialize_fwd()

        logger.info("Initializing Skew Benchmark Screener...")
        initialize_skew_benchmark()
        
        logger.info("Initializing strike ls Screener...")
        initialize_strike_ls()

        logger.info("Initializing long short Screener...")
        initialize_long_short()

        logger.info("Initializing strike ls Screener...")
        initialize_intra_long_short()


        logger.info("Initializing ATR ls Screener...")
        initialize_atr()

        logger.info("Initializing Surface IV Screener...")
        initialize_surface_iv()
        logger.info("Initialization complete.")

    return launch_backend_server_events


def terminate_backend_server_event_handler(backend_app: fastapi.FastAPI) -> typing.Any:
    async def stop_backend_server_events() -> None:
        print("Shuting down")
        metadata_map.clear_metadata()

    return stop_backend_server_events
