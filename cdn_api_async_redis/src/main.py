import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Depends
from fastapi.responses import ORJSONResponse

from api.v1 import films
from core.config import settings
from core.logger import LOGGING
from db import redis
from db.scheduler import get_scheduler, add_startup_jobs
from services.redis import rate_limit


async def startup():
    redis.redis = redis.Redis(host=settings.redis_host,
                              port=settings.redis_port,
                              ssl=False)

    # Connecting to scheduler
    scheduler = await get_scheduler()
    await add_startup_jobs(scheduler, redis.redis)
    scheduler.start()
    logging.info(f'List of scheduled jobs: {scheduler.get_jobs()}')


async def shutdown():
    scheduler = await get_scheduler()
    scheduler.shutdown()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await startup()
    yield
    await shutdown()


app = FastAPI(
    title=settings.project_name,
    description="CDN API. Could be used to upload/download data to S3",
    version="1.0.0",
    docs_url='/api/openapi-cdn',
    openapi_url='/api/openapi-cdn.json',
    default_response_class=ORJSONResponse,
    lifespan=lifespan,
    dependencies=[Depends(rate_limit)])

app.include_router(films.router, prefix='/api/v1/films', tags=['films'])

if __name__ == '__main__':
    uvicorn.run(
        'main:app',
        host=f'{settings.host}',
        port=settings.port,
        log_config=LOGGING,
        log_level=logging.DEBUG
    )
