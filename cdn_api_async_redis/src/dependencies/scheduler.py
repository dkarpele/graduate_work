from functools import lru_cache
from typing import Annotated

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import Depends

from connectors.scheduler import get_scheduler


@lru_cache()
def get_scheduler_service(
        scheduler: AsyncIOScheduler = Depends(get_scheduler)) \
        -> AsyncIOScheduler:
    return scheduler


SchedulerDep = Annotated[AsyncIOScheduler, Depends(get_scheduler_service)]
