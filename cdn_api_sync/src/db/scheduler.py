from typing import Any

from apscheduler.job import Job
from apscheduler.schedulers.background import BackgroundScheduler


scheduler: BackgroundScheduler | None = BackgroundScheduler()


def get_scheduler() -> BackgroundScheduler:
    return scheduler


def jobs(job: BackgroundScheduler, function: Any = None, *args, **kwargs)\
        -> None | Job:
    """
    List of jobs to schedule
    :param function: function to execute
    :param job: job name as a function
    :return:
    """
    if not function:
        return None
    job.add_job(function, *args, **kwargs)
