import os
from logging import config as logging_config

from pydantic import BaseSettings, Field

from core.logger import LOGGING

logging_config.dictConfig(LOGGING)


class MainConf(BaseSettings):
    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'


class Settings(MainConf):
    project_name: str = Field(..., env='PROJECT_NAME')
    host: str = Field(..., env='HOST_CDN')
    port: int = Field(..., env='PORT_CDN')
    bucket_name: str = Field(..., env='BUCKET_NAME')
    upload_part_size: int = Field(..., env='UPLOAD_PART_SIZE')
    ipapi_key: str = Field(..., env='IPAPI_KEY')
    # host_auth: str = Field(..., env='HOST_AUTH')
    # port_auth: str = Field(..., env='PORT_AUTH')


settings = Settings()


class MongoCreds(MainConf):
    host: str = Field(..., env="MONGO_HOST")
    port: str = Field(..., env="MONGO_PORT")
    user: str = Field(default=None, env="MONGO_INITDB_ROOT_USERNAME")
    password: str = Field(default=None, env="MONGO_INITDB_ROOT_PASSWORD")
    db: str = Field(..., env="MONGO_INITDB_DATABASE")

    class Config:
        env_file = '.env'


mongo_settings = MongoCreds()


class RateLimit(MainConf):
    request_limit_per_minute: int = Field(env="REQUEST_LIMIT_PER_MINUTE",
                                          default=20)
    is_rate_limit: bool = (os.getenv('IS_RATE_LIMIT', 'False') == 'True')


rl = RateLimit()


class CronSettings:
    finish_in_progress_tasks: dict = {
        'hour': 18,
        'minute': 56,
        'second': 10,
        'timezone': 'UTC'
    }
    abort_old_tasks: dict = {
        'hour': 18,
        'minute': 49,
        'second': 30,
        'timezone': 'UTC'
    }


cron_settings = CronSettings()
