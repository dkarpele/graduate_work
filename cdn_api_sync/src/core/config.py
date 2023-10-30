from logging import config as logging_config

from pydantic import BaseSettings, Field

from core.logger import LOGGING

# Применяем настройки логирования
logging_config.dictConfig(LOGGING)


class MainConf(BaseSettings):
    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'


class Settings(MainConf):
    project_name: str = Field(..., env='PROJECT_NAME')
    # redis_host: str = Field(..., env='REDIS_HOST')
    # redis_port: int = Field(..., env='REDIS_PORT')
    # elastic_host: str = Field(..., env='ELASTIC_HOST')
    # elastic_port: int = Field(..., env='ELASTIC_PORT')
    host: str = Field(..., env='HOST_CDN')
    port: int = Field(..., env='PORT_CDN')
    bucket_name: str = Field(..., env='BUCKET_NAME')
    upload_part_size: int = Field(..., env='UPLOAD_PART_SIZE')
    ipapi_key: str = Field(..., env='IPAPI_KEY')
    # host_auth: str = Field(..., env='HOST_AUTH')
    # port_auth: str = Field(..., env='PORT_AUTH')
    # cache_expire_in_seconds: int = Field(..., env='CACHE_EXPIRE_IN_SECONDS')


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
