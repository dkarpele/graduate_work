from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    project_name: str = Field(..., env='PROJECT_NAME')
    redis_host: str = Field(..., env='REDIS_HOST')
    redis_port: int = Field(..., env='REDIS_PORT')
    elastic_host: str = Field(..., env='ELASTIC_HOST')
    elastic_port: int = Field(..., env='ELASTIC_PORT')
    es_indexes: str = Field(..., env='ELASTIC_INDEXES')
    es_id_field: str = 'id'
    service_url: str = Field('http://127.0.0.1:81', env='SERVICE_URL')
    auth_url: str = Field('http://127.0.0.1:80', env='AUTH_URL')

    class Config:
        env_file = '.env'


settings = Settings()
