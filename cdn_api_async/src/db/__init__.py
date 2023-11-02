from abc import ABC, abstractmethod
from typing import Optional

from minio.datatypes import Object
from pymongo.results import DeleteResult

from models.model import Model


class AbstractS3(ABC):
    """
    Abstract class to work with S3 storage
    """

    @abstractmethod
    def __init__(self, *args, **kwargs):
        self.endpoint: str | None = None
        self.client: Optional = None

    @abstractmethod
    async def get_url(self, *args, **kwargs) -> str:
        """
        Get presigned url with to download or preview data
        :return:
        """
        pass

    @abstractmethod
    async def bucket_exists(self, bucket_name) -> bool:
        pass

    @abstractmethod
    async def get_object(self,
                         bucket_name: str,
                         object_name: str,
                         s3=None,
                         offset: int = 0,
                         length: int = 1,
                         size: int = 1,
                         *args,
                         **kwargs) -> dict | bool:
        pass

    @abstractmethod
    async def stat_object(self,
                          bucket_name: str,
                          object_name: str
                          ) -> Object:
        pass

    @abstractmethod
    async def copy_object(self, source: str, destination: str) -> None:
        pass

    @abstractmethod
    async def fget_object(self,
                          bucket_name: str,
                          object_name: str,
                          file_name) -> bool:
        pass

    @abstractmethod
    async def remove_object(self,
                            bucket_name: str,
                            object_name: str,
                            ):
        pass


class AbstractStorage(ABC):
    """
    Абстрактный класс для работы с хранилищем данных.
    Описывает какие методы должны быть у подобных классов.
    get_by_id - возвращает один экземпляр класса модели,
    по которой строятся получение из хранилища
    get_list - возвращает список объектов модели, переданной в
    качестве параметра.
    """

    @abstractmethod
    async def insert_data(
            self,
            insert: Model | dict,
            collection: str,
    ) -> None:
        pass

    @abstractmethod
    async def get_data(
            self,
            query: dict | str,
            collection: str,
            projection: dict | None = None,
            sort: tuple | None = None,
            page: int | None = None,
            size: int | None = None
    ) -> list:
        pass

    @abstractmethod
    async def update_data(
            self,
            query: dict,
            update: dict,
            collection: str,
    ) -> None:
        pass

    @abstractmethod
    async def delete_data(
            self,
            document: Model | dict,
            collection: str,
    ) -> DeleteResult:
        pass