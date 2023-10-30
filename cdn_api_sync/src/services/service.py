import logging
from typing import Optional, Any

from db import AbstractStorage, AbstractCache, AbstractS3
from db.aws_s3 import S3MultipartUpload


class CDNService:
    def __init__(self, s3: AbstractS3):
        self.s3 = s3

    def download(self, bucket_name, object_name):
        self.s3.get_url(bucket_name=bucket_name,
                        object_name=object_name)


class IdRequestService:
    def __init__(self, cache: AbstractCache, storage: AbstractStorage, model):
        self.cache = cache
        self.storage = storage
        self.model = model

    async def process_by_id(self, _id: str, index: str) -> Optional:
        entity = await self.cache.get_from_cache_by_id(_id=_id,
                                                       model=self.model)
        if not entity:
            entity = await self.storage.get_by_id(_id, index, self.model)
            if not entity:
                return None
            await self.cache.put_to_cache_by_id(entity=entity)

        return entity


class ListService:
    def __init__(self, cache: AbstractCache, storage: AbstractStorage, model):
        self.cache = cache
        self.storage = storage
        self.model = model

    async def process_list(self,
                           index: str,
                           sort: str = None,
                           search: dict = None,
                           key: str = None,
                           page: int = None,
                           size: int = None) -> Optional:

        if key:
            entities = await self.cache.get_from_cache_by_key(model=self.model,
                                                              key=key,
                                                              sort=sort)
        else:
            entities = None

        if not entities:
            entities = await self.storage.get_list(self.model,
                                                   index,
                                                   sort,
                                                   search,
                                                   page,
                                                   size)
            if not entities:
                return None
            if key:
                await self.cache.put_to_cache_by_key(key, entities)

        return entities


def multipart_upload(storage: AbstractStorage,
                     upload_client: S3MultipartUpload,
                     origin_client: AbstractS3 | None = None,
                     object_: Any = None,
                     collection: str = "api",
                     mpu_id: str = None):
    if mpu_id:
        logging.info(f"Continuing upload with id={mpu_id}")
        finished_parts: list = upload_client.get_uploaded_parts(mpu_id)
        # upload parts
        parts = upload_client.upload_bytes(mpu_id,
                                           storage=storage,
                                           parts=finished_parts,
                                           origin_client=origin_client,
                                           object_=object_,
                                           collection=collection)
    else:
        # abort all multipart uploads for this bucket (optional,
        # for starting over)
        # upload_client.abort_all()

        # create new multipart upload
        mpu_id = upload_client.create()
        logging.info(f"Starting upload with id={mpu_id}")
        # upload parts
        parts = upload_client.upload_bytes(mpu_id,
                                           storage=storage,
                                           origin_client=origin_client,
                                           object_=object_,
                                           collection=collection
                                           )

    # Complete object upload
    res = upload_client.complete(mpu_id, parts)

    # Uploading finished status to MongoDB
    object_name = upload_client.key
    query = {"object_name": object_name,
             "node": upload_client.client.meta.endpoint_url}
    update = {"object_name": object_name,
              "status": "finished"}
    storage.update_data(query=query,
                        update=update,
                        collection=collection)
    logging.info(f"Upload completed with metadata: "
                 f"{res}")
    return res
