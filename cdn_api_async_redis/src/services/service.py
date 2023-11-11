import logging
from datetime import datetime

from fastapi import UploadFile

from connectors.abstract import AbstractS3, AbstractCache
from connectors.aws_s3 import S3MultipartUpload
from models.model import Status


async def multipart_upload(cache: AbstractCache,
                           upload_client: S3MultipartUpload,
                           status: str,
                           origin_client: AbstractS3 | None = None,
                           origin_client_s3=None,
                           object_: UploadFile = None,
                           collection: str = "api",
                           mpu_id: str = None,
                           ):
    async with upload_client.client as s3:
        if mpu_id:
            logging.info(f"Continuing upload with id={mpu_id}")
            finished_parts: list = await upload_client.get_uploaded_parts(
                s3, mpu_id)
            # upload parts
            parts = await upload_client.upload_bytes(
                s3=s3,
                mpu_id=mpu_id,
                cache=cache,
                status_=status,
                parts=finished_parts,
                origin_client=origin_client,
                origin_client_s3=origin_client_s3,
                object_=object_,
                collection=collection,
                )
        else:
            # create new multipart upload
            mpu_id = await upload_client.create(s3)
            logging.info(f"Starting upload with id={mpu_id}")
            # upload parts
            parts = await upload_client.upload_bytes(
                s3=s3,
                mpu_id=mpu_id,
                cache=cache,
                status_=status,
                origin_client=origin_client,
                origin_client_s3=origin_client_s3,
                object_=object_,
                collection=collection,
                )

        # Complete object upload
        res = await upload_client.complete(s3, mpu_id, parts)

    # Uploading finished status to cache
    object_name = upload_client.key
    status_ = Status.FINISHED.value
    key = f"{collection}^{object_name}^{upload_client.endpoint}"
    entity = {"last_modified": str(datetime.utcnow()),
              "status": status_}
    await cache.put_to_cache_by_key(key, entity)

    logging.info(f"Upload completed with metadata: "
                 f"{res}")
    return res
