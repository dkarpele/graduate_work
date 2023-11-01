import logging
from typing import Any

from db import AbstractStorage, AbstractS3
from db.aws_s3 import S3MultipartUpload


class CDNService:
    def __init__(self, s3: AbstractS3):
        self.s3 = s3

    def download(self, bucket_name, object_name):
        self.s3.get_url(bucket_name=bucket_name,
                        object_name=object_name)


async def multipart_upload(storage: AbstractStorage,
                           upload_client: S3MultipartUpload,
                           origin_client: AbstractS3 | None = None,
                           origin_client_s3=None,
                           object_: Any = None,
                           collection: str = "api",
                           mpu_id: str = None):
    async with upload_client.client as s3:
        if mpu_id:
            logging.info(f"Continuing upload with id={mpu_id}")
            finished_parts: list = await upload_client.get_uploaded_parts(
                s3, mpu_id)
            # upload parts
            parts = await upload_client.upload_bytes(
                s3=s3,
                mpu_id=mpu_id,
                storage=storage,
                parts=finished_parts,
                origin_client=origin_client,
                origin_client_s3=origin_client_s3,
                object_=object_,
                collection=collection)
        else:
            # abort all multipart uploads for this bucket (optional,
            # for starting over)
            # await upload_client.abort_all(s3)

            # create new multipart upload
            mpu_id = await upload_client.create(s3)
            logging.info(f"Starting upload with id={mpu_id}")
            # upload parts
            parts = await upload_client.upload_bytes(
                s3=s3,
                mpu_id=mpu_id,
                storage=storage,
                origin_client=origin_client,
                origin_client_s3=origin_client_s3,
                object_=object_,
                collection=collection
            )

        # Complete object upload
        res = await upload_client.complete(s3, mpu_id, parts)

    # Uploading finished status to MongoDB
    object_name = upload_client.key
    query = {"object_name": object_name,
             "node": upload_client.endpoint}
    update = {"object_name": object_name,
              "status": "finished"}
    await storage.update_data(query=query,
                              update=update,
                              collection=collection)
    logging.info(f"Upload completed with metadata: "
                 f"{res}")
    return res
