#!/usr/bin/env python3
# See https://gist.github.com/teasherm/bb73f21ed2f3b46bc1c2ca48ec2c1cf5
import argparse
import os

import boto3
import magic
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()


class S3MultipartUpload(object):
    # AWS throws EntityTooSmall error for parts smaller than 5 MB
    PART_MINIMUM = int(5e6)

    def __init__(self,
                 bucket,
                 key,
                 local_path,
                 content_type,
                 part_size=int(6e6),
                 profile_name=None,
                 region_name="eu-west-1",
                 verbose=False):
        self.bucket = bucket
        self.key = key
        self.path = local_path
        self.content_type = content_type
        self.total_bytes = os.stat(local_path).st_size
        self.part_bytes = part_size
        assert part_size > self.PART_MINIMUM
        # assert (self.total_bytes % part_size == 0
        #         or self.total_bytes % part_size > self.PART_MINIMUM)
        self.s3 = boto3.client('s3',
                               endpoint_url='http://192.168.16.5:9000',
                               aws_access_key_id='minioadmin',
                               aws_secret_access_key='minioadmin',
                               aws_session_token=None,
                               config=boto3.session.Config(
                                   signature_version='s3v4'),
                               verify=False
                               )
        # self.s3 = boto3.session.Session(
        #     profile_name=profile_name, region_name=region_name).client("s3")
        if verbose:
            boto3.set_stream_logger(name="botocore")

    def abort_all(self):
        mpus = self.s3.list_multipart_uploads(Bucket=self.bucket)  # Prefix=key
        aborted = []
        print("Aborting", len(mpus), "uploads")
        if "Uploads" in mpus:
            for u in mpus["Uploads"]:
                upload_id = u["UploadId"]  # also: Key
                aborted.append(
                    self.s3.abort_multipart_upload(
                        Bucket=self.bucket, Key=self.key, UploadId=upload_id))
        return aborted

    def get_uploaded_parts(self, upload_id):
        parts = []
        res = self.s3.list_parts(Bucket=self.bucket, Key=self.key,
                                 UploadId=upload_id)
        if "Parts" in res:
            for p in res["Parts"]:
                parts.append(p)  # PartNumber, ETag, Size [bytes], ...
        return parts

    def create(self):
        mpu = self.s3.create_multipart_upload(Bucket=self.bucket,
                                              Key=self.key,
                                              ContentType=self.content_type)
        mpu_id = mpu["UploadId"]
        return mpu_id

    def upload(self, mpu_id, parts=None):
        if parts is None:
            parts = []
        uploaded_bytes = 0
        with open(self.path, "rb") as f:
            i = 1
            while True:
                data = f.read(self.part_bytes)
                if not len(data):
                    break

                if len(parts) >= i:
                    # Already uploaded, go to the next one
                    part = parts[i - 1]
                    if len(data) != part["Size"]:
                        raise Exception("Size mismatch: local " + str(
                            len(data)) + ", remote: " + str(part["Size"]))
                    parts[i - 1] = {k: part[k] for k in ('PartNumber', 'ETag')}
                else:
                    part = self.s3.upload_part(
                        # We could include `ContentMD5='hash'` to discover if
                        # data has been corrupted upon transfer
                        Body=data,
                        Bucket=self.bucket,
                        Key=self.key,
                        UploadId=mpu_id,
                        PartNumber=i,
                    )

                    parts.append({"PartNumber": i, "ETag": part["ETag"]})

                uploaded_bytes += len(data)
                print("{0} of {1} bytes uploaded ({2:.3f}%)".format(
                    uploaded_bytes, self.total_bytes,
                    as_percent(uploaded_bytes, self.total_bytes)))
                i += 1
        return parts

    def complete(self, mpu_id, parts):
        # print("complete: parts=" + str(parts))
        result = self.s3.complete_multipart_upload(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=mpu_id,
            MultipartUpload={"Parts": parts})
        return result


# Helper
def as_percent(num, denom):
    return float(num) / float(denom) * 100.0


def parse_args():
    parser = argparse.ArgumentParser(description='Multipart upload')
    parser.add_argument('--bucket', required=True)
    parser.add_argument('--key', required=True)
    parser.add_argument('--path', required=True)
    parser.add_argument('--region', default="eu-west-1")
    parser.add_argument('--profile', default=None)
    parser.add_argument('--uploadid',
                        default=None)  # To continue a previous upload
    return parser.parse_args()


def main():
    # args = parse_args()
    file_path = '/home/dkarpele/Videos/sample-20s.mp4'
    file_name = file_path[file_path.rindex('/') + 1:]
    key = f'multipart-video/{file_name}'
    content_type = magic.from_file(file_path,
                                   mime=True)
    mpu = S3MultipartUpload(
        'asiatrip',
        key,
        file_path,
        content_type,
        profile_name=None,
        region_name='eu-west-3')
    # cont = "hksGYQapBfP3jBN4vPNp3avMD3AhME1yZReBTU11zBTtKLz2fwQ1WtYvxQJKTvn82mRFcfEIFllj2PCs52g3o6UYfM9hthPJPZ2JdIePIfE-" is not None
    cont = None
    if cont:
        mpu_id = 'hksGYQapBfP3jBN4vPNp3avMD3AhME1yZReBTU11zBTtKLz2fwQ1WtYvxQJKTvn82mRFcfEIFllj2PCs52g3o6UYfM9hthPJPZ2JdIePIfE-'
        print("Continuing upload with id=", mpu_id)
        finished_parts = mpu.get_uploaded_parts(mpu_id)
        parts = mpu.upload(mpu_id, finished_parts)
    else:
        # abort all multipart uploads for this bucket (optional,
        # for starting over)
        mpu.abort_all()
        # create new multipart upload
        mpu_id = mpu.create()
        print("Starting upload with id=", mpu_id)
        # upload parts
        parts = mpu.upload(mpu_id)

    # complete multipart upload
    print(mpu.complete(mpu_id, parts))


if __name__ == "__main__":
    main()
