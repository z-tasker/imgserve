from __future__ import annotations
import io
from pathlib import Path

import PIL

from .errors import S3Error
from .logger import simple_logger

log = simple_logger("imgserve.s3")


def s3_put_image(
    s3_client: botocore.clients.s3,
    image: Union[PIL.Image, Path, bytes],
    bucket: str,
    object_path: Path,
    overwrite: bool = False,
) -> None:

    if isinstance(image, PIL.Image.Image):
        image_bytes = io.BytesIO()
        image.save(image_bytes, format="PNG")
        image_bytes = image_bytes.getvalue()
    elif isinstance(image, Path):
        image_bytes = image.read_bytes()
    elif isinstance(image, bytes):
        image_bytes = image
    else:
        raise ValueError(f"{image} is not a known type")

    try:
        # only write images to s3 that don't already exist unless overwrite is passed
        try:
            s3_client.get_object(Bucket=bucket, Key=str(object_path))
            if not overwrite:
                log.debug(f"{object_path} already exists in s3, not overwriting")
                return
        except s3_client.exceptions.NoSuchKey:
            pass

        s3_client.put_object(Body=image_bytes, Bucket=bucket, Key=str(object_path))
        log.info(f"uploaded {object_path} to s3.")
    except s3_client.exceptions.ClientError:
        s3_client_attributes = {
            attr: getattr(s3_client, attr) for attr in s3_client.__dict__.keys()
        }
        s3_client_attributes.update({
            "bucket": bucket,
            "object_path": object_path,
        })
        raise S3Error(f"{s3_client_attributes} S3 ClientError")


def get_s3_bytes(
    s3_client: botocore.clients.s3, bucket_name: str, s3_path: Path
) -> bytes:
    return s3_client.get_object(Bucket=bucket_name, Key=str(s3_path))["Body"].read()
