from __future__ import annotations
import io
from pathlib import Path

from .logger import simple_logger

log = simple_logger("imgserve.s3")


def s3_put_image(
    s3_client: botocore.clients.s3,
    image: PIL.Image,
    bucket: str,
    object_path: Path,
    overwrite: bool = False,
) -> None:

    image_bytes = io.BytesIO()
    image.save(image_bytes, format="PNG")

    # only write images to s3 that don't already exist unless overwrite is passed
    try:
        s3_client.get_object(Bucket=bucket, Key=str(object_path))
        if not overwrite:
            log.debug(f"{object_path} already exists in s3, not overwriting")
            return
    except s3_client.exceptions.NoSuchKey:
        pass

    s3_client.put_object(
        Body=image_bytes.getvalue(), Bucket=bucket, Key=str(object_path)
    )
    log.info(f"uploaded {object_path} to s3.")
