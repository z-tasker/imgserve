from __future__ import annotations
import copy
import json
import requests
from dataclasses import dataclass
from pathlib import Path

from elasticsearch import helpers

from .elasticsearch import (
    RAW_IMAGES_INDEX_PATTERN,
    COLORGRAMS_INDEX_PATTERN,
    get_response_value,
)
from .errors import APIError, MissingCredentialsError, UnexpectedStatusCodeError
from .logger import simple_logger
from .s3 import get_s3_bytes


class RawImageDocument:
    def __init__(self, doc: Dict[str, Any]) -> None:
        self.doc = doc
        self.source = self.doc["_source"]
        self.path = (
            Path("data")
            .joinpath(self.source["trial_id"])
            .joinpath(self.source["hostname"])
            .joinpath(self.source["query"].replace(" ", "_"))
            .joinpath(self.source["trial_timestamp"])
            .joinpath("images")
            .joinpath(self.source["image_id"])
            .with_suffix(".jpg")
        )


class ColorgramDocument:
    def __init__(self, doc: Dict[str, Any]) -> None:
        self.doc = doc
        self.source = self.doc["_source"]
        self.path = Path(self.source["experiment_name"]).joinpath(self.source["s3_key"])


@dataclass
class Experiment:
    bucket_name: str
    elasticsearch_client: Elasticsearch
    local_data_store: Path
    name: str
    s3_client: botocore.client.s3
    query: Optional[Dict[str, Any]] = None
    dry_run: bool = False
    debug: bool = False

    def __post_init__(self) -> None:
        if self.query is None:
            self.query = {
                "query": {
                    "bool": {"filter": [{"term": {"experiment_name": self.name}}]}
                }
            }
        self.log = simple_logger(
            f"imgserve.{self.name}" + (f".DRY_RUN" if self.dry_run else "")
        )
        self.log.info(f"initialized")

    def _sync_s3_path(self, path: Path) -> Path:
        local_path = self.local_data_store.joinpath(path)
        if not local_path.is_file():
            local_path.parent.mkdir(exist_ok=True, parents=True)
            local_path.write_bytes(
                get_s3_bytes(
                    s3_client=self.s3_client, bucket_name=self.bucket_name, s3_path=path
                )
            )
        return local_path

    def _delete_s3_object(self, s3_path: Path) -> None:
        self.s3_client.delete_object(Bucket=self.bucket_name, Key=str(s3_path))

    @property
    def raw_images(self) -> Generator[RawImageDocument]:
        yielded = 0
        for doc in helpers.scan(
            self.elasticsearch_client, index=RAW_IMAGES_INDEX_PATTERN, query=self.query
        ):
            raw_image_document = RawImageDocument(doc)
            yield raw_image_document

    @property
    def colorgrams(self) -> Generator[ColorgramDocuments]:
        for doc in helpers.scan(
            self.elasticsearch_client, index=COLORGRAMS_INDEX_PATTERN, query=self.query,
        ):
            colorgram_document = ColorgramDocument(doc)
            yield colorgram_document

    def get(self, word: str) -> Generator[Tuple[Dict[str, Any], Path], None, None]:
        """
            Get all images associated with a given word for this experiment
        """
        docs = get_response_value(
            elasticsearch_client=self.elasticsearch_client,
            index="colorgrams",
            query={"query": {"bool": { "filter": [{"term": {"query": word }}, {"term": {"experiment_name": self.name}}]}}},
            value_keys=["hits", "hits"],
            size=100,
            debug=self.debug
        )
        if len(docs) == 0:
            raise FileNotFoundError(
                f"No colorgram for {word} from {self.name} found!"
            )
        if len(docs) > 1:
            self.log.info(f"more than one colorgram for {word}")

        for doc in docs:
            yield (doc, self._sync_s3_path(ColorgramDocument(doc).path))


    def delete(self) -> None:
        self.log.info(f"deleting raw-images from S3...")
        deleted = 0
        for raw_image_document in self.raw_images:
            if not self.dry_run:
                try:
                    self._delete_s3_object(raw_image_document.path)
                except self.s3_client.exceptions.NoSuchKey:
                    deleted -= 1
            deleted += 1
            if raw_image_document.path.is_file():
                raw_image_document.path.unlink()
        self.log.info(f"deleted {deleted} raw images from s3")

        self.log.info(f"deleting colorgrams from S3...")
        deleted = 0
        for colorgram_document in self.colorgrams:
            if not self.dry_run:
                try:
                    self._delete_s3_object(colorgram_document.path)
                except self.s3_client.exceptions.NoSuchKey:
                    deleted -= 1
            deleted += 1
            if colorgram_document.path.is_file():
                colorgram_document.path.unlink()
        self.log.info(f"deleted {deleted} colorgrams from s3")

        self.log.info(f"deleting documents from elasticsearch...")
        delete_query = copy.deepcopy(self.query)

        if not self.dry_run:
            resp = self.elasticsearch_client.delete_by_query(
                index="_all", body=delete_query
            )
            self.log.info(resp)
        else:
            delete_query.update(
                {"aggs": {"count": {"value_count": {"field": "query"}}}}
            )
            would_delete = get_response_value(
                elasticsearch_client=self.elasticsearch_client,
                index="_all",
                query=delete_query,
                value_keys=["aggregations", "count", "value"],
                debug=self.debug
            )
            self.log.info(f"would delete {would_delete} documents from elasticsearch")

    def pull(self, pull_raw_images: bool = False) -> None:
        self.log.info(
            "pulling colorgrams" + (" and raw-images" if pull_raw_images else "")
        )
        pulled_colorgrams = 0
        for colorgram_document in self.colorgrams:
            if not self.dry_run:
                self._sync_s3_path(colorgram_document.path)
            pulled_colorgrams += 1
        self.log.info(
            f"pulled {pulled_colorgrams} colorgrams to {self.local_data_store}"
        )

        if pull_raw_images:
            pulled_raw_images = 0
            for raw_image_document in self.raw_images:
                if not self.dry_run:
                    self._sync_s3_path(raw_image_document.path)
                pulled_raw_images += 1
            self.log.info(
                f"pulled {pulled_raw_images} raw images to {self.local_data_store}"
            )


class ImgServe:
    def __init__(self, remote_url: str, username: str = "", password: str = "") -> None:

        local = remote_url.startswith("http://localhost")
        if not local:
            if username == "" or password == "":
                raise MissingCredentialsError(
                    "must set remote username and password when using {remote_url}"
                )

        self.remote_url = remote_url
        self.auth = (
            requests.auth.HTTPBasicAuth(username, password) if not local else None
        )
        self.log = simple_logger("ImgServe" + " local" if local else " remote")

    def get_experiment(self, name: str) -> Dict[str, Any]:
        try:
            response = requests.get(
                f"{self.remote_url}/experiments/{name}", auth=self.auth
            )
            if response.status_code != 200:
                raise UnexpectedStatusCodeError(
                    f"{response.status_code} from {self.remote_url}: {response.text}"
                )
            resp = json.loads(response.text)
        except requests.exceptions.ConnectionError as e:
            raise APIError(
                f"connection to {self.remote_url} failed, is it running?"
            ) from e

        return resp
