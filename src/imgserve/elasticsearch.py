from __future__ import annotations
import json
from pathlib import Path

import elasticsearch

from .logger import simple_logger

log = simple_logger("imgserve.elasticsearch")


COLORGRAMS_INDEX_PATTERN = "colorgrams"
RAW_IMAGES_INDEX_PATTERN = "raw-images"

class ElasticsearchUnreachableError(Exception):
    pass

class ElasticsearchNotReadyError(Exception):
    pass

class MissingTemplateError(Exception):
    pass


def check_elasticsearch(elasticsearch_client: Elasticsearch, elasticsearch_fqdn: str, elasticsearch_port: str) -> None:
    try:
        health = elasticsearch_client.cluster.health()
        log.info(f"cluster at {elasticsearch_fqdn}:{elasticsearch_port} is called '{health['cluster_name']}' and is {health['status']}.")
        if health["status"] == "red":
            raise ElasticsearchNotReadyError("cluster is red")
    except elasticsearch.exceptions.ConnectionError as e:
        raise ElasticsearchUnreachableError(
            f"while attempting to connect to {elasticsearch_fqdn}:{elasticsearch_port}"
        ) from e


def _overridable_template_paths() -> Dict[str, Any]:
    template_paths = dict()
    for index in ["colorgrams", "raw-images", "hosts"]:
        template = json.loads(
            Path(__file__).parents[2].joinpath(f"db/{index}.template.json").read_text()
        )
        assert (
            len(template) > 0
        ), f"the index template 'db/{index}.template.json' must exist"
        template_paths.update({index: template})

    return template_paths


def document_exists(
    elasticsearch_client: Elasticsearch,
    doc: Dict[str, Any],
    index: str,
    identity_fields: List[str],
    overwrite: bool,
) -> bool:

    query_filters = list()
    for field in identity_fields:
        try:
            query_filters.append(
                {"terms": {field: doc[field]}}
                if isinstance(doc[field], list)
                else {"term": {field: doc[field]}}
            )
        except KeyError:
            # if the doc is missing an identity field, we will index the new document
            return False

    body = {"query": {"bool": {"filter": query_filters}}}

    try:
        resp = elasticsearch_client.search(index=index, body=body)
    except elasticsearch.exceptions.NotFoundError:
        return False

    hits = resp["hits"]["hits"]
    if len(hits) > 0:
        if overwrite:
            if len(hits) > 1:
                log.warning(
                    f"{len(hits)} colorgram documents matched the query: {body}"
                )
            for hit in hits:
                log.info(
                    f"deleting existing colorgram document matching query (id: {hit['_id']})"
                )
                elasticsearch_client.delete(index=index, id=hit["_id"])
                return False
        else:
            return True
    else:
        return False


def doc_gen(
    elasticsearch_client: Elasticsearch,
    docs: List[Dict[str, Any]],
    index: str,
    identity_fields: Optional[List[str]],
    overwrite: bool,
) -> Generator[Dict[str, Any], None, None]:

    if identity_fields is not None:
        # must have manage permission on index to refresh, this is only necessary for idempotent indexing calls
        elasticsearch_client.indices.refresh(index=index, ignore_unavailable=True)

    yielded = 0
    exists = 0
    for doc in docs:
        if identity_fields is not None and document_exists(
            elasticsearch_client, doc, index, identity_fields, overwrite
        ):
            exists += 1
            continue
        doc.update(_index=index)
        yield doc
        yielded += 1
    log.info(
        f"{yielded} documents yielded for indexing to {index}."
        + (f" ({exists} already existed)" if exists > 0 else "")
    )


def index_to_elasticsearch(
    elasticsearch_client: Elasticsearch,
    index: str,
    docs: Iterator[Dict[str, Any]],
    identity_fields: Optional[List[str]] = None,
    overwrite: bool = False,
    apply_template: bool = False,
) -> None:

    if apply_template:
        try:
            elasticsearch_client.indices.put_template(
                name=index, body=_overridable_template_paths()[index]
            )
        except KeyError as e:
            raise MissingTemplateError(
                f"no index template for {index}, please add one to db/{index}.template.json and update '_overridable_template_paths' in src/imgserve/elasticsearch.py"
            ) from e

    elasticsearch.helpers.bulk(
        elasticsearch_client,
        doc_gen(elasticsearch_client, docs, index, identity_fields, overwrite),
    )

    log.info("bulk indexing complete!")
