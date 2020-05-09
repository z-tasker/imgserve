from __future__ import annotations
import json
from pathlib import Path

import elasticsearch

from .logger import simple_logger

log = simple_logger("imgserve.elasticsearch")


def document_exists(
    elasticsearch_client: Elasticsearch,
    doc: Dict[str, Any],
    index: str,
    identity_fields: List[str],
    overwrite: bool,
) -> bool:

    query_filters = list()
    for field in identity_fields:
        query_filters.append(
            {"terms": {field: doc[field]}}
            if isinstance(doc[field], list)
            else {"term": {field: doc[field]}}
        )

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
    identity_fields: List[str],
    overwrite: bool,
) -> Generator[Dict[str, Any], None, None]:

    elasticsearch_client.indices.refresh(index=index, ignore_unavailable=True)

    yielded = 0
    exists = 0
    for doc in docs:
        if document_exists(
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
    identity_fields: List[str],
    overwrite: bool = False,
) -> None:

    elasticsearch.helpers.bulk(
        elasticsearch_client,
        doc_gen(elasticsearch_client, docs, index, identity_fields, overwrite),
    )

    log.info("bulk indexing complete!")
