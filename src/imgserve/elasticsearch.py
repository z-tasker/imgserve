from __future__ import annotations
import copy
import json
from pathlib import Path

import elasticsearch
from elasticsearch_dsl import Search
from retry import retry

from .errors import (
    ElasticsearchUnreachableError,
    ElasticsearchNotReadyError,
    MissingTemplateError,
)
from .logger import simple_logger
from .utils import recurse_splat_key

log = simple_logger("imgserve.elasticsearch")


COLORGRAMS_INDEX_PATTERN = "colorgrams"
RAW_IMAGES_INDEX_PATTERN = "raw-images"
CROPPED_FACE_INDEX_PATTERN = "cropped-face"
MTURK_HIT_INDEX_PATTERN = "mturk-hit"


def _overridable_template_paths() -> Dict[str, Any]:
    template_paths = dict()
    for index in ["colorgrams", "raw-images", "hosts", "cropped-face-images", "mturk-hits"]:
        template = json.loads(
            Path(__file__).parents[2].joinpath(f"db/{index}.template.json").read_text()
        )
        assert (
            len(template) > 0
        ), f"the index template 'db/{index}.template.json' must exist"
        template_paths.update({index: template})

    return template_paths


def check_elasticsearch(
    elasticsearch_client: Elasticsearch,
    elasticsearch_fqdn: str,
    elasticsearch_port: str,
) -> None:
    try:
        health = elasticsearch_client.cluster.health()
        log.info(
            f"cluster at {elasticsearch_fqdn}:{elasticsearch_port} is called '{health['cluster_name']}' and is {health['status']}"
        )
        if health["status"] == "red":
            raise ElasticsearchNotReadyError("cluster is red")
    except elasticsearch.exceptions.ConnectionError as e:
        raise ElasticsearchUnreachableError(
            f"while attempting to connect to {elasticsearch_fqdn}:{elasticsearch_port}"
        ) from e


@retry(tries=3, backoff=5, delay=2)
def document_exists(
    elasticsearch_client: Elasticsearch,
    doc: Dict[str, Any],
    index: str,
    identity_fields: List[str],
    overwrite: bool = False,
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
                log.warning(f"{len(hits)} {index} documents matched the query: {body}")
            for hit in hits:
                log.info(
                    f"deleting existing {index} document matching query (id: {hit['_id']})"
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
        doc = dict(doc) # convert Index classes to plain dictionaries for Elasticsearch API
        if identity_fields is not None and document_exists(
            elasticsearch_client, doc, index, identity_fields, overwrite
        ):
            exists += 1
            continue
        doc.update(_index=index)
        yield doc
        yielded += 1
    log.info(
        f"{yielded} documents yielded for indexing to {index}"
        + (f" ({exists} already existed)" if exists > 0 else "")
    )


def fields_in_hits(hits: Iterator[Dict[str, Any]]) -> List[str]:

    fields = set()
    for hit in hits:
        for field in hit["_source"].keys():
            fields.add(field)

    return list(fields)


@retry(tries=3, backoff=5, delay=2)
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

    log.info("bulk indexing complete")


@retry(tries=3, backoff=5, delay=2)
def all_field_values(
    elasticsearch_client: Elasticsearch, field: str, query: Dict[str, Any], index_pattern: str = RAW_IMAGES_INDEX_PATTERN
) -> Generator[str, None, None]:

    s = Search(using=elasticsearch_client, index=index_pattern)
    agg = {"aggs": {"all_values": {"terms": {"field": field, "size": 100000}}}}
    agg["query"] = query["query"]
    s.update_from_dict(agg)
    resp = s.execute()
    unique_values = 0
    for item in resp.aggregations.all_values.buckets:
        yield item.key
        unique_values += 1
    log.debug(f"{unique_values} unique values for {field}")


@retry(tries=3, backoff=5, delay=2)
def get_response_value(
    elasticsearch_client: Elasticsearch,
    index: str,
    query: Dict[str, Any],
    value_keys: List[str],
    size: int = 0,
    debug: bool = False,
    drop_in: bool = False,
    composite_aggregation_name: Optional[str] = None,
) -> Union[Any, Generator[Any]]:
    log.info(f"retrieving value from query against {index} at {value_keys}")
    if debug:
        print(f"GET /{index}/_search?size={size}\n{json.dumps(query,indent=2)}")

    resp = elasticsearch_client.search(index=index, body=query, size=size)

    if composite_aggregation_name is not None:
        try:
            after_key = resp["aggregations"][composite_aggregation_name]["after_key"]
        except KeyError as exc:
            raise KeyError(
                f"No composite aggregation continuation key found at '{composite_aggregation_name}'"
            ) from exc
        values = 0
        while len(list(recurse_splat_key(resp, value_keys))) > 0:
            for value in recurse_splat_key(resp, value_keys):
                yield value
                values += 1

            query["aggregations"][composite_aggregation_name]["composite"].update(
                after=after_key
            )
            resp = elasticsearch_client.search(index=index, body=query, size=size)
        log.info(f"composite aggregation yielded {values} values")

    else:
        values = [value for value in recurse_splat_key(resp, value_keys)]

        if len(values) == 0:
            values = None
        elif len(values) == 1:
            yield values[0]
        else:
            yield from values

        log.info(f"query returned {len(values) if values is not None else 0} values")
