#!/usr/bin/env python3
from __future__ import annotations
import argparse
import json

from imgserve.args import get_elasticsearch_args, get_s3_args
from imgserve.clients import get_clients
from imgserve.elasticsearch import get_response_value, fields_in_hits, all_field_values


def get_experiment_metadata(
    elasticsearch_client: Elasticsearch, experiment_name: str, debug: bool = False
) -> Dict[str, Any]:
    return {
        "colorgrams": get_response_value(
            elasticsearch_client=elasticsearch_client,
            index="colorgrams",
            query={
                "query": {
                    "bool": {"must": {"term": {"experiment_name": experiment_name}}}
                },
                "aggs": {"count": {"value_count": {"field": "query.keyword"}}},
            },
            value_keys=["aggregations", "count", "value"],
            debug=debug,
        ),
        "raw-images": get_response_value(
            elasticsearch_client=elasticsearch_client,
            index="raw-images",
            query={
                "query": {
                    "bool": {"must": {"term": {"experiment_name": experiment_name}}}
                },
                "aggs": {"count": {"value_count": {"field": "query"}}},
            },
            value_keys=["aggregations", "count", "value"],
            debug=debug,
        ),
        "dimensions": fields_in_hits(
            get_response_value(
                elasticsearch_client=elasticsearch_client,
                index="raw-images",
                query={
                    "query": {
                        "bool": {"must": {"term": {"experiment_name": experiment_name}}}
                    }
                },
                value_keys=["hits", "hits"],
                size=10,
                debug=debug,
            )
        ),
        "timestamps": set(ts[:10] for ts in get_response_value(
            elasticsearch_client=elasticsearch_client,
            index="raw-images",
            query={
                "query": {
                    "bool": {"must": {"term": {"experiment_name": experiment_name}}}
                },
                "aggs": {"dates": {"terms": {"field": "trial_timestamp"}}},
            },
            value_keys=["aggregations", "dates", "buckets", "*", "key_as_string"],
            debug=debug,
        ))
    }


def get_experiments(
    elasticsearch_client: Elasticsearch, debug: bool = False
) -> Dict[str, Any]:
    """
        Get list of all experiments from colorgrams index, associate metadata with each one to include in informational tooltip
    """
    experiment_names = all_field_values(
        elasticsearch_client,
        field="experiment_name",
        query={"query": {"match_all": {}}},
    )
    experiments = dict()
    for experiment_name in experiment_names:
        experiments.update(
            {
                experiment_name: get_experiment_metadata(
                    elasticsearch_client=elasticsearch_client,
                    experiment_name=experiment_name,
                    debug=debug,
                )
            }
        )
    return experiments


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    get_elasticsearch_args(parser)
    get_s3_args(parser)

    parser.add_argument(
        "--debug", action="store_true", help="print elasticsearch queries for debugging"
    )

    args = parser.parse_args()

    elasticsearch_client, s3_client = get_clients(args)

    experiments = get_experiments(elasticsearch_client, debug=args.debug)

    print(json.dumps(experiments, indent=2))
