#!/usr/bin/env python3
from __future__ import annotations
import argparse
import copy
import hashlib
import json
from pathlib import Path

import requests
import xmltodict
from elasticsearch.helpers import scan
from tqdm import tqdm

from imgserve.api import Experiment, MturkHitDocument
from imgserve.args import (
    get_elasticsearch_args,
    get_experiment_args,
    get_imgserve_args,
    get_mturk_args,
    get_s3_args,
)
from imgserve.clients import get_clients, get_mturk_client
from imgserve.elasticsearch import document_exists, index_to_elasticsearch, get_response_value, MTURK_HITS_INDEX_PATTERN, MTURK_ANSWERS_INDEX_PATTERN
from imgserve.logger import simple_logger
from imgserve.mturk import create_mturk_image_hit

# iterate over cropped-face*, creating an MturkHitDocument for each one
# create mturk HIT batch with each combination


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument("--get-mturk-results", action="store_true", help="get mturk")
    get_elasticsearch_args(parser)
    get_s3_args(parser)
    get_mturk_args(parser)
    get_experiment_args(parser)
    get_imgserve_args(parser)

    return parser.parse_args()


class ImageUrlNon200ResponseCodeError(Exception):
    pass


def validate_image_url(image_url: str) -> None:
    resp = requests.head(image_url)
    if resp.status_code != 200:
        raise ImageUrlNon200ResponseCodeError(f"{image_url} gave {resp.status_code} response code.")


def paginate_mturk(
    mturk_client: MturkClient, client_method: str, iterator_key: str, **kwargs
) -> Generator[Dict[str, Any], None, None]:

    log = simple_logger("imgserve.mturk.paginate")
    resp = getattr(mturk_client, client_method)(**kwargs)

    pages = 0
    while "NextToken" in resp.keys():

        yield from resp[iterator_key]

        kwargs.update(NextToken=resp["NextToken"])
        resp = getattr(mturk_client, client_method)(**kwargs)
        pages += 1

    log.debug(f"{pages} pages of {client_method} results yielded")


def format_filter(filty: Dict[str, Any]) -> Dict[str, Any]:
    for filter_type, filter_compact in filty.items():
        field = list(filter_compact.keys())[0]
        value = list(filter_compact.values())[0]
        if filter_type == "term":
            return {
                "term": {
                    field: {
                        "value": value
                    }
                }
            }
        else:
            raise Exception(f"filter type {filter_type} is unimplemented from RequesterAnnotation")

def main(args: argparse.Namespace) -> None:
    """ image gathering trial and analysis of arbitrary trials"""

    log = simple_logger("imgserve.mturk")

    elasticsearch_client, s3_client = get_clients(args)
    mturk_client = get_mturk_client(args)

    log.info(f"mturk @ {mturk_client.meta._endpoint_url} has balance: ${mturk_client.get_account_balance()['AvailableBalance']}")

    #import IPython; IPython.embed() # interactive mturk client


    if args.get_mturk_results:

        log.info("scanning through reviewable HITs")
        reviewable_hits = 0
        unindexed_hits = list()
        malformed_hits = list()
        mturk_answers = list()
        with tqdm(desc="Scanning Reviewable MTurk HITs") as pbar:
            for reviewable_hit in paginate_mturk(
                mturk_client,
                "list_reviewable_hits",
                "HITs",
                **{"HITTypeId": args.mturk_cropped_face_images_hit_type_id},
            ):
                reviewable_hits += 1
                pbar.update(1)
                hit_resp = mturk_client.get_hit(HITId=reviewable_hit["HITId"])

                if "RequesterAnnotation" not in hit_resp["HIT"]:
                    # annotation does not exist
                    malformed_hits.append(reviewable_hit["HITId"])
                    continue

                annotation_filter = json.loads(hit_resp["HIT"]["RequesterAnnotation"])
                if not isinstance(annotation_filter, list):
                    # annotation filter is not the right format
                    malformed_hits.append(reviewable_hit["HITId"])
                    continue
                annotation_filter.append({"term": {"experiment_name": args.experiment_name}})
                formatted_filter = list()
                for filty in annotation_filter:
                    formatted_filter.append(format_filter(filty))
                annotation_filter = formatted_filter


                try:
                    hit_metadata: Dict[str, str] = [resp for resp in get_response_value(
                        elasticsearch_client=elasticsearch_client,
                        index="mturk-hits*",
                        query={"query": {"bool": {"filter": annotation_filter}}},
                        size=1,
                        value_keys=["hits", "hits", "*", "_source"],
                        #debug=True
                    )][0]
                    del hit_metadata["Question"] # too big
                except IndexError:
                    unindexed_hits.append(annotation_filter)
                    continue

                for assignment_resp in paginate_mturk(
                    mturk_client,
                    "list_assignments_for_hit",
                    "Assignments",
                    **{"HITId": reviewable_hit["HITId"]},
                ):
                    assignment_answer = xmltodict.parse(assignment_resp["Answer"])
                    mturk_answer = copy.deepcopy(hit_metadata)
                    mturk_answer.update(
                        {
                            "AssignmentId": assignment_resp["AssignmentId"],
                            "AcceptTime": assignment_resp["AcceptTime"].strftime("%Y-%m-%dT%H:%M:%SZ"),
                            "SubmitTime": assignment_resp["SubmitTime"].strftime("%Y-%m-%dT%H:%M:%SZ"),
                            "worker_seconds_spent": (assignment_resp["SubmitTime"] - assignment_resp["AcceptTime"]).total_seconds()
                        }
                    )
                    for answer in assignment_answer["QuestionFormAnswers"]["Answer"]:
                        mturk_answer.update(
                            {
                                answer["QuestionIdentifier"]: answer["FreeText"]
                            }
                        )
                    mturk_answers.append(mturk_answer)

        log.info(f"reviewed {len(mturk_answers)}/{reviewable_hits}")
        log.info(f"{len(unindexed_hits)} mturk-hits documents for ReviewableHITs were not indexed in Elasticsearch.")
        #log.info(json.dumps(unindexed_hits))
        log.info(f"{len(malformed_hits)} HITs had malformed RequesterAnnotation field for use by this program.")
        # index
        index_to_elasticsearch(
            elasticsearch_client=elasticsearch_client,
            index=MTURK_ANSWERS_INDEX_PATTERN,
            docs=mturk_answers,
            identity_fields=["AssignmentId"],
            apply_template=True,
        )

    else:

        log.info("creating MTURK HITs for each cropped-face document")
        query = {
            "query": {
                "bool": {"filter": [{"term": {"experiment_name": args.experiment_name}},]}
            }
        }
        count = 0
        search_terms = list()
        mturk_hit_documents = list()
        with tqdm(total=elasticsearch_client.count(index="cropped-face*", body=query)["count"], desc="Creating MTurk HITs from cropped-face documents") as pbar:
            for cropped_face in scan(
                client=elasticsearch_client, index="cropped-face*", query=query
            ):
                source = cropped_face["_source"]
                image_url = (
                    f"https://compsyn.s3.ca-central-1.amazonaws.com/{args.experiment_name}/faces/"
                    + source["face_id"]
                    + ".jpg"
                )
                #validate image_url is working
                validate_image_url(image_url)
                search_term = source["SearchTerm"]
                if search_term in search_terms:
                    continue
                else:
                    search_terms.append(search_term)

                mturk_layout_parameters = [
                    {"Name": "image_url", "Value": image_url},
                    {"Name": "search_term", "Value": search_term},
                ]

                mturk_hit_document = copy.deepcopy(source)
                mturk_hit_document.update(
                    {
                        "hit_state": "submitted",
                        "internal_hit_id": hashlib.sha256(
                            "-".join(
                                [
                                    source["face_id"],
                                    source["query"],
                                    args.mturk_cropped_face_images_hit_type_id,
                                    args.mturk_cropped_face_images_hit_layout_id,
                                    json.dumps(mturk_layout_parameters, sort_keys=True),
                                ]
                            ).encode("utf-8")
                        ).hexdigest(),
                        "mturk_hit_type_id": args.mturk_cropped_face_images_hit_type_id,
                        "mturk_hit_layout_id": args.mturk_cropped_face_images_hit_layout_id,
                        "mturk_layout_parameters": mturk_layout_parameters,
                    }
                )

                if document_exists(
                    elasticsearch_client=elasticsearch_client,
                    doc=mturk_hit_document,
                    index="mturk-hits*",
                    identity_fields=["internal_hit_id"]
                ):
                    pbar.update(1)
                    continue

                mturk_hit_document = create_mturk_image_hit(
                    mturk_client=mturk_client,
                    mturk_hit_document=MturkHitDocument({"_source": mturk_hit_document}),
                    requester_annotation=json.dumps(
                        [
                            {
                                "term": {
                                    "face_id": Path(image_url).stem,
                                }
                            },
                            {
                                "term": {
                                    "query": source["query"],
                                }
                            },
                        ]
                    ),
                )

                mturk_hit_documents.append(mturk_hit_document.source)

                pbar.update(1)
                count += 1
                if count >= 5:
                    break

        log.info(f"indexing {count} documents to elasticsearch")
        index_to_elasticsearch(
            elasticsearch_client=elasticsearch_client,
            index=MTURK_HITS_INDEX_PATTERN,
            docs=mturk_hit_documents,
            identity_fields=["internal_hit_id"],
            apply_template=True,
        )


if __name__ == "__main__":
    main(parse_args())
