#!/usr/bin/env python3
from __future__ import annotations
import argparse
import copy
import hashlib
import json
import pickle
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
    parser.add_argument("--commit-mturk-results", action="store_true", help="Commiting mturk results means disposing of the HIT and marking the elasticsearch mturk-answers documents as 'committed'")
    parser.add_argument("--make-pickle", type=Path, help="path to save pickle to")
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
        all_hits = 0
        reviewable_hits = 0
        reviewed_hits = list()
        unindexed_hits = list()
        malformed_hits = list()
        mturk_answers = list()
        mturk_pickles = list()
        reviewed_docs = list()
        with tqdm(total=elasticsearch_client.count(index="mturk-hits*", body={
            "query": {"bool": {"filter": [{"term": {"experiment_name": args.experiment_name}}]}}
        })["count"], desc="Scanning Reviewable MTurk HITs") as pbar:
            # replace with for HITId in elasticsearch, get_hit
            for hit in get_response_value(
                elasticsearch_client=elasticsearch_client,
                index="mturk-hits*",
                query={
                    "query": {"bool": {"filter": [{"term": {"experiment_name": args.experiment_name}}]}},
                    "aggregations": {
                        "hit_id": {
                            "composite": {
                                "size": 500,
                                "sources": [
                                    {"hit_id": {"terms": { "field": "HITId" }}},
                                    {"hit_state": {"terms": {"field": "hit_state"}}},
                                    {"doc_id": {"terms": { "field": "_id" }}},
                                ]
                            }
                        }
                    }
                },
                value_keys=["aggregations", "hit_id", "buckets", "*", "key"],
                #debug=True,
                composite_aggregation_name="hit_id"
            ):
                all_hits += 1
                pbar.update(1)
                if hit["hit_state"] != "created":
                    continue
                hit_resp = mturk_client.get_hit(HITId=hit["hit_id"])
                if hit_resp["HIT"]["HITStatus"] != "Reviewable":
                    continue
                reviewable_hits += 1

                if "RequesterAnnotation" not in hit_resp["HIT"]:
                    # annotation does not exist
                    malformed_hits.append(hit["hit_id"])
                    continue

                annotation_filter = json.loads(hit_resp["HIT"]["RequesterAnnotation"])
                if not isinstance(annotation_filter, list):
                    # annotation filter is not the right format
                    malformed_hits.append(hit["hit_id"])
                    continue

                reviewed_hits.append(hit["hit_id"])

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
                    **{"HITId": hit["hit_id"]},
                ):
                    if args.make_pickle is not None:
                        mturk_pickles.append(assignment_resp)
                    assignment_answer = xmltodict.parse(assignment_resp["Answer"])
                    mturk_answer = copy.deepcopy(hit_metadata)
                    mturk_answer.update(
                        {
                            "AssignmentId": assignment_resp["AssignmentId"],
                            "AcceptTime": assignment_resp["AcceptTime"].strftime("%Y-%m-%dT%H:%M:%SZ"),
                            "SubmitTime": assignment_resp["SubmitTime"].strftime("%Y-%m-%dT%H:%M:%SZ"),
                            "worker_seconds_spent": (assignment_resp["SubmitTime"] - assignment_resp["AcceptTime"]).total_seconds(),
                            "mturk_status": "Reviewable" if not args.commit_mturk_results else "Committed",
                        }
                    )
                    for answer in assignment_answer["QuestionFormAnswers"]["Answer"]:
                        mturk_answer.update(
                            {
                                answer["QuestionIdentifier"]: answer["FreeText"]
                            }
                        )
                    mturk_answers.append(mturk_answer)
                reviewed_docs.append(hit["doc_id"])

        log.info(f"reviewed {len(reviewed_docs)}/{reviewable_hits}")
        if len(unindexed_hits) > 0:
            log.warning(f"{len(unindexed_hits)} mturk-hits documents for ReviewableHITs were not indexed in Elasticsearch.")
        if len(malformed_hits) > 0:
            log.info(f"{len(malformed_hits)} HITs had malformed RequesterAnnotation field for use by this program.")
        #if args.commit_mturk_results:
        #    all_unindexable = unindexed_hits + malformed_hits
        # pickle all hits and answers?
        if args.make_pickle is not None:
            pickle.dump(mturk_pickles, open(Path(args.make_pickle).with_suffix(".p"), "wb"))
            log.info(f"pickled {len(mturk_pickles)} mturk assignments")
        # index
        index_to_elasticsearch(
            elasticsearch_client=elasticsearch_client,
            index=MTURK_ANSWERS_INDEX_PATTERN,
            docs=mturk_answers,
            identity_fields=["AssignmentId"],
            apply_template=True,
        )
        with tqdm(total=len(reviewed_docs), desc="Update Reviewed MTurk HITs in Elasticsearch") as pbar:
            for reviewed_doc in reviewed_docs:
                elasticsearch_client.update("mturk-hits", reviewed_doc, {"doc": {"hit_state": "reviewed"}})
                pbar.update(1)
        # update mturk representation of reviewed_hits?
        # if there is no burden of holding these hits until they expire, that is probably preferable


    else:

        log.info("creating MTURK HITs for each cropped-face document")
        query = {
            "query": {
                "bool": {"filter": [{"term": {"experiment_name": args.experiment_name}},]}
            }
        }
        image_urls_work = False
        count = 0
        search_terms = list()
        mturk_hit_documents = list()
        with tqdm(total=elasticsearch_client.count(index="cropped-face*", body=query)["count"], desc="Creating MTurk HITs from cropped-face documents") as pbar:
            for cropped_face in scan(
                client=elasticsearch_client, index="cropped-face*", query=query
            ):
                source = cropped_face["_source"]
                image_url = (
                    f"https://{args.s3_bucket}.s3.{args.s3_region_name}.amazonaws.com/{args.experiment_name}/faces/"
                    + source["face_id"]
                    + ".jpg"
                )
                #validate image_url is working for the first image
                if not image_urls_work:
                    validate_image_url(image_url)
                    image_urls_work = True

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
                                    source["query"], # this internal hid id makes it so that we will only create 1 HIT for each face per query that generated that face. That is, the same face may have multiple hits, one for each query that returned it.
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
                    # skip faces that already have an associated mturk hit
                    pbar.update(1)
                    continue

                # submit the HIT to mturk
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
                if len(mturk_hit_documents)>= 100:
                    break

        if len(mturk_hit_documents) > 0:
            log.info(f"indexing {len(mturk_hit_documents)} documents to elasticsearch")
            index_to_elasticsearch(
                elasticsearch_client=elasticsearch_client,
                index=MTURK_HITS_INDEX_PATTERN,
                docs=mturk_hit_documents,
                identity_fields=["internal_hit_id"],
                apply_template=True,
            )
    # NOTE: this approach leaves HITs in MTurk in a "Reviewable" state, as completion logic is handled in Elasticsearch.
    # does accumulating Reviewable HITs slow down the get_hit API call?
    # optionally pickle before discarding?



if __name__ == "__main__":
    main(parse_args())
