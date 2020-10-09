#!/usr/bin/env python3
from __future__ import annotations
import argparse
import copy
import hashlib
import json
from pathlib import Path

import xmltodict
from elasticsearch.helpers import scan

from imgserve.api import Experiment, MturkHitDocument
from imgserve.args import (
    get_elasticsearch_args,
    get_experiment_args,
    get_imgserve_args,
    get_mturk_args,
    get_s3_args,
)
from imgserve.clients import get_clients, get_mturk_client
from imgserve.elasticsearch import index_to_elasticsearch, get_response_value, MTURK_HITS_INDEX_PATTERN, MTURK_ANSWERS_INDEX_PATTERN
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


def main(args: argparse.Namespace) -> None:
    """ image gathering trial and analysis of arbitrary trials"""

    log = simple_logger("imgserve.mturk")

    elasticsearch_client, s3_client = get_clients(args)
    mturk_client = get_mturk_client(args)

    log.info(f"mturk @ {mturk_client.meta._endpoint_url} has balance: ${mturk_client.get_account_balance()['AvailableBalance']}")

    # import IPython; IPython.embed() # interactive mturk client

    log.info("starting scan")

    if args.get_mturk_results:

        reviewable_hits = 0
        mturk_answers = list()
        for reviewable_hit in paginate_mturk(
            mturk_client,
            "list_reviewable_hits",
            "HITs",
            **{"HITTypeId": args.mturk_cropped_face_images_hit_type_id},
        ):
            reviewable_hits += 1
            hit_resp = mturk_client.get_hit(HITId=reviewable_hit["HITId"])

            if "RequesterAnnotation" not in hit_resp["HIT"]:
                continue

            hit_metadata: Dict[str, str] = [resp for resp in get_response_value(
                elasticsearch_client=elasticsearch_client,
                index="cropped-face",
                query={"query": {"bool": {"filter": [{"term": json.loads(hit_resp["HIT"]["RequesterAnnotation"])}]}}},
                size=1,
                value_keys=["hits", "hits", "*", "_source"]
            )][0]

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
        # index
        index_to_elasticsearch(
            elasticsearch_client=elasticsearch_client,
            index=MTURK_ANSWERS_INDEX_PATTERN,
            docs=mturk_answers,
            identity_fields=["AssignmentId"],
            apply_template=True,
        )
        return

    query = {
        "query": {
            "bool": {"filter": [{"term": {"experiment_name": args.experiment_name}},]}
        }
    }
    count = 0
    search_terms = list()
    mturk_hit_documents = list()
    for cropped_face in scan(
        client=elasticsearch_client, index="cropped-face*", query=query
    ):
        source = cropped_face["_source"]
        image_url = (
            "https://compsyn.s3.ca-central-1.amazonaws.com/bias-fairness-transparency/faces/"
            + source["face_id"]
            + ".jpg"
        )
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

        log.info(image_url)
        log.info(search_term)

        mturk_hit_document = create_mturk_image_hit(
            mturk_client=mturk_client,
            mturk_hit_document=MturkHitDocument({"_source": mturk_hit_document}),
            requester_annotation=json.dumps(
                {
                    "face_id": Path(image_url).stem,
                }
            ),
        )

        mturk_hit_documents.append(mturk_hit_document.source)

        count += 1
        if count >= 2:
            break

    log.info(f"indexing {count} documents to elasticsearch")
    print(mturk_hit_documents[0])
    index_to_elasticsearch(
        elasticsearch_client=elasticsearch_client,
        index=MTURK_HITS_INDEX_PATTERN,
        docs=mturk_hit_documents,
        identity_fields=["internal_hit_id"],
        apply_template=True,
    )


if __name__ == "__main__":
    main(parse_args())
