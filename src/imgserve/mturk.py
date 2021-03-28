from __future__ import annotations
import time

from .api import MturkHitDocument

def create_mturk_image_hit(mturk_client: botocore.clients.mturk, mturk_hit_type_id: str, mturk_hit_layout_id: str, mturk_hit_layout_parameters: List[Dict[str, str]], requester_annotation: str = "", max_assignments: int = 1) -> Dict[str, Any]:
    """
    See boto3 documentation for a description of all available parameters:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/mturk.html#MTurk.Client.create_hit_with_hit_type
    """
    start = time.time()
    mturk_hit_resp = mturk_client.create_hit_with_hit_type(
        HITTypeId=mturk_hit_type_id,
        HITLayoutId=mturk_hit_layout_id,
        HITLayoutParameters=mturk_hit_layout_parameters,
        MaxAssignments=max_assignments,
        LifetimeInSeconds=1209600,
        #AutoApprovalDelayInSeconds=900, # not supported here, must be set in HitType?
        RequesterAnnotation=requester_annotation,
    )

    api_runtime = time.time() - start
    if api_runtime > 1:
        print(f"SLOW MTURK: HIT created in {time.time() - start}")

    mturk_hit_document = dict()
    mturk_hit_document.update(hit_state="created")
    mturk_hit_document.update(
        {
            field_key: mturk_hit_resp["HIT"][field_key] for field_key in [
                "HITId", "Title", "Description", "Question", "Keywords", "HITStatus", "MaxAssignments", "Reward", "AutoApprovalDelayInSeconds", "AssignmentDurationInSeconds"
            ]
        }
    )
    mturk_hit_document.update(
        {
            date_field_key: mturk_hit_resp["HIT"][date_field_key].isoformat() for date_field_key in [
                "CreationTime", "Expiration"
            ]
        }
    )

    return mturk_hit_document
