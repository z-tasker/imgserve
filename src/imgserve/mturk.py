from __future__ import annotations

from .api import MturkHitDocument

def create_mturk_image_hit(mturk_client: botocore.clients.mturk, mturk_hit_document: MturkHitDocument) -> Dict[str, Any]:
    """
    See boto3 documentation for a description of all available parameters:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/mturk.html#MTurk.Client.create_hit_with_hit_type
    """
    mturk_hit_resp = mturk_client.create_hit_with_hit_type(
        HITTypeId=mturk_hit_document.source["mturk_hit_type_id"],
        HITLayoutId=mturk_hit_document.source["mturk_layout_id"],
        HITLayoutParameters=mturk_hit_document.source["mturk_layout_parameters"],
        MaxAssignments=10,
        LifetimeInSeconds=3600,
    )

    mturk_hit_document.source.update(
        {
            field_key: mturk_hit_resp[field_key] for field_key in [
                "HITId", "Title", "Description", "Question", "Keywords", "HITStatus", "MaxAssignments", "Reward", "AutoApprovalDelayInSeconds", "AssignmentDurationInSeconds"
            ]
        }
    )
    mturk_hit_document.source.update(
        {
            date_field_key: mturk_hit_resp[date_field_key].isoformat() for date_field_key in [ 
                "CreationTime", "Expiration" 
            ]
        }
    )

    return mturk_hit_document
