import core_framework as util
from core_helper.magic import MagicS3Client
from core_framework.models import TaskPayload, ActionResource


def save_actions(payload_data: TaskPayload, actions: list[ActionResource]):
    """
    Save the deploy specification to S3 if real_aws is True.
    """

    s3_actions_client = MagicS3Client.get_client(Region=actions.bucket_region, DataPath=actions.data_path)

    data: list[dict] = []

    for action in actions:
        if isinstance(action, ActionResource):
            data.append(action.model_dump())
        else:
            raise TypeError(f"Expected ActionResource, got {type(action)}")

    s3_actions_client.put_object(
        Bucket=actions.bucket_name,
        Key=actions.key,
        Body=util.to_yaml(data),
        ContentType=actions.content_type,
    )
