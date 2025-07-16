import core_framework as util
from core_helper.magic import MagicS3Client
from core_framework.models import TaskPayload, DeploySpec


def save_deploy_spec(payload_data: TaskPayload, deploy_spec: DeploySpec):
    """
    Save the deploy specification to S3 if real_aws is True.
    """
    actions = payload_data.actions

    s3_actions_client = MagicS3Client.get_client(
        Region=actions.bucket_region, DataPath=actions.data_path
    )

    data: list = deploy_spec.model_dump()["Actions"]

    s3_actions_client.put_object(
        Bucket=actions.bucket_name,
        Key=actions.key,
        Body=util.to_yaml(data),
        ContentType=actions.content_type,
    )
