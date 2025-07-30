import traceback
import pytest
from unittest.mock import MagicMock
import io
import core_framework as util
from core_helper.magic import MagicS3Client
from core_framework.models import TaskPayload, DeploySpec

from core_execute.actionlib.actions.aws.upload_context import UploadContextActionSpec, UploadContextActionParams
from core_execute.handler import handler as execute_handler
from core_execute.execute import save_actions, save_state, load_state

from .aws_fixtures import *


@pytest.fixture
def task_payload():
    """
    Fixture to provide a sample payload data for testing.
    This can be used to mock the payload in tests.
    """
    data = {
        "Task": "deploy",
        "DeploymentDetails": {
            "Client": "client",
            "Portfolio": "portfolio",
            "Environment": "production",
            "Scope": "portfolio",  # Test this execution with a scope of portfolio
            "DataCenter": "zone-1",  # name of the data center ('availability zone' in AWS)
        },
    }
    return TaskPayload(**data)


@pytest.fixture
def deploy_spec():
    """
    Fixture to provide a deployspec data for testing.
    This can be used to mock the deployspec in tests.
    """
    validated_params = UploadContextActionParams(
        **{
            "Account": "123456789012",
            "BucketName": "my-upload-bucket",
            "Region": util.get_region(),
            "Prefix": "uploads/",
        }
    )

    action_spec = UploadContextActionSpec(Name="upload-context", Params=validated_params.model_dump())

    return DeploySpec(actions=[action_spec])


def test_upload_context_action(task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session):
    """Test the upload context action successful execution."""
    try:

        save_actions(task_payload, deploy_spec.actions)  # Fixed: use .actions not .action_specs
        save_state(
            task_payload,
            {
                "prn:portfolio:app:branch:build:output/variable1": "value1",
                "prn:portfolio:app:branch:build:output/variable2": "value2",
                "prn:portfolio:app:branch:build:output/variable3": "value3",
                "prn:portfolio:app:branch:build:output/variable4": ["value4a", "value4b", "value4c"],
                "prn:portfolio:app:branch:build:output/variable5": {"key1": "value5a", "key2": "value5b"},
                "prn:portfolio:app:branch:build:component:output/variable6": {"key1": "value6a", "key2": "value6b"},
            },
        )

        # Execute the handler
        event = task_payload.model_dump()
        response = execute_handler(event, None)

        # Verify the response
        assert response is not None, "Response should not be None"
        assert isinstance(response, dict), "Response should be a dictionary"

        # Parse the response back into TaskPayload
        updated_payload = TaskPayload(**response)
        assert updated_payload.flow_control == "success", "Flow control should be success"

        # Load the saved state to verify completion
        state = load_state(updated_payload)
        assert state is not None, "State should not be None"

        # Verify state tracking with namespace
        action_namespace = "upload-context"
        assert state.get(f"{action_namespace}/status") == "success", "Should have success status"
        assert state.get(f"{action_namespace}/variable_count") == 6, "Should track correct number of context variables"

        # Verify uploaded files list
        uploaded_files = state.get(f"{action_namespace}/uploaded_files")
        assert uploaded_files is not None, "Should track uploaded files"
        assert len(uploaded_files) == 2, "Should have uploaded 2 files"
        assert "uploads/context.yaml" in uploaded_files, "Should track YAML file"
        assert "uploads/context.json" in uploaded_files, "Should track JSON file"

        # Verify individual file tracking
        assert state.get(f"{action_namespace}/yaml_file") == "uploads/context.yaml", "Should track YAML file path"
        assert state.get(f"{action_namespace}/json_file") == "uploads/context.json", "Should track JSON file path"
        assert state.get(f"{action_namespace}/bucket_name") == "my-upload-bucket", "Should track bucket name"
        assert state.get(f"{action_namespace}/prefix") == "uploads", "Should track prefix"

        account = "123456789012"
        role_arn = util.get_provisioning_role_arn(account)
        s3_client = MagicS3Client.get_client(util.get_region(), role_arn)

        buffer = io.BytesIO()
        s3_client.download_fileobj(Bucket="my-upload-bucket", Key="uploads/context.yaml", Fileobj=buffer)
        data = util.from_yaml(buffer.getvalue().decode("utf-8"))

        assert data["pipeline"]["variable1"] == "value1", "Should have correct variable1 value"
        assert data["component"]["variable6"] == {"key1": "value6a", "key2": "value6b"}, "Should have correct variable6 value"

    except Exception as e:
        traceback.print_exc()
        pytest.fail(f"Test failed with exception: {e}")
