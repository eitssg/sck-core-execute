from typing import Any
import traceback
from unittest import mock
import pytest
from unittest.mock import MagicMock


from core_framework.models import TaskPayload, DeploySpec

from core_execute.actionlib.actions.aws.create_image import CreateImageActionSpec, CreateImageActionParams
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
    Parameters are fore: CreateImageActionParams
    """
    spec: dict[str, Any] = {
        "Params": {
            "Account": "123456789012",
            "Region": "ap-southeast-1",
            "InstanceId": "i-1234567890abcdef0",
            "ImageName": "My-Image-Name",
            "Tags": {"Environment": "production", "Project": "my-project"},
        }
    }

    action_spec = CreateImageActionSpec(**spec)

    deploy_spec: dict[str, Any] = {"actions": [action_spec]}

    return DeploySpec(**deploy_spec)


def test_create_image_action(task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session):

    try:

        mock_client = MagicMock()

        # Mock create_image response - this should only return ImageId
        mock_client.create_image.return_value = {"ImageId": "ami-12345678"}

        # Mock describe_images response - this needs complete image details
        mock_client.describe_images.return_value = {
            "Images": [
                {
                    "ImageId": "ami-12345678",
                    "Name": "My-Image-Name",  # Matches ImageName parameter
                    "State": "available",
                    "Size": 8,
                    "Architecture": "x86_64",
                    "Platform": "Linux",
                    "Description": "AMI created from instance i-1234567890abcdef0",
                    "CreationDate": "2024-01-15T10:30:00.000Z",
                    "BlockDeviceMappings": [
                        {
                            "DeviceName": "/dev/sda1",
                            "Ebs": {"SnapshotId": "snap-12345678", "VolumeSize": 8, "VolumeType": "gp3", "Encrypted": True},
                        },
                        {
                            "DeviceName": "/dev/sdb",
                            "Ebs": {"SnapshotId": "snap-87654321", "VolumeSize": 20, "VolumeType": "gp3", "Encrypted": True},
                        },
                    ],
                }
            ]
        }

        # Mock create_tags response
        mock_client.create_tags.return_value = {
            "ResponseMetadata": {
                "RequestId": "12345678-1234-1234-1234-123456789012",
                "HTTPStatusCode": 200,
            }
        }

        # Mock deregister_image response (for rollback)
        mock_client.deregister_image.return_value = {
            "ResponseMetadata": {
                "RequestId": "12345678-1234-1234-1234-123456789012",
                "HTTPStatusCode": 200,
            }
        }

        # Mock delete_snapshot response (for rollback)
        mock_client.delete_snapshot.return_value = {
            "ResponseMetadata": {
                "RequestId": "12345678-1234-1234-1234-123456789012",
                "HTTPStatusCode": 200,
            }
        }

        mock_session.client = MagicMock(return_value=mock_client)

        save_actions(task_payload, deploy_spec.actions)
        save_state(task_payload, {})

        event = task_payload.model_dump()

        result = execute_handler(event, None)

        assert result is not None, "Result should not be None"
        assert isinstance(result, dict), "Result should be a dictionary"

        task_payload = TaskPayload(**result)

        # Validate the flow control in the task payload
        assert task_payload.flow_control == "success", "Expected flow_control to be 'success'"

        state = load_state(task_payload)

        # Verify that create_image was called with correct parameters
        mock_client.create_image.assert_called_once_with(
            InstanceId="i-1234567890abcdef0", Name="My-Image-Name"  # Matches parameter  # Matches parameter
        )

        # Verify that create_tags was called for both image and snapshots
        expected_calls = [
            # Tag the image
            mock.call(Resources=["ami-12345678"], Tags=mock.ANY),
            # Tag the snapshots
            mock.call(Resources=["snap-12345678", "snap-87654321"], Tags=mock.ANY),
        ]
        mock_client.create_tags.assert_has_calls(expected_calls, any_order=True)

        # Validate state outputs that should be set by the action
        action_name = "action-aws-createimage-name"

        # Check basic parameters are stored in state
        assert f"{action_name}/SourceInstanceId" in state
        assert state[f"{action_name}/SourceInstanceId"] == "i-1234567890abcdef0"

        assert f"{action_name}/ImageName" in state
        assert state[f"{action_name}/ImageName"] == "My-Image-Name"

        assert f"{action_name}/Region" in state
        assert state[f"{action_name}/Region"] == "ap-southeast-1"

        # Check image creation results
        assert f"{action_name}/ImageId" in state
        assert state[f"{action_name}/ImageId"] == "ami-12345678"

        assert f"{action_name}/ImageState" in state
        assert state[f"{action_name}/ImageState"] == "available"

        assert f"{action_name}/ImageCreationCompleted" in state
        assert state[f"{action_name}/ImageCreationCompleted"] is True

        assert f"{action_name}/SnapshotIds" in state
        assert state[f"{action_name}/SnapshotIds"] == ["snap-12345678", "snap-87654321"]

        assert f"{action_name}/StatusCode" in state
        assert state[f"{action_name}/StatusCode"] == "complete"

    except Exception as e:
        print(f"Exception occurred: {e}")
        print(traceback.format_exc())
        pytest.fail(f"Test failed with exception: {e}")
