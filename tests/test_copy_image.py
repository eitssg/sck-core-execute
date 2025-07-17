from typing import Any
import traceback
import pytest
from unittest.mock import MagicMock

import core_framework as util

from core_framework.models import TaskPayload, DeploySpec

from core_execute.actionlib.actions.aws.copy_image import CopyImageActionSpec, CopyImageActionParams
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
    Parameters are fore: CopyImageActionParams
    """
    spec: dict[str, Any] = {
        "Params": {
            "Account": "234567890123",
            "ImageName": "test-image",
            "DestinationImageName": "test-image-copy",
            "KmsKeyArn": "arn:aws:kms:us-east-1:123456789012:key/abcd1234-56ef-78gh-90ij-klmnopqrstuv",
            "Region": "us-east-1",
            "Tags": {
                "Environment": "test",
                "Project": "test-project",
            },
        }
    }

    action_spec = CopyImageActionSpec(**spec)

    deploy_spec: dict[str, Any] = {"actions": [action_spec]}

    return DeploySpec(**deploy_spec)


def test_copy_image_action(task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session):

    try:

        mock_client = MagicMock()
        mock_client.copy_image.return_value = {
            "ImageId": "ami-12345678",
            "RequestId": "req-12345678",
        }
        mock_client.describe_images.return_value = {
            "Images": [
                {
                    "ImageId": "ami-12345678",
                    "Name": "test-image-copy",
                    "State": "available",
                }
            ]
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

    except Exception as e:
        print(traceback.format_exc())
        assert False, str(e)
