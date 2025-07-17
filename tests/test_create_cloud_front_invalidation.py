from typing import Any
import traceback
from unittest import mock
import pytest
from unittest.mock import MagicMock
from datetime import datetime, timezone

from core_framework.models import TaskPayload, DeploySpec

from core_execute.actionlib.actions.aws.create_cloud_front_invalidation import (
    CreateCloudFrontInvalidationActionSpec,
    CreateCloudFrontInvalidationActionParams,
)
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
    Parameters are for: CopyImCreateCloudFrontInvalidationActionParamsageActionParams
    """

    # create params for CreateCloudFrontInvalidationActionParams
    spec: dict[str, Any] = {
        "Name": "action-aws-createcloudfrontinvalidation-name",
        "Params": {
            "Account": "123456789012",  # Example AWS account ID
            "Region": "us-east-1",  # Example AWS region
            "DistributionId": "E1234567890ABC",  # Example CloudFront distribution ID
            "Paths": ["/path/to/invalidate/*"],  # Example paths to invalidate
        },
    }

    action_spec = CreateCloudFrontInvalidationActionSpec(**spec)

    deploy_spec: dict[str, Any] = {"actions": [action_spec]}

    return DeploySpec(**deploy_spec)


def test_lambda_handler(task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session):
    try:
        mock_cloudfront_client = MagicMock()

        # Create datetime objects for the mock response
        create_time = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)

        # Set the proper return value for create_invalidation that matches AWS CloudFront API
        mock_cloudfront_client.create_invalidation.return_value = {
            "Invalidation": {
                "Id": "I2J3K4L5M6N7O8P9Q0",  # Example invalidation ID
                "Status": "InProgress",  # Status can be "InProgress" or "Completed"
                "CreateTime": create_time,  # datetime object, not string
                "InvalidationBatch": {
                    "Paths": {"Quantity": 1, "Items": ["/path/to/invalidate/*"]},
                    "CallerReference": "test-caller-reference",
                },
            },
            "Location": "https://cloudfront.amazonaws.com/2020-05-31/distribution/E1234567890ABC/invalidation/I2J3K4L5M6N7O8P9Q0",
            "ETag": "E1234567890ABC",
        }

        # Also mock get_invalidation for the _check method
        mock_cloudfront_client.get_invalidation.return_value = {
            "Invalidation": {
                "Id": "I2J3K4L5M6N7O8P9Q0",
                "Status": "Completed",  # Status changes to "Completed" when checking
                "CreateTime": create_time,  # datetime object, not string
                "InvalidationBatch": {
                    "Paths": {"Quantity": 1, "Items": ["/path/to/invalidate/*"]},
                    "CallerReference": "test-caller-reference",
                },
            }
        }

        # Set the mock session to return our CloudFront client
        mock_session.client.return_value = mock_cloudfront_client

        save_actions(task_payload, deploy_spec.actions)  # Fixed: should be .actions not .action_specs
        save_state(task_payload, {})

        # Create TaskPayload instance from the payload data
        event = task_payload.model_dump()

        response = execute_handler(event, None)

        task_payload = TaskPayload(**response)

        # Add assertions to verify the test passed
        assert response is not None, "Response should not be None"
        assert isinstance(response, dict), "Response should be a dictionary"

        state = load_state(task_payload)

        # Verify that create_invalidation was called
        mock_cloudfront_client.create_invalidation.assert_called_once()

        # Verify the call arguments
        call_args = mock_cloudfront_client.create_invalidation.call_args
        assert call_args[1]["DistributionId"] == "E1234567890ABC"
        assert call_args[1]["InvalidationBatch"]["Paths"]["Items"] == ["/path/to/invalidate/*"]

        # Validate state outputs that should be set by the action
        action_name = "action-aws-createcloudfrontinvalidation-name"

        # Verify basic distribution and path information
        assert f"{action_name}/DistributionId" in state, "DistributionId should be in state"
        assert state[f"{action_name}/DistributionId"] == "E1234567890ABC", "DistributionId should match expected value"

        assert f"{action_name}/InvalidationPaths" in state, "InvalidationPaths should be in state"
        assert state[f"{action_name}/InvalidationPaths"] == [
            "/path/to/invalidate/*"
        ], "InvalidationPaths should match expected value"

        assert f"{action_name}/Region" in state, "Region should be in state"
        assert state[f"{action_name}/Region"] == "us-east-1", "Region should match expected value"

        # Verify invalidation creation results
        assert f"{action_name}/InvalidationId" in state, "InvalidationId should be in state"
        assert state[f"{action_name}/InvalidationId"] == "I2J3K4L5M6N7O8P9Q0", "InvalidationId should match mock response"

        assert f"{action_name}/InvalidationStatus" in state, "InvalidationStatus should be in state"
        assert state[f"{action_name}/InvalidationStatus"] in ["InProgress", "Completed"], "InvalidationStatus should be valid"

        assert f"{action_name}/InvalidationStarted" in state, "InvalidationStarted should be in state"
        assert state[f"{action_name}/InvalidationStarted"] is True, "InvalidationStarted should be True"

        # Verify timestamp fields exist and are valid
        assert f"{action_name}/CreationTime" in state, "CreationTime should be in state"
        creation_time_str = state[f"{action_name}/CreationTime"]
        assert creation_time_str == create_time, "CreationTime should be in ISO format"

        assert f"{action_name}/CallerReference" in state, "CallerReference should be in state"
        caller_reference = state[f"{action_name}/CallerReference"]
        assert caller_reference is not None, "CallerReference should not be None"
        assert isinstance(caller_reference, str), "CallerReference should be a string"

        # Verify action completion status
        assert f"{action_name}/StatusCode" in state, "StatusCode should be in state"
        assert state[f"{action_name}/StatusCode"] == "complete", "Action should be completed"

        assert f"{action_name}/StatusReason" in state, "StatusReason should be in state"
        status_reason = state[f"{action_name}/StatusReason"]
        assert "successfully" in status_reason.lower(), "StatusReason should indicate success"

        # Verify the action executed properly
        assert task_payload.flow_control == "success", "Expected flow_control to be 'success'"

        # Optional: Verify account information if it's being tracked
        if f"{action_name}/Account" in state:
            account = state[f"{action_name}/Account"]
            assert account is not None, "Account should not be None if present"
            assert isinstance(account, str), "Account should be a string if present"

        # Optional: If completion tracking is implemented
        if f"{action_name}/InvalidationCompleted" in state:
            assert isinstance(state[f"{action_name}/InvalidationCompleted"], bool), "InvalidationCompleted should be boolean"

        print(f"✅ All state validations passed. Found {len(state)} state items.")
        print(
            f"📊 Key state items: InvalidationId={state.get(f'{action_name}/InvalidationId')}, Status={state.get(f'{action_name}/InvalidationStatus')}"
        )

    except Exception as e:
        traceback.print_exc()
        assert False, f"Exception occurred: {e}"
