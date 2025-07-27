from typing import Any
import traceback
from unittest import mock
import pytest
from unittest.mock import MagicMock
from datetime import datetime, timezone
from botocore.exceptions import ClientError

from core_framework.models import TaskPayload, DeploySpec

from core_execute.actionlib.actions.aws.delete_change_set import (
    DeleteChangeSetActionSpec,
    DeleteChangeSetActionParams,
)
from core_execute.handler import handler as execute_handler
from core_execute.execute import save_actions, save_state, load_state

from .aws_fixtures import *


# Scope this so it's created fresh for each test
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
    Parameters are fore: DeleteChangeSetActionParams
    """
    spec: dict[str, Any] = {
        "Params": {
            "Account": "154798051514",
            "Region": "ap-southeast-1",
            "StackName": "my-stack",
            "ChangeSetName": "my-changeset",
        }
    }

    action_spec = DeleteChangeSetActionSpec(**spec)

    deploy_spec: dict[str, Any] = {"actions": [action_spec]}

    return DeploySpec(**deploy_spec)


def test_delete_change_set_action(
    task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session
):

    try:

        creation_time = datetime(2023, 10, 1, 12, 0, 0, tzinfo=timezone.utc)

        mock_client = MagicMock()

        # Mock CloudFormation client methods for delete_change_set action

        # Mock describe_change_set - called in _execute() to check if change set exists
        mock_client.describe_change_set.return_value = {
            "ChangeSetId": "12345678-1234-1234-1234-123456789012",
            "ChangeSetName": "my-changeset",
            "StackId": "arn:aws:cloudformation:ap-southeast-1:154798051514:stack/my-stack/12345678-1234-1234-1234-123456789012",
            "StackName": "my-stack",
            "Status": "CREATE_COMPLETE",
            "StatusReason": "Change set created successfully",
            "CreationTime": creation_time,
            "Id": "arn:aws:cloudformation:ap-southeast-1:154798051514:changeSet/my-changeset/12345678-1234-1234-1234-123456789012",
        }

        # Mock delete_change_set - called in _execute() to delete the change set
        mock_client.delete_change_set.return_value = {}

        # Mock for error scenarios - change set not found case
        def describe_change_set_side_effect(*args, **kwargs):
            if "ChangeSetName" in kwargs:
                changeset_name = kwargs["ChangeSetName"]
                stack_name = kwargs.get("StackName", "")

                # Test case for non-existent change set
                if "non-existent" in str(changeset_name) or "non-existent" in str(
                    stack_name
                ):
                    error_response = {
                        "Error": {
                            "Code": "ChangeSetNotFoundException",
                            "Message": f"ChangeSet [{changeset_name}] does not exist",
                        }
                    }
                    raise ClientError(error_response, "DescribeChangeSet")
            return mock_client.describe_change_set.return_value

        # Mock for delete_change_set error scenarios
        def delete_change_set_side_effect(*args, **kwargs):
            if "ChangeSetName" in kwargs:
                changeset_name = kwargs["ChangeSetName"]
                stack_name = kwargs.get("StackName", "")

                # Test case for change set already deleted (race condition)
                if "already-deleted" in str(changeset_name):
                    error_response = {
                        "Error": {
                            "Code": "ChangeSetNotFoundException",
                            "Message": f"ChangeSet [{changeset_name}] does not exist",
                        }
                    }
                    raise ClientError(error_response, "DeleteChangeSet")

                # Test case for other deletion errors
                if "invalid-operation" in str(changeset_name):
                    error_response = {
                        "Error": {
                            "Code": "InvalidChangeSetStatusException",
                            "Message": "Change set cannot be deleted in current status",
                        }
                    }
                    raise ClientError(error_response, "DeleteChangeSet")
            return {}

        # Apply side effects for error testing
        mock_client.describe_change_set.side_effect = describe_change_set_side_effect
        mock_client.delete_change_set.side_effect = delete_change_set_side_effect

        mock_session.client.return_value = mock_client

        save_actions(task_payload, deploy_spec.actions)
        save_state(task_payload, {})

        # Execute the action
        event = task_payload.model_dump()
        result = execute_handler(event, None)

        assert result is not None, "Result should not be None"
        assert isinstance(result, dict), "Result should be a dictionary"

        task_payload = TaskPayload(**result)

        # Validate the flow control in the task payload
        assert (
            task_payload.flow_control == "success"
        ), f"Expected flow_control to be 'success', got '{task_payload.flow_control}'"

        state = load_state(task_payload)

        # Validate that describe_change_set was called to check existence
        mock_client.describe_change_set.assert_called()
        describe_call_args = mock_client.describe_change_set.call_args
        assert describe_call_args[1]["StackName"] == "my-stack"
        assert describe_call_args[1]["ChangeSetName"] == "my-changeset"

        # Validate that delete_change_set was called with correct parameters
        mock_client.delete_change_set.assert_called_once()
        delete_call_args = mock_client.delete_change_set.call_args
        assert delete_call_args[1]["StackName"] == "my-stack"
        assert delete_call_args[1]["ChangeSetName"] == "my-changeset"

        # Validate state was set correctly
        assert "action-aws-deletechangeset-name/ChangeSetName" in state
        assert "action-aws-deletechangeset-name/StackName" in state
        assert "action-aws-deletechangeset-name/DeletionResult" in state
        assert state["action-aws-deletechangeset-name/DeletionResult"] == "SUCCESS"
        assert "action-aws-deletechangeset-name/DeletionCompleted" in state
        assert state["action-aws-deletechangeset-name/DeletionCompleted"] == True
        assert "action-aws-deletechangeset-name/ChangeSetExists" in state
        assert state["action-aws-deletechangeset-name/ChangeSetExists"] == True

        # Validate output variables
        outputs = [
            key
            for key in state.keys()
            if key.startswith("action-aws-deletechangeset-name/")
            and not key.endswith("/state")
        ]
        assert len(outputs) > 0, "Should have output variables set"

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
        pytest.fail(f"Test failed due to an exception: {e}")
