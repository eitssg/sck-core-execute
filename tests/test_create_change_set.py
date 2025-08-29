from typing import Any
import traceback
from unittest import mock
import pytest
from unittest.mock import MagicMock
from datetime import datetime, timezone
from botocore.exceptions import ClientError

from core_framework.models import TaskPayload, DeploySpec

from core_execute.actionlib.actions.aws.create_change_set import (
    CreateChangeSetActionSpec,
    CreateChangeSetActionParams,
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
    Parameters are fore: CreateChangeSetActionParams
    """
    spec: dict[str, Any] = {
        "Spec": {
            "Account": "154798051514",
            "Region": "ap-southeast-1",
            "StackName": "my-stack",
            "ChangeSetName": "my-changeset",
            "TemplateUrl": "s3://my-bucket/portfolio/my-template.yaml",
            "StackParameters": {"InstanceType": "t2.micro"},
        }
    }

    action_spec = CreateChangeSetActionSpec(**spec)

    return DeploySpec(actions=[action_spec])


def test_create_change_set_action(
    task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session
):

    try:

        creation_time = datetime(2023, 10, 1, 12, 0, 0, tzinfo=timezone.utc)

        mock_client = MagicMock()

        # Mock CloudFormation client methods for create_change_set action

        # Mock describe_stacks - called to check if stack exists
        mock_client.describe_stacks.return_value = {
            "Stacks": [
                {
                    "StackId": "arn:aws:cloudformation:ap-southeast-1:154798051514:stack/my-stack/12345678-1234-1234-1234-123456789012",
                    "StackName": "my-stack",
                    "StackStatus": "UPDATE_COMPLETE",
                    "CreationTime": creation_time,
                    "LastUpdatedTime": creation_time,
                }
            ]
        }

        # Mock create_change_set - called in _execute()
        mock_client.create_change_set.return_value = {
            "Id": "arn:aws:cloudformation:ap-southeast-1:154798051514:changeSet/my-changeset/12345678-1234-1234-1234-123456789012",
            "StackId": "arn:aws:cloudformation:ap-southeast-1:154798051514:stack/my-stack/12345678-1234-1234-1234-123456789012",
        }

        # Mock describe_change_set - called in _check()
        mock_client.describe_change_set.return_value = {
            "ChangeSetId": "12345678-1234-1234-1234-123456789012",
            "ChangeSetName": "my-changeset",
            "StackId": "arn:aws:cloudformation:ap-southeast-1:154798051514:stack/my-stack/12345678-1234-1234-1234-123456789012",
            "StackName": "my-stack",
            "Status": "CREATE_COMPLETE",
            "StatusReason": "Change set created successfully",
            "CreationTime": creation_time,
            "Id": "arn:aws:cloudformation:ap-southeast-1:154798051514:changeSet/my-changeset/12345678-1234-1234-1234-123456789012",
            "Changes": [
                {
                    "Action": "Modify",
                    "ResourceChange": {
                        "Action": "Modify",
                        "LogicalResourceId": "MyEC2Instance",
                        "PhysicalResourceId": "i-1234567890abcdef0",
                        "ResourceType": "AWS::EC2::Instance",
                        "Replacement": "False",
                        "Scope": ["Properties"],
                        "Details": [
                            {
                                "Target": {
                                    "Attribute": "Properties",
                                    "Name": "InstanceType",
                                    "RequiresRecreation": "Never",
                                },
                                "Evaluation": "Static",
                                "ChangeSource": "DirectModification",
                            }
                        ],
                    },
                }
            ],
        }

        # Mock delete_change_set - called in _unexecute() or error scenarios
        mock_client.delete_change_set.return_value = {}

        # Mock for error scenarios - stack not found case
        def describe_stacks_side_effect(*args, **kwargs):
            if "StackName" in kwargs and kwargs["StackName"] == "non-existent-stack":
                error_response = {
                    "Error": {
                        "Code": "ValidationError",
                        "Message": "Stack with id non-existent-stack does not exist",
                    }
                }
                raise ClientError(error_response, "DescribeStacks")
            return mock_client.describe_stacks.return_value

        # Mock for change set not found error scenarios
        def describe_change_set_side_effect(*args, **kwargs):
            if "ChangeSetName" in kwargs and "non-existent" in kwargs["ChangeSetName"]:
                error_response = {
                    "Error": {
                        "Code": "ChangeSetNotFoundException",
                        "Message": f"ChangeSet [{kwargs['ChangeSetName']}] does not exist",
                    }
                }
                raise ClientError(error_response, "DescribeChangeSet")
            return mock_client.describe_change_set.return_value

        # Apply side effects for error testing
        mock_client.describe_stacks.side_effect = describe_stacks_side_effect
        mock_client.describe_change_set.side_effect = describe_change_set_side_effect

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

        # Validate that the create_change_set was called with correct parameters
        mock_client.create_change_set.assert_called_once()
        create_call_args = mock_client.create_change_set.call_args
        assert create_call_args[1]["StackName"] == "my-stack"
        assert create_call_args[1]["ChangeSetName"] == "my-changeset"
        assert (
            create_call_args[1]["TemplateURL"]
            == "s3://my-bucket/portfolio/my-template.yaml"
        )
        assert create_call_args[1]["ChangeSetType"] == "UPDATE"
        assert "CAPABILITY_IAM" in create_call_args[1]["Capabilities"]
        assert "CAPABILITY_NAMED_IAM" in create_call_args[1]["Capabilities"]
        assert "CAPABILITY_AUTO_EXPAND" in create_call_args[1]["Capabilities"]

        # Validate parameters were passed correctly
        assert len(create_call_args[1]["Parameters"]) == 1
        assert create_call_args[1]["Parameters"][0]["ParameterKey"] == "InstanceType"
        assert create_call_args[1]["Parameters"][0]["ParameterValue"] == "t2.micro"

        # Validate state was set correctly
        assert "action-aws-createchangeset-name/ChangeSetArn" in state
        assert "action-aws-createchangeset-name/ChangeSetId" in state
        assert "action-aws-createchangeset-name/StackId" in state
        assert "action-aws-createchangeset-name/CreationResult" in state
        assert state["action-aws-createchangeset-name/CreationResult"] == "SUCCESS"

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
        pytest.fail(f"Test failed due to an exception: {e}")
