from typing import Any
import traceback
from unittest import mock
import pytest
from unittest.mock import MagicMock
from datetime import datetime, timezone
from botocore.exceptions import ClientError

from core_framework.models import TaskPayload, DeploySpec

from core_execute.actionlib.actions.aws.apply_change_set import (
    ApplyChangeSetActionParams,
    ApplyChangeSetActionParams,
    ApplyChangeSetActionSpec,
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
    Parameters are fore: ApplyChangeSetActionParams
    """
    spec: dict[str, Any] = {
        "Params": {"Account": "154798051514", "Region": "ap-southeast-1", "StackName": "my-stack", "ChangeSetName": "my-changeset"}
    }

    action_spec = ApplyChangeSetActionSpec(**spec)

    deploy_spec: dict[str, Any] = {"actions": [action_spec]}

    return DeploySpec(**deploy_spec)


def test_apply_change_set_action(task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session):

    try:

        creation_time = datetime(2023, 10, 1, 12, 0, 0, tzinfo=timezone.utc)

        mock_client = MagicMock()

        # Mock CloudFormation client methods for apply_change_set action

        # Mock describe_change_set - called in _execute() to verify change set exists and is ready
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
                                "Target": {"Attribute": "Properties", "Name": "InstanceType", "RequiresRecreation": "Never"},
                                "Evaluation": "Static",
                                "ChangeSource": "DirectModification",
                            }
                        ],
                    },
                }
            ],
        }

        # Mock execute_change_set - called in _execute() to apply the change set
        mock_client.execute_change_set.return_value = {}

        # Mock describe_stacks - called in _check() to monitor stack status
        mock_client.describe_stacks.return_value = {
            "Stacks": [
                {
                    "StackId": "arn:aws:cloudformation:ap-southeast-1:154798051514:stack/my-stack/12345678-1234-1234-1234-123456789012",
                    "StackName": "my-stack",
                    "StackStatus": "UPDATE_COMPLETE",
                    "CreationTime": creation_time,
                    "LastUpdatedTime": creation_time,
                    "Outputs": [
                        {"OutputKey": "InstanceId", "OutputValue": "i-1234567890abcdef0", "Description": "EC2 Instance ID"},
                        {"OutputKey": "PublicIP", "OutputValue": "203.0.113.12", "Description": "Public IP address"},
                    ],
                }
            ]
        }

        # Mock describe_stack_resources - called in _get_stack_resources() to get detailed resource info
        mock_client.get_paginator.return_value.paginate.return_value = [
            {
                "StackResources": [
                    {
                        "LogicalResourceId": "MyEC2Instance",
                        "PhysicalResourceId": "i-1234567890abcdef0",
                        "ResourceType": "AWS::EC2::Instance",
                        "ResourceStatus": "UPDATE_COMPLETE",
                        "Timestamp": creation_time,
                    },
                    {
                        "LogicalResourceId": "MySecurityGroup",
                        "PhysicalResourceId": "sg-1234567890abcdef0",
                        "ResourceType": "AWS::EC2::SecurityGroup",
                        "ResourceStatus": "CREATE_COMPLETE",
                        "Timestamp": creation_time,
                    },
                ]
            }
        ]

        # Mock cancel_update_stack - called in _unexecute() for rollback scenarios
        mock_client.cancel_update_stack.return_value = {}

        # Mock for error scenarios - change set not found case
        def describe_change_set_side_effect(*args, **kwargs):
            if "ChangeSetName" in kwargs and "non-existent" in str(kwargs["ChangeSetName"]):
                error_response = {
                    "Error": {
                        "Code": "ChangeSetNotFoundException",
                        "Message": f"ChangeSet [{kwargs['ChangeSetName']}] does not exist",
                    }
                }
                raise ClientError(error_response, "DescribeChangeSet")
            return mock_client.describe_change_set.return_value

        # Mock for stack not found error scenarios
        def describe_stacks_side_effect(*args, **kwargs):
            if "StackName" in kwargs and "non-existent" in str(kwargs["StackName"]):
                error_response = {
                    "Error": {"Code": "StackNotFoundException", "Message": f"Stack with id {kwargs['StackName']} does not exist"}
                }
                raise ClientError(error_response, "DescribeStacks")
            return mock_client.describe_stacks.return_value

        # Mock for change set not ready scenarios
        def describe_change_set_not_ready(*args, **kwargs):
            response = mock_client.describe_change_set.return_value.copy()
            response["Status"] = "CREATE_IN_PROGRESS"
            return response

        # Apply side effects for error testing
        mock_client.describe_change_set.side_effect = describe_change_set_side_effect
        mock_client.describe_stacks.side_effect = describe_stacks_side_effect

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
        assert task_payload.flow_control == "success", f"Expected flow_control to be 'success', got '{task_payload.flow_control}'"

        state = load_state(task_payload)

        # Validate that the execute_change_set was called with correct parameters
        mock_client.execute_change_set.assert_called_once()
        execute_call_args = mock_client.execute_change_set.call_args
        assert execute_call_args[1]["StackName"] == "my-stack"
        assert execute_call_args[1]["ChangeSetName"] == "my-changeset"

        # Validate that describe_change_set was called to verify change set status
        mock_client.describe_change_set.assert_called()
        describe_call_args = mock_client.describe_change_set.call_args
        assert describe_call_args[1]["StackName"] == "my-stack"
        assert describe_call_args[1]["ChangeSetName"] == "my-changeset"

        # Validate that describe_stacks was called to monitor progress
        mock_client.describe_stacks.assert_called()

        # Validate state was set correctly
        assert "action-aws-applychangeset-name/ChangeSetName" in state
        assert "action-aws-applychangeset-name/StackName" in state
        assert "action-aws-applychangeset-name/ApplicationResult" in state
        assert state["action-aws-applychangeset-name/ApplicationResult"] == "SUCCESS"
        assert "action-aws-applychangeset-name/ResourcesCreated" in state
        assert "action-aws-applychangeset-name/ResourcesUpdated" in state
        assert "action-aws-applychangeset-name/StackOutputs" in state

        # Validate output variables
        assert state["action-aws-applychangeset-name/StackOutputs"] is not None
        assert len(state["action-aws-applychangeset-name/StackOutputs"]) == 2

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
        pytest.fail(f"Test failed due to an exception: {e}")
