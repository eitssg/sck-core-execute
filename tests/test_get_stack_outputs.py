from os import name
import traceback
import pytest
from unittest.mock import MagicMock
from datetime import datetime
from botocore.exceptions import ClientError

import core_framework as util
from core_framework.models import TaskPayload, DeploySpec

from core_execute.actionlib.actions.aws.get_stack_outputs import (
    GetStackOutputsActionResource,
    GetStackOutputsActionSpec,
)

from core_execute.execute import save_state, save_actions, load_state

from core_execute.handler import handler as execute_handler

from .aws_fixtures import *

action_anme = "get-stack-outputs-test"


@pytest.fixture
def task_payload() -> TaskPayload:
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
            "Scope": "portfolio",
            "DataCenter": "zone-1",
        },
    }
    return TaskPayload.model_validate(data)


@pytest.fixture
def deploy_spec() -> DeploySpec:
    """
    Fixture to provide a sample deploy spec for get stack outputs testing.
    """
    params = {
        "Account": "123456789012",
        "Region": "us-east-1",
        "StackName": "test-stack-name",
    }
    spec = GetStackOutputsActionSpec.model_validate(params)
    action_resource = GetStackOutputsActionResource(name=action_anme, spec=spec)
    return DeploySpec(actions=[action_resource])


def test_get_stack_outputs_action(task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session):
    """Test the get stack outputs action execution with comprehensive state tracking."""

    try:
        # Mock CloudFormation client
        mock_cfn_client = mock_session().client(
            "cloudformation",
            client_type="target",
            aws_account_id="123456789012",
            region_name="us-east-1",
            **get_role_credentials(RoleArn=util.get_provisioning_role_arn("123456789012")),
        )

        # Mock the describe_stacks response with sample outputs
        mock_describe_response = {
            "Stacks": [
                {
                    "StackId": "arn:aws:cloudformation:us-east-1:123456789012:stack/test-stack-name/12345678-1234-1234-1234-123456789012",
                    "StackStatus": "CREATE_COMPLETE",
                    "CreationTime": datetime(2025, 1, 29, 10, 30, 0),
                    "Outputs": [
                        {
                            "OutputKey": "VpcId",
                            "OutputValue": "vpc-12345678",
                            "Description": "The VPC ID",
                        },
                        {
                            "OutputKey": "SubnetId",
                            "OutputValue": "subnet-87654321",
                            "Description": "The Subnet ID",
                        },
                        {
                            "OutputKey": "SecurityGroupId",
                            "OutputValue": "sg-abcdef12",
                            "Description": "The Security Group ID",
                        },
                    ],
                }
            ]
        }

        mock_cfn_client.describe_stacks.return_value = mock_describe_response

        save_actions(task_payload, deploy_spec.actions)
        save_state(task_payload, {})

        event = task_payload.model_dump()
        response = execute_handler(event, None)

        # Verify the response
        assert response is not None, "Response should not be None"
        assert isinstance(response, dict), "Response should be a dictionary"

        # Parse the response back into TaskPayload and check state
        updated_payload = TaskPayload.model_validate(response)

        # Load the saved state to verify progress tracking
        state = load_state(updated_payload)
        print(f"\n=== GET STACK OUTPUTS STATE ===")
        print(f"State: {state}")

        # Check that state contains expected values
        if "actions" in state and len(state["actions"]) > 0:
            action_state = state["actions"][0].get("state", {})
            print(f"Action State: {action_state}")

            # Verify state tracking
            assert action_state.get("stack_name") == "test-stack-name"
            assert action_state.get("account") == "123456789012"
            assert action_state.get("region") == "us-east-1"
            assert (
                action_state.get("stack_id")
                == "arn:aws:cloudformation:us-east-1:123456789012:stack/test-stack-name/12345678-1234-1234-1234-123456789012"
            )
            assert action_state.get("stack_status") == "CREATE_COMPLETE"
            assert action_state.get("status") == "completed"
            assert action_state.get("outputs_count") == 3
            assert action_state.get("start_time") is not None
            assert action_state.get("completion_time") is not None

            # Check outputs
            action_outputs = state["actions"][0].get("outputs", {})
            print(f"Action Outputs: {action_outputs}")

            # Verify basic action outputs
            assert action_outputs.get("status") == "success"
            assert action_outputs.get("stack_name") == "test-stack-name"
            assert (
                action_outputs.get("stack_id")
                == "arn:aws:cloudformation:us-east-1:123456789012:stack/test-stack-name/12345678-1234-1234-1234-123456789012"
            )
            assert action_outputs.get("stack_status") == "CREATE_COMPLETE"
            assert action_outputs.get("account") == "123456789012"
            assert action_outputs.get("region") == "us-east-1"
            assert action_outputs.get("outputs_count") == 3
            assert "Successfully retrieved 3 outputs" in action_outputs.get("message", "")

            # Verify stack outputs were saved
            assert action_outputs.get("VpcId") == "vpc-12345678"
            assert action_outputs.get("VpcId_description") == "The VPC ID"
            assert action_outputs.get("SubnetId") == "subnet-87654321"
            assert action_outputs.get("SubnetId_description") == "The Subnet ID"
            assert action_outputs.get("SecurityGroupId") == "sg-abcdef12"
            assert action_outputs.get("SecurityGroupId_description") == "The Security Group ID"

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
        pytest.fail(f"Test failed due to exception: {e}")


def test_get_stack_outputs_action_stack_not_exists(task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session):
    """Test the get stack outputs action when stack doesn't exist."""

    reset()

    try:
        # Mock CloudFormation client
        mock_cfn_client = mock_session().client(
            "cloudformation",
            client_type="target",
            aws_account_id="123456789012",
            region_name="us-east-1",
            **get_role_credentials(RoleArn=util.get_provisioning_role_arn("123456789012")),
        )

        error_response = {
            "Error": {
                "Code": "ValidationError",
                "Message": "Stack with id test-stack-name does not exist",
            }
        }
        mock_cfn_client.describe_stacks.side_effect = ClientError(error_response, "DescribeStacks")

        save_actions(task_payload, deploy_spec.actions)
        save_state(task_payload, {})

        event = task_payload.model_dump()
        response = execute_handler(event, None)

        # Verify the response handles the non-existent stack gracefully
        assert response is not None, "Response should not be None"
        assert isinstance(response, dict), "Response should be a dictionary"

        # Parse response and check state
        response_payload = TaskPayload.model_validate(response)
        state = load_state(response_payload)

        print(f"\n=== STACK NOT EXISTS STATE ===")
        print(f"State: {state}")

        if "actions" in state and len(state["actions"]) > 0:
            action_state = state["actions"][0].get("state", {})
            action_outputs = state["actions"][0].get("outputs", {})

            print(f"Action State: {action_state}")
            print(f"Action Outputs: {action_outputs}")

            # Verify error handling state
            assert action_state.get("status") == "completed_not_found"
            assert action_state.get("outputs_count") == 0

            # Verify outputs for non-existent stack
            assert action_outputs.get("status") == "success"
            assert action_outputs.get("stack_name") == "test-stack-name"
            assert action_outputs.get("outputs_count") == 0
            assert "does not exist" in action_outputs.get("message", "")

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
        pytest.fail(f"Test failed due to exception: {e}")


def test_get_stack_outputs_action_no_outputs(task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session):
    """Test the get stack outputs action when stack has no outputs."""

    reset()

    try:
        # Mock CloudFormation client
        mock_cfn_client = mock_session().client(
            "cloudformation",
            client_type="target",
            aws_account_id="123456789012",
            region_name="us-east-1",
            **get_role_credentials(RoleArn=util.get_provisioning_role_arn("123456789012")),
        )

        # Mock describe_stacks response with no outputs
        mock_describe_response = {
            "Stacks": [
                {
                    "StackId": "arn:aws:cloudformation:us-east-1:123456789012:stack/test-stack-name/12345678-1234-1234-1234-123456789012",
                    "StackStatus": "CREATE_COMPLETE",
                    "CreationTime": datetime(2025, 1, 29, 10, 30, 0),
                    "Outputs": [],  # No outputs
                }
            ]
        }

        mock_cfn_client.describe_stacks.return_value = mock_describe_response

        save_actions(task_payload, deploy_spec.actions)
        save_state(task_payload, {})

        event = task_payload.model_dump()
        response = execute_handler(event, None)

        # Verify the response
        assert response is not None, "Response should not be None"
        assert isinstance(response, dict), "Response should be a dictionary"

        # Parse response and check state
        response_payload = TaskPayload.model_validate(response)
        state = load_state(response_payload)

        print(f"\n=== NO OUTPUTS STATE ===")
        print(f"State: {state}")

        if "actions" in state and len(state["actions"]) > 0:
            action_state = state["actions"][0].get("state", {})
            action_outputs = state["actions"][0].get("outputs", {})

            print(f"Action State: {action_state}")
            print(f"Action Outputs: {action_outputs}")

            # Verify completion state for stack with no outputs
            assert action_state.get("status") == "completed"
            assert action_state.get("outputs_count") == 0
            assert action_state.get("stack_status") == "CREATE_COMPLETE"

            # Verify outputs
            assert action_outputs.get("status") == "success"
            assert action_outputs.get("outputs_count") == 0
            assert "Successfully retrieved 0 outputs" in action_outputs.get("message", "")

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
        pytest.fail(f"Test failed due to exception: {e}")
