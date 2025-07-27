from typing import Any
import traceback
import pytest
from unittest.mock import MagicMock

import core_framework as util

from core_framework.models import TaskPayload, DeploySpec

from core_execute.actionlib.actions.aws.put_user import PutUserActionSpec
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
    spec: dict[str, Any] = {
        "Params": {
            "Account": "1234567890123",  # Example AWS account ID
            "Region": util.get_region(),  # Example AWS region
            "UserNames": "My Name",  # Example KMS Key ID
            "Roles": ["Role1", "Role2"],
        }
    }

    action_spec = PutUserActionSpec(**spec)

    deploy_spec: dict[str, Any] = {"actions": [action_spec]}

    return DeploySpec(**deploy_spec)


def test_put_user(task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session):
    try:
        # Create mock IAM client with proper method implementations
        mock_client = MagicMock()

        # Mock user doesn't exist initially
        from botocore.exceptions import ClientError

        mock_client.get_user.side_effect = ClientError(
            error_response={
                "Error": {
                    "Code": "NoSuchEntity",
                    "Message": "The user with name My Name cannot be found.",
                }
            },
            operation_name="GetUser",
        )

        # Mock successful user creation
        mock_client.create_user.return_value = {
            "User": {
                "UserName": "My Name",
                "UserId": "AIDACKCEVSQ6C2EXAMPLE",
                "Arn": "arn:aws:iam::1234567890123:user/My Name",
                "Path": "/",
                "CreateDate": "2023-10-01T12:00:00Z",
            }
        }

        # Mock no existing policy initially
        mock_client.get_user_policy.side_effect = ClientError(
            error_response={
                "Error": {
                    "Code": "NoSuchEntity",
                    "Message": "The user policy does not exist.",
                }
            },
            operation_name="GetUserPolicy",
        )

        # Mock successful policy attachment
        mock_client.put_user_policy.return_value = {
            "ResponseMetadata": {
                "RequestId": "12345678-1234-1234-1234-123456789012",
                "HTTPStatusCode": 200,
            }
        }

        mock_session.client.return_value = mock_client

        save_actions(task_payload, deploy_spec.action_specs)
        save_state(task_payload, {})

        # Execute the handler
        event = task_payload.model_dump()
        response = execute_handler(event, None)

        task_payload = TaskPayload(**response)
        assert task_payload.flow_control == "success"

        state = load_state(task_payload)
        action_name = "action-aws-putuser-name"

        # Verify existing state keys
        created_users = state[f"{action_name}/CreatedUsers"]
        failed_users = state[f"{action_name}/FailedUsers"]
        skipped_users = state[f"{action_name}/SkippedUsers"]
        users_with_policies = state[f"{action_name}/UsersWithPolicies"]
        assigned_roles = state[f"{action_name}/AssignedRoles"]
        final_policies = state[f"{action_name}/FinalPolicies"]

        assert created_users == ["My Name"]
        assert failed_users == []
        assert skipped_users == []
        assert users_with_policies == ["My Name"]
        assert assigned_roles == ["Role1", "Role2"]

        # Verify final policies structure
        assert "My Name" in final_policies
        user_policy = final_policies["My Name"]
        assert user_policy["PolicyName"] == "My Name-AssumeRoles-Policy"

        policy_doc = user_policy["PolicyDocument"]
        assert policy_doc["Version"] == datetime.fromisoformat("2012-10-17")
        assert "Statement" in policy_doc
        assert len(policy_doc["Statement"]) == 1

        statement = policy_doc["Statement"][0]
        assert statement["Effect"] == "Allow"
        assert statement["Action"] == "sts:AssumeRole"
        assert "arn:aws:iam::1234567890123:role/Role1" in statement["Resource"]
        assert "arn:aws:iam::1234567890123:role/Role2" in statement["Resource"]

        print("✅ test_put_user passed - User created and policy attached successfully")

    except Exception as e:
        # If an exception occurs, print the traceback for debugging
        traceback.print_exc()
        pytest.fail(f"Test failed due to an exception: {e}")
