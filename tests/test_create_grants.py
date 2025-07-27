from typing import Any
import traceback
import pytest
from unittest.mock import MagicMock

import core_framework as util

from core_framework.models import TaskPayload, DeploySpec

from core_execute.actionlib.actions.aws.kms.create_grants import CreateGrantsActionSpec
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
            "KmsKeyId": "kms-key-id-1234567890abcdef",  # Example KMS Key ID
            "GranteePrincipals": ["arn:aws:iam::123456789012:role/ExampleRole"],
            "Operations": ["Decrypt", "Encrypt", "GenerateDataKey"],
            "IgnoreFailedGrants": "false",  # Set to True to ignore failed grants
        }
    }

    action_spec = CreateGrantsActionSpec(**spec)

    deploy_spec: dict[str, Any] = {"actions": [action_spec]}

    return DeploySpec(**deploy_spec)


def test_lambda_handler(
    task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session
):

    try:

        # update the mock_session fixtures such that its client() returns a new mock kms client with the create_grants() function return value set appropraitely
        mock_kms_client = MagicMock()
        mock_kms_client.create_grant.return_value = {
            "GrantToken": "example-grant-token",
            "GrantId": "example-grant-id",
        }
        # Add list_grants mock for the _check method
        mock_kms_client.list_grants.return_value = {
            "Grants": [
                {
                    "GrantId": "example-grant-id",
                    "GrantToken": "example-grant-token",
                    "Name": "arn-aws-iam--123456789012-role-ExampleRole",
                    "GranteePrincipal": "arn:aws:iam::123456789012:role/ExampleRole",
                    "Operations": ["Decrypt", "Encrypt", "GenerateDataKey"],
                    "KeyId": "kms-key-id-1234567890abcdef",
                }
            ]
        }

        # Add retire_grant mock for the _unexecute method (if needed)
        mock_kms_client.retire_grant.return_value = {}

        mock_session.client.return_value = mock_kms_client

        save_actions(task_payload, deploy_spec.action_specs)
        save_state(task_payload, {})

        # Create TaskPayload instance from the payload data.  This validates the structure and populates defauluts.

        event = task_payload.model_dump()

        response = execute_handler(event, None)

        # Validate the response structure and content

        task_payload = TaskPayload(**response)

        assert task_payload.task == "deploy"

        assert (
            task_payload.flow_control == "success"
        ), "Expected flow_control to be 'success'"

        state = load_state(task_payload)

        assert state is not None, "Expected state to be loaded successfully"

        assert (
            "action-aws-kms-creategrants-name/GrantIds" in state
        ), "Expected GrantId to be set in state"

        assert (
            "example-grant-id" in state["action-aws-kms-creategrants-name/GrantIds"]
        ), "Expected GrantIds to be ['example-grant-id']"

        assert (
            "action-aws-kms-creategrants-name/GrantTokens" in state
        ), "Expected GrantTokens to be set in state"

        assert (
            "example-grant-token"
            in state["action-aws-kms-creategrants-name/GrantTokens"]
        ), "Expected GrantToken to be ['example-grant-token']"

        assert (
            "action-aws-kms-creategrants-name/KmsKeyId" in state
        ), "Expected KeyId to be set in state"

        assert (
            state["action-aws-kms-creategrants-name/KmsKeyId"]
            == "kms-key-id-1234567890abcdef"
        ), "Expected KeyId to be 'kms-key-id-1234567890abcdef'"

    except Exception as e:
        print(traceback.format_exc())
        assert False, str(e)
