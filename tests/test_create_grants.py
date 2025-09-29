from typing import Any
import traceback
import pytest
from unittest.mock import MagicMock

import core_framework as util

from core_framework.models import TaskPayload, DeploySpec

from core_execute.actionlib.actions.aws.kms.create_grants import (
    CreateGrantsActionResource,
    CreateGrantsActionSpec,
)
from core_execute.handler import handler as execute_handler
from core_execute.execute import save_actions, save_state, load_state

from .aws_fixtures import *

action_name = "kms-creategrants-test"


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
    return TaskPayload.model_validate(data)


@pytest.fixture
def deploy_spec():
    """
    Fixture to provide a deployspec data for testing.
    This can be used to mock the deployspec in tests.
    """
    spec_params = {
        "Account": "1234567890123",  # Example AWS account ID
        "Region": util.get_region(),  # Example AWS region
        "KmsKeyId": "kms-key-id-1234567890abcdef",  # Example KMS Key ID
        "GranteePrincipals": ["arn:aws:iam::123456789012:role/ExampleRole"],
        "Operations": ["Decrypt", "Encrypt", "GenerateDataKey"],
        "IgnoreFailedGrants": "false",  # Set to True to ignore failed grants
    }
    spec = CreateGrantsActionSpec.model_validate(spec_params)

    action_resource = CreateGrantsActionResource(name=action_name, spec=spec)

    return DeploySpec(actions=[action_resource])


def test_lambda_handler(task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session):

    try:

        # update the mock_session fixtures such that its client() returns a new mock kms client with the create_grants() function return value set appropraitely
        mock_kms_client = mock_session().client(
            'kms',
            client_type="role",
            region_name=util.get_region(),
            **get_role_credentials(
                RoleArn=util.get_provisioning_role_arn("1234567890123"),
            ),
        )

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
        mock_kms_client.create_grant.return_value = {
            "GrantId": "example-grant-id",
            "GrantToken": "example-grant-token",
        }

        # Add retire_grant mock for the _unexecute method (if needed)
        mock_kms_client.retire_grant.return_value = {}

        save_actions(task_payload, deploy_spec.actions)
        save_state(task_payload, {})

        # Create TaskPayload instance from the payload data.  This validates the structure and populates defauluts.

        event = task_payload.model_dump()

        response = execute_handler(event, None)

        # Validate the response structure and content

        task_payload = TaskPayload.model_validate(response)

        assert task_payload.task == "deploy"

        assert task_payload.flow_control == "success", "Expected flow_control to be 'success'"

        state = load_state(task_payload)

        assert state is not None, "Expected state to be loaded successfully"

        assert f"var/{action_name}/GrantIds" in state, "Expected GrantId to be set in state"

        assert "example-grant-id" in state[f"var/{action_name}/GrantIds"], "Expected GrantIds to be ['example-grant-id']"

        assert f"var/{action_name}/GrantTokens" in state, "Expected GrantTokens to be set in state"

        assert "example-grant-token" in state[f"var/{action_name}/GrantTokens"], "Expected GrantToken to be ['example-grant-token']"

        assert f"var/{action_name}/KmsKeyId" in state, "Expected KeyId to be set in state"

        assert (
            state[f"var/{action_name}/KmsKeyId"] == "kms-key-id-1234567890abcdef"
        ), "Expected KeyId to be 'kms-key-id-1234567890abcdef'"

    except Exception as e:
        print(traceback.format_exc())
        assert False, str(e)
