from typing import Any
import traceback
import pytest

import core_framework as util

from core_framework.models import TaskPayload, DeploySpec

from core_execute.actionlib.actions.aws.kms.create_grants import CreateGrantsActionSpec
from core_execute.handler import handler as execute_handler


@pytest.fixture
def real_aws(pytestconfig):
    return pytestconfig.getoption("--real-aws")


@pytest.fixture
def mock_aws(pytestconfig):
    return pytestconfig.getoption("--mock-aws")


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
            "IgnoreFailedGrants": False,  # Set to True to ignore failed grants
        }
    }

    action_spec = CreateGrantsActionSpec(**spec)

    deploy_spec: dict[str, Any] = {"actions": [action_spec]}

    return DeploySpec(**deploy_spec)


from core_execute.execute import save_actions, save_state


def test_lambda_handler(task_payload: TaskPayload, deploy_spec: DeploySpec):

    try:

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

    except Exception as e:
        print(traceback.format_exc())
        assert False, str(e)
