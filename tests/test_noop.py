import traceback
import pytest
import json

from core_framework.models import TaskPayload, DeploySpec

from core_execute.actionlib.actions.system.no_op import NoOpActionSpec
from core_execute.actionlib.factory import ActionFactory
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
    params = {
        "Account": "1234567890123",  # Example AWS account ID
        "Region": "us-east-1",  # Example AWS region
    }

    # Define the action specifications with the no-op action
    action_spec = NoOpActionSpec(**{"params": params})

    # Please note that "DeploySpec" is NOT part of sck-core-execute.  However, the model is defined within the core framework
    # and is intantiated here only to be illustrative.  Plus, if you wanted to test multiple actions in the array, the
    # DeploySpec model does have a validator that inspects all actions.
    return DeploySpec(**{"actions": [action_spec]})


from core_execute.execute import save_state, save_actions


def test_lambda_handler(task_payload: TaskPayload, deploy_spec: DeploySpec):

    try:

        save_actions(task_payload, deploy_spec.actions)
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
