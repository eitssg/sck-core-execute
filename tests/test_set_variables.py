import traceback
import pytest

from core_framework.models import TaskPayload, DeploySpec

from core_execute.actionlib.actions.system.set_variables import SetVariablesActionSpec
from core_execute.handler import handler as execute_handler

from core_execute.execute import save_state, save_actions, load_state

from .aws_fixtures import *


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
def deploy_spec(task_payload: dict):
    """
    Fixture to provide a deployspec data for testing.
    This can be used to mock the deployspec in tests.
    """

    data = {
        "Account": "1234567890123",  # Example AWS account ID
        "Region": "us-east-1",  # Example AWS region
        "Variables": {
            "Name": "John Smith",
            "Age": 25,
            "Height": "6'2",
            "Weight": 180,
        },
    }

    # Define the action specifications with the no-op action
    set_variables_action = SetVariablesActionSpec(**{"Params": data})

    return DeploySpec(**{"actions": [set_variables_action]})


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

        # I need to check the state information

        state = load_state(task_payload)

        assert state is not None, "Expected state to be loaded successfully"
        assert (
            "action-system-set-variables-name/Name" in state
        ), "Expected variable 'Name' to be set in state"
        assert (
            state["action-system-set-variables-name/Name"] == "John Smith"
        ), "Expected variable 'Name' to be 'John Smith'"
        assert (
            "action-system-set-variables-name/Age" in state
        ), "Expected variable 'Age' to be set in state"
        assert (
            state["action-system-set-variables-name/Age"] == 25
        ), "Expected variable 'Age' to be 25"
        assert (
            "action-system-set-variables-name/Height" in state
        ), "Expected variable 'Height' to be set in state"
        assert (
            state["action-system-set-variables-name/Height"] == "6'2"
        ), "Expected variable 'Height' to be '6'2'"
        assert (
            "action-system-set-variables-name/Weight" in state
        ), "Expected variable 'Weight' to be set in state"
        assert (
            state["action-system-set-variables-name/Weight"] == 180
        ), "Expected variable 'Weight' to be 180"

    except Exception as e:
        print(traceback.format_exc())
        assert False, str(e)
