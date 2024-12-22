import pytest
import os
from core_execute import handler


@pytest.fixture
def real_aws(pytestconfig):
    return pytestconfig.getoption("--real-aws")


def test_lambda_handler():

    os.environ["AWS_PROFILE"] = "test_profile"
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["LOCAL_MODE"] = "true"

    event = {
        "Package": {
            "Branch": "Branch",
            "Build": "Build",
            "Mode": "Mode",
            "Portfolio": "Portfolio",
            "Region": "Region",
        },
        "DeploymentDetails": {"BranchShortName": "BranchShortName", "Build": "Build"},
    }
    context = {}

    response = handler(event, context)

    assert response == "Hello from Lambda!"
