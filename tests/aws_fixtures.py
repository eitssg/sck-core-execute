import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta, timezone


@pytest.fixture
def real_aws(pytestconfig):
    return pytestconfig.getoption("--real-aws")


@pytest.fixture
def mock_aws(pytestconfig):
    return pytestconfig.getoption("--mock-aws")


@pytest.fixture(scope="session")
def mock_identity():
    identity = {
        "Arn": "arn:aws:iam::123456789012:user/jbarwick",
        "UserId": "AIDAJDPLRKLG7UEXAMPLE",
        "Account": "123456789012",
    }

    return identity


@pytest.fixture(scope="session")
def mock_credentials():

    credentials = {
        "AccessKeyId": "mock_access_key",
        "SecretAccessKey": "mock_secret_key",
        "SessionToken": "mock_session_token",
        "Expiration": datetime.now(timezone.utc) + timedelta(hours=1),
    }

    return credentials


@pytest.fixture(scope="session")
def mock_client(mock_credentials, mock_identity):

    mock_client = MagicMock()
    mock_client.get_caller_identity.return_value = mock_identity
    mock_client.assume_role.return_value = {
        "Credentials": mock_credentials,
        "ResponseMetadata": {"HTTPStatusCode": 200},
    }

    mock_client.get_session_token.return_value = mock_credentials

    return mock_client


@pytest.fixture(scope="session")
def mock_session_credentials(mock_credentials):

    mock_frozen_credentials = MagicMock()
    mock_frozen_credentials.access_key = mock_credentials["AccessKeyId"]
    mock_frozen_credentials.secret_key = mock_credentials["SecretAccessKey"]
    mock_frozen_credentials.token = mock_credentials["SessionToken"]

    mock_session_credentials = MagicMock()
    mock_session_credentials.get_frozen_credentials.return_value = (
        mock_frozen_credentials
    )

    return mock_session_credentials


@pytest.fixture(scope="session")
def mock_session(mock_client, mock_session_credentials):
    mock_session = MagicMock()
    mock_session.region_name = "us-west-2"
    mock_session.get_credentials.return_value = mock_session_credentials
    mock_session.client.return_value = mock_client

    with patch("boto3.session.Session", return_value=mock_session):
        yield mock_session
