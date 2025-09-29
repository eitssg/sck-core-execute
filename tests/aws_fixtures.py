from dataclasses import dataclass
from typing import Any
import pytest

import random
import string

from unittest.mock import MagicMock, patch

import core_framework as util
from core_helper.aws import store


@pytest.fixture
def real_aws(pytestconfig: pytest.Config):
    return pytestconfig.getoption("--real-aws")


@pytest.fixture
def mock_aws(pytestconfig: pytest.Config):
    return pytestconfig.getoption("--mock-aws")


@pytest.fixture(scope="module")
def mock_client(mock_identity) -> MagicMock:

    identity = {
        "Arn": "arn:aws:iam::123456789012:user/jbarwick",
        "UserId": "AIDAJDPLRKLG7UEXAMPLE",
        "Account": "123456789012",
    }

    credentials = {
        "AccessKeyId": "mock_access_key",
        "SecretAccessKey": "mock_secret_key",
        "SessionToken": "mock_session_token",
    }

    mock_client = MagicMock()
    mock_client.get_caller_identity.return_value = identity
    mock_client.assume_role.return_value = {
        "Credentials": credentials,
        "ResponseMetadata": {"HTTPStatusCode": 200},
    }

    mock_client.get_session_token.return_value = credentials

    return mock_client


def make_access_key():
    return "ASIA" + "".join(random.choices(string.ascii_uppercase + string.digits, k=16))


def make_secret_key():
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=40))


def make_session_token():
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=40))


def generate_role_credentials():
    return {
        "Credentials": {
            "AccessKeyId": make_access_key(),
            "SecretAccessKey": make_secret_key(),
            "SessionToken": make_session_token(),
        }
    }


@dataclass
class Config:
    aws_access_key_id = "mock_access_key"
    aws_secret_access_key = "mock_secret_key"
    aws_session_token = "mock_session_token"
    region_name = util.get_aws_region()
    profile_name = "default"
    aws_account_id = ""


role_creds: dict[str, Any] = {}
clients: dict[str, Any] = {}
sessions: dict[str, Any] = {}


def get_role_credentials(**kwargs) -> dict[str, Any]:
    """
    Returns what "assume_role()" would return.

    {
        "Credentials": {
            "AccessKeyId": "ASIA...",
            "SecretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "SessionToken": "FwoGZX IvYXdzE...",
            "Expiration": datetime(2011, 5, 13),
        },
    }

    """

    role_arn = kwargs.get("role_arn", kwargs.get("RoleArn", ""))
    if role_arn not in role_creds:
        role_creds[role_arn] = generate_role_credentials()
    return role_creds[role_arn]


def _generate_client_or_resource(service_name: str, **kwargs) -> MagicMock:

    credentials = kwargs.get("Credentials")
    if credentials:
        aws_access_key_id = credentials.get("AccessKeyId")
        aws_secret_access_key = credentials.get("SecretAccessKey")
        aws_session_token = credentials.get("SessionToken")
    else:
        aws_access_key_id = kwargs.get("aws_access_key_id")
        aws_secret_access_key = kwargs.get("aws_secret_access_key")
        aws_session_token = kwargs.get("aws_session_token")

    region_name = kwargs.get("region_name", Config.region_name)
    client_type = kwargs.get("client_type", Config.profile_name)
    aws_account_id = kwargs.get("aws_account_id", Config.aws_account_id)

    key = "-".join(
        [
            str(service_name),
            str(region_name),
            str(aws_access_key_id),
            str(aws_secret_access_key),
            str(aws_session_token),
        ]
    )
    if key not in clients:
        c = MagicMock(
            client_type=client_type,
            aws_account_id=aws_account_id,
            service_name=service_name,
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
        )
        c.assume_role.side_effect = get_role_credentials
        clients[key] = c
    return clients[key]


class MagicMockSession(MagicMock):

    def __init__(self, *args, **kwargs):

        if "region_name" not in kwargs or not kwargs["region_name"]:
            kwargs["region_name"] = Config.region_name
        if "aws_access_key_id" not in kwargs or not kwargs["aws_access_key_id"]:
            kwargs["aws_access_key_id"] = Config.aws_access_key_id
        if "aws_secret_access_key" not in kwargs or not kwargs["aws_secret_access_key"]:
            kwargs["aws_secret_access_key"] = Config.aws_secret_access_key
        if "aws_session_token" not in kwargs or not kwargs["aws_session_token"]:
            kwargs["aws_session_token"] = Config.aws_session_token
        if "profile_name" not in kwargs or not kwargs["profile_name"]:
            kwargs["profile_name"] = Config.profile_name
        if "aws_account_id" not in kwargs or not kwargs["aws_account_id"]:
            kwargs["aws_account_id"] = Config.aws_account_id

        super().__init__(*args, **kwargs)

    def get_credentials(self) -> MagicMock:

        mock_frozen_credentials = MagicMock()
        mock_frozen_credentials.access_key = self.aws_access_key_id
        mock_frozen_credentials.secret_key = self.aws_secret_access_key
        mock_frozen_credentials.token = self.aws_session_token

        credentials = MagicMock()
        credentials.get_frozen_credentials.return_value = mock_frozen_credentials

        return credentials

    def client(self, service_name: str, **kwargs) -> MagicMock:

        if "aws_account_id" not in kwargs:
            kwargs["aws_account_id"] = self.aws_account_id
        if "region_name" not in kwargs:
            kwargs["region_name"] = self.region_name
        if "aws_access_key_id" not in kwargs:
            kwargs["aws_access_key_id"] = self.aws_access_key_id
        if "aws_secret_access_key" not in kwargs:
            kwargs["aws_secret_access_key"] = self.aws_secret_access_key
        if "aws_session_token" not in kwargs:
            kwargs["aws_session_token"] = self.aws_session_token

        return _generate_client_or_resource(service_name, **kwargs)

    def resource(self, service_name: str, **kwargs) -> MagicMock:
        if "aws_account_id" not in kwargs:
            kwargs["aws_account_id"] = self.aws_account_id
        if "region_name" not in kwargs:
            kwargs["region_name"] = self.region_name
        if "aws_access_key_id" not in kwargs:
            kwargs["aws_access_key_id"] = self.aws_access_key_id
        if "aws_secret_access_key" not in kwargs:
            kwargs["aws_secret_access_key"] = self.aws_secret_access_key
        if "aws_session_token" not in kwargs:
            kwargs["aws_session_token"] = self.aws_session_token

        return _generate_client_or_resource(service_name, **kwargs)


def get_session(
    aws_access_key_id=None,
    aws_secret_access_key=None,
    aws_session_token=None,
    region_name=None,
    botocore_session=None,
    profile_name=None,
    aws_account_id=None,
    **kwargs,
):

    if not aws_access_key_id:
        aws_access_key_id = Config.aws_access_key_id
    if not aws_secret_access_key:
        aws_secret_access_key = Config.aws_secret_access_key
    if not aws_session_token:
        aws_session_token = Config.aws_session_token
    if not region_name:
        region_name = Config.region_name
    if not profile_name:
        profile_name = Config.profile_name
    if not aws_account_id:
        aws_account_id = Config.aws_account_id

    key = "-".join(
        [
            str(aws_access_key_id),
            str(aws_secret_access_key),
            str(aws_session_token),
            str(region_name),
            str(profile_name),
            str(aws_account_id),
        ]
    )
    if key not in sessions:
        sessions[key] = MagicMockSession(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=region_name,
            profile_name=profile_name,
            aws_account_id=aws_account_id,
            **kwargs,
        )
    return sessions[key]


def reset():

    store.reset()
    role_creds.clear()
    clients.clear()
    sessions.clear()


@pytest.fixture(scope="module")
def mock_session():

    with patch("boto3.session.Session", side_effect=get_session) as mock_session:
        yield mock_session
