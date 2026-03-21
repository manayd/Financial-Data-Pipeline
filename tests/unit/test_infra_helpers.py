from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from infra.helpers import (
    get_boto3_session,
    poll_until_status,
    resource_exists,
    tag_dict,
    tag_list,
    wait_with_log,
)


def test_tag_list():
    result = tag_list({"Project": "test", "Env": "dev"})
    assert {"Key": "Project", "Value": "test"} in result
    assert {"Key": "Env", "Value": "dev"} in result


def test_tag_list_empty():
    assert tag_list({}) == []


def test_tag_dict():
    tags = [{"Key": "Project", "Value": "test"}, {"Key": "Env", "Value": "dev"}]
    result = tag_dict(tags)
    assert result == {"Project": "test", "Env": "dev"}


def test_tag_dict_empty():
    assert tag_dict([]) == {}


def test_tag_roundtrip():
    original = {"A": "1", "B": "2"}
    assert tag_dict(tag_list(original)) == original


def test_resource_exists_found():
    result = resource_exists(lambda: {"id": "abc"})
    assert result == {"id": "abc"}


def test_resource_exists_not_found():
    def raise_not_found():
        raise ClientError(
            {"Error": {"Code": "NotFoundException", "Message": "not found"}}, "Describe"
        )

    result = resource_exists(raise_not_found)
    assert result is None


def test_resource_exists_unexpected_error():
    def raise_other():
        raise ClientError({"Error": {"Code": "AccessDenied", "Message": "forbidden"}}, "Describe")

    with pytest.raises(ClientError):
        resource_exists(raise_other)


def test_resource_exists_custom_codes():
    def raise_custom():
        raise ClientError({"Error": {"Code": "DBInstanceNotFoundFault", "Message": ""}}, "Describe")

    result = resource_exists(raise_custom, not_found_codes=("DBInstanceNotFoundFault",))
    assert result is None


@patch("infra.helpers.boto3.Session")
def test_get_boto3_session(mock_session_cls):
    from infra.config import InfraSettings

    settings = InfraSettings()
    get_boto3_session(settings)
    mock_session_cls.assert_called_once_with(region_name="us-east-1")


def test_wait_with_log():
    mock_client = MagicMock()
    mock_waiter = MagicMock()
    mock_client.get_waiter.return_value = mock_waiter

    wait_with_log(mock_client, "db_instance_available", DBInstanceIdentifier="test-db")

    mock_client.get_waiter.assert_called_once_with("db_instance_available")
    mock_waiter.wait.assert_called_once()


def test_poll_until_status_immediate():
    describe = MagicMock(return_value={"Cluster": {"State": "ACTIVE"}})
    result = poll_until_status(
        describe, ["Cluster", "State"], "ACTIVE", poll_interval=1, resource_name="test"
    )
    assert result["Cluster"]["State"] == "ACTIVE"
    describe.assert_called_once()


def test_poll_until_status_failed():
    describe = MagicMock(return_value={"Cluster": {"State": "FAILED"}})
    with pytest.raises(RuntimeError, match="terminal state"):
        poll_until_status(
            describe, ["Cluster", "State"], "ACTIVE", poll_interval=1, resource_name="test"
        )
