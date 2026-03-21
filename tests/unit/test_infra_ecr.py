from unittest.mock import MagicMock

from botocore.exceptions import ClientError

from infra.config import InfraSettings
from infra.ecr import create_ecr_repositories, destroy_ecr_repositories


def _not_found_error():
    return ClientError(
        {"Error": {"Code": "RepositoryNotFoundException", "Message": ""}}, "Describe"
    )


def test_create_ecr_creates_five_repos():
    session = MagicMock()
    ecr = MagicMock()
    session.client.return_value = ecr
    settings = InfraSettings()

    ecr.describe_repositories.side_effect = _not_found_error()
    ecr.create_repository.return_value = {
        "repository": {"repositoryUri": "123.dkr.ecr.us-east-1.amazonaws.com/repo"}
    }

    result = create_ecr_repositories(session, settings)

    assert len(result) == 5
    assert ecr.create_repository.call_count == 5
    assert ecr.put_lifecycle_policy.call_count == 5


def test_create_ecr_idempotent():
    session = MagicMock()
    ecr = MagicMock()
    session.client.return_value = ecr
    settings = InfraSettings()

    ecr.describe_repositories.return_value = {
        "repositories": [{"repositoryUri": "123.dkr.ecr.us-east-1.amazonaws.com/existing"}]
    }

    result = create_ecr_repositories(session, settings)

    assert len(result) == 5
    ecr.create_repository.assert_not_called()
    # Lifecycle policy still applied
    assert ecr.put_lifecycle_policy.call_count == 5


def test_destroy_ecr():
    session = MagicMock()
    ecr = MagicMock()
    session.client.return_value = ecr
    settings = InfraSettings()

    destroy_ecr_repositories(session, settings)

    assert ecr.delete_repository.call_count == 5
    for call in ecr.delete_repository.call_args_list:
        assert call[1]["force"] is True


def test_destroy_ecr_not_found():
    session = MagicMock()
    ecr = MagicMock()
    session.client.return_value = ecr
    settings = InfraSettings()

    ecr.delete_repository.side_effect = ClientError(
        {"Error": {"Code": "RepositoryNotFoundException", "Message": ""}}, "Delete"
    )

    # Should not raise
    destroy_ecr_repositories(session, settings)
