from unittest.mock import MagicMock

from botocore.exceptions import ClientError

from infra.config import InfraSettings
from infra.secrets import create_secrets, generate_db_password, update_database_secret


def test_create_secrets_new():
    session = MagicMock()
    sm = MagicMock()
    session.client.return_value = sm
    settings = InfraSettings()

    sm.describe_secret.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": ""}}, "Describe"
    )
    sm.create_secret.return_value = {"ARN": "arn:aws:secretsmanager:us-east-1:123:secret:test"}

    result = create_secrets(session, settings)

    assert len(result) == 4
    assert sm.create_secret.call_count == 4
    assert "database-url" in result
    assert "openai-api-key" in result


def test_create_secrets_idempotent():
    session = MagicMock()
    sm = MagicMock()
    session.client.return_value = sm
    settings = InfraSettings()

    sm.describe_secret.return_value = {"ARN": "arn:existing"}

    result = create_secrets(session, settings)

    assert len(result) == 4
    sm.create_secret.assert_not_called()


def test_update_database_secret():
    session = MagicMock()
    sm = MagicMock()
    session.client.return_value = sm
    settings = InfraSettings()

    update_database_secret(session, settings, "my-db.rds.amazonaws.com", "s3cret")

    sm.put_secret_value.assert_called_once()
    call_args = sm.put_secret_value.call_args[1]
    assert "my-db.rds.amazonaws.com" in call_args["SecretString"]
    assert "s3cret" in call_args["SecretString"]
    assert "pipeline" in call_args["SecretString"]


def test_generate_db_password():
    pw = generate_db_password()
    assert len(pw) > 20
    assert pw != generate_db_password()  # not deterministic
