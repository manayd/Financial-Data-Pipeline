from unittest.mock import MagicMock

from botocore.exceptions import ClientError

from infra.config import InfraSettings
from infra.database import create_database, destroy_database


def test_create_database_new():
    session = MagicMock()
    rds = MagicMock()
    session.client.return_value = rds
    settings = InfraSettings()

    # Subnet group does not exist
    rds.describe_db_subnet_groups.side_effect = ClientError(
        {"Error": {"Code": "DBSubnetGroupNotFoundFault", "Message": ""}}, "Describe"
    )
    # DB instance does not exist
    rds.describe_db_instances.side_effect = [
        ClientError({"Error": {"Code": "DBInstanceNotFoundFault", "Message": ""}}, "Describe"),
        # After creation and wait
        {"DBInstances": [{"Endpoint": {"Address": "db.example.com", "Port": 5432}}]},
    ]

    result = create_database(session, settings, ["subnet-1", "subnet-2"], "sg-db", "pass123")

    rds.create_db_subnet_group.assert_called_once()
    rds.create_db_instance.assert_called_once()
    create_kwargs = rds.create_db_instance.call_args[1]
    assert create_kwargs["Engine"] == "postgres"
    assert create_kwargs["EngineVersion"] == "16"
    assert create_kwargs["DBInstanceClass"] == "db.t3.micro"
    assert create_kwargs["DBName"] == "financial_data"
    assert create_kwargs["MasterUsername"] == "pipeline"
    assert create_kwargs["MasterUserPassword"] == "pass123"
    assert create_kwargs["PubliclyAccessible"] is False
    assert create_kwargs["StorageEncrypted"] is True
    assert create_kwargs["MultiAZ"] is False
    assert result["endpoint"] == "db.example.com"
    assert result["port"] == 5432


def test_create_database_idempotent():
    session = MagicMock()
    rds = MagicMock()
    session.client.return_value = rds
    settings = InfraSettings()

    # Both exist
    rds.describe_db_subnet_groups.return_value = {}
    rds.describe_db_instances.return_value = {
        "DBInstances": [{"Endpoint": {"Address": "existing.rds.amazonaws.com", "Port": 5432}}]
    }

    result = create_database(session, settings, ["subnet-1"], "sg-db", "pass")

    rds.create_db_instance.assert_not_called()
    assert result["endpoint"] == "existing.rds.amazonaws.com"


def test_destroy_database():
    session = MagicMock()
    rds = MagicMock()
    session.client.return_value = rds
    settings = InfraSettings()

    destroy_database(session, settings)

    rds.modify_db_instance.assert_called_once()
    rds.delete_db_instance.assert_called_once_with(
        DBInstanceIdentifier="financial-data-pipeline-dev-postgres",
        SkipFinalSnapshot=True,
    )


def test_destroy_database_not_found():
    session = MagicMock()
    rds = MagicMock()
    session.client.return_value = rds
    settings = InfraSettings()

    rds.modify_db_instance.side_effect = ClientError(
        {"Error": {"Code": "DBInstanceNotFoundFault", "Message": ""}}, "Modify"
    )

    # Should not raise
    destroy_database(session, settings)
