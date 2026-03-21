from __future__ import annotations

from typing import Any

import boto3
import structlog
from botocore.exceptions import ClientError

from infra.config import InfraSettings
from infra.helpers import tag_list, wait_with_log

logger = structlog.get_logger(__name__)


def create_database(
    session: boto3.Session,
    settings: InfraSettings,
    private_subnet_ids: list[str],
    sg_db_id: str,
    db_password: str,
) -> dict[str, Any]:
    rds = session.client("rds")
    prefix = settings.resource_prefix
    instance_id = f"{prefix}-postgres"
    subnet_group = f"{prefix}-db-subnets"

    # --- Subnet group ---
    try:
        rds.describe_db_subnet_groups(DBSubnetGroupName=subnet_group)
        logger.info("db_subnet_group_exists", name=subnet_group)
    except ClientError as e:
        if e.response["Error"]["Code"] != "DBSubnetGroupNotFoundFault":
            raise
        rds.create_db_subnet_group(
            DBSubnetGroupName=subnet_group,
            DBSubnetGroupDescription=f"Private subnets for {prefix} RDS",
            SubnetIds=private_subnet_ids,
            Tags=tag_list(settings.default_tags),
        )
        logger.info("db_subnet_group_created", name=subnet_group)

    # --- RDS instance ---
    try:
        resp = rds.describe_db_instances(DBInstanceIdentifier=instance_id)
        db = resp["DBInstances"][0]
        endpoint = db["Endpoint"]["Address"]
        port = db["Endpoint"]["Port"]
        logger.info("rds_instance_exists", instance_id=instance_id, endpoint=endpoint)
    except ClientError as e:
        if e.response["Error"]["Code"] != "DBInstanceNotFoundFault":
            raise
        rds.create_db_instance(
            DBInstanceIdentifier=instance_id,
            DBInstanceClass=settings.db_instance_class,
            Engine="postgres",
            EngineVersion="16",
            MasterUsername=settings.db_username,
            MasterUserPassword=db_password,
            DBName=settings.db_name,
            AllocatedStorage=settings.db_allocated_storage,
            DBSubnetGroupName=subnet_group,
            VpcSecurityGroupIds=[sg_db_id],
            PubliclyAccessible=False,
            StorageEncrypted=True,
            BackupRetentionPeriod=1,
            MultiAZ=False,
            DeletionProtection=False,
            CopyTagsToSnapshot=True,
            Tags=tag_list(settings.default_tags),
        )
        logger.info("rds_instance_creating", instance_id=instance_id)

        wait_with_log(
            rds,
            "db_instance_available",
            "Waiting for RDS instance (5-10 min)...",
            delay=30,
            max_attempts=40,
            DBInstanceIdentifier=instance_id,
        )

        resp = rds.describe_db_instances(DBInstanceIdentifier=instance_id)
        db = resp["DBInstances"][0]
        endpoint = db["Endpoint"]["Address"]
        port = db["Endpoint"]["Port"]
        logger.info("rds_instance_created", instance_id=instance_id, endpoint=endpoint)

    return {"instance_id": instance_id, "endpoint": endpoint, "port": port}


def destroy_database(session: boto3.Session, settings: InfraSettings) -> None:
    rds = session.client("rds")
    prefix = settings.resource_prefix
    instance_id = f"{prefix}-postgres"
    subnet_group = f"{prefix}-db-subnets"

    try:
        rds.modify_db_instance(DBInstanceIdentifier=instance_id, DeletionProtection=False)
        rds.delete_db_instance(DBInstanceIdentifier=instance_id, SkipFinalSnapshot=True)
        logger.info("rds_instance_deleting", instance_id=instance_id)
        wait_with_log(
            rds,
            "db_instance_deleted",
            "Waiting for RDS deletion...",
            delay=30,
            max_attempts=40,
            DBInstanceIdentifier=instance_id,
        )
        logger.info("rds_instance_deleted", instance_id=instance_id)
    except ClientError as e:
        if e.response["Error"]["Code"] == "DBInstanceNotFoundFault":
            logger.info("rds_instance_not_found", instance_id=instance_id)
        else:
            raise

    try:
        rds.delete_db_subnet_group(DBSubnetGroupName=subnet_group)
        logger.info("db_subnet_group_deleted", name=subnet_group)
    except ClientError as e:
        if e.response["Error"]["Code"] != "DBSubnetGroupNotFoundFault":
            raise
