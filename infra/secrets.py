from __future__ import annotations

import secrets as stdlib_secrets

import boto3
import structlog
from botocore.exceptions import ClientError

from infra.config import InfraSettings

logger = structlog.get_logger(__name__)

SECRET_NAMES = ("database-url", "alpha-vantage-api-key")


def _secret_name(prefix: str, name: str) -> str:
    return f"{prefix}/{name}"


def generate_db_password() -> str:
    return stdlib_secrets.token_urlsafe(32)


def create_secrets(session: boto3.Session, settings: InfraSettings) -> dict[str, str]:
    sm = session.client("secretsmanager")
    prefix = settings.resource_prefix
    arns: dict[str, str] = {}

    for name in SECRET_NAMES:
        full_name = _secret_name(prefix, name)
        try:
            resp = sm.describe_secret(SecretId=full_name)
            arn = resp["ARN"]
            logger.info("secret_exists", name=full_name, arn=arn)
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceNotFoundException":
                raise
            initial_value = "CHANGE_ME"
            resp = sm.create_secret(
                Name=full_name,
                SecretString=initial_value,
                Tags=[
                    {"Key": "Project", "Value": settings.project_name},
                    {"Key": "Environment", "Value": settings.environment},
                    {"Key": "ManagedBy", "Value": "boto3"},
                ],
            )
            arn = resp["ARN"]
            logger.info("secret_created", name=full_name, arn=arn)

        arns[name] = arn

    return arns


def update_database_secret(
    session: boto3.Session,
    settings: InfraSettings,
    db_endpoint: str,
    db_password: str,
) -> None:
    sm = session.client("secretsmanager")
    full_name = _secret_name(settings.resource_prefix, "database-url")
    db_url = (
        f"postgresql+asyncpg://{settings.db_username}:{db_password}"
        f"@{db_endpoint}:5432/{settings.db_name}?ssl=require"
    )
    sm.put_secret_value(SecretId=full_name, SecretString=db_url)
    logger.info("database_secret_updated", name=full_name)


def destroy_secrets(session: boto3.Session, settings: InfraSettings) -> None:
    sm = session.client("secretsmanager")
    prefix = settings.resource_prefix

    for name in SECRET_NAMES:
        full_name = _secret_name(prefix, name)
        try:
            sm.delete_secret(SecretId=full_name, ForceDeleteWithoutRecovery=True)
            logger.info("secret_deleted", name=full_name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                logger.info("secret_not_found", name=full_name)
            else:
                raise
