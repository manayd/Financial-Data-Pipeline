from __future__ import annotations

import json

import boto3
import structlog
from botocore.exceptions import ClientError

from infra.config import InfraSettings
from infra.helpers import tag_list

logger = structlog.get_logger(__name__)

LIFECYCLE_POLICY = json.dumps(
    {
        "rules": [
            {
                "rulePriority": 1,
                "description": "Keep last 10 images",
                "selection": {
                    "tagStatus": "any",
                    "countType": "imageCountMoreThan",
                    "countNumber": 10,
                },
                "action": {"type": "expire"},
            }
        ]
    }
)


def create_ecr_repositories(session: boto3.Session, settings: InfraSettings) -> dict[str, str]:
    ecr = session.client("ecr")
    prefix = settings.resource_prefix
    repos: dict[str, str] = {}

    for service in settings.services:
        repo_name = f"{prefix}-{service}"
        try:
            resp = ecr.describe_repositories(repositoryNames=[repo_name])
            uri = resp["repositories"][0]["repositoryUri"]
            logger.info("ecr_repo_exists", repo=repo_name, uri=uri)
        except ClientError as e:
            if e.response["Error"]["Code"] != "RepositoryNotFoundException":
                raise
            resp = ecr.create_repository(
                repositoryName=repo_name,
                imageScanningConfiguration={"scanOnPush": True},
                imageTagMutability="MUTABLE",
                tags=tag_list(settings.default_tags),
            )
            uri = resp["repository"]["repositoryUri"]
            logger.info("ecr_repo_created", repo=repo_name, uri=uri)

        ecr.put_lifecycle_policy(repositoryName=repo_name, lifecyclePolicyText=LIFECYCLE_POLICY)
        repos[service] = uri

    return repos


def destroy_ecr_repositories(session: boto3.Session, settings: InfraSettings) -> None:
    ecr = session.client("ecr")
    prefix = settings.resource_prefix

    for service in settings.services:
        repo_name = f"{prefix}-{service}"
        try:
            ecr.delete_repository(repositoryName=repo_name, force=True)
            logger.info("ecr_repo_deleted", repo=repo_name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "RepositoryNotFoundException":
                logger.info("ecr_repo_not_found", repo=repo_name)
            else:
                raise
