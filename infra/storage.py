from __future__ import annotations

import boto3
import structlog
from botocore.exceptions import ClientError

from infra.config import InfraSettings
from infra.helpers import tag_list

logger = structlog.get_logger(__name__)


def _bucket_name(settings: InfraSettings) -> str:
    return f"{settings.resource_prefix}-data-lake"


def create_s3_bucket(session: boto3.Session, settings: InfraSettings) -> str:
    s3 = session.client("s3")
    bucket = _bucket_name(settings)

    try:
        s3.head_bucket(Bucket=bucket)
        logger.info("s3_bucket_exists", bucket=bucket)
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("404", "NoSuchBucket"):
            raise
        create_kwargs: dict = {"Bucket": bucket}
        if settings.aws_region != "us-east-1":
            create_kwargs["CreateBucketConfiguration"] = {"LocationConstraint": settings.aws_region}
        s3.create_bucket(**create_kwargs)
        logger.info("s3_bucket_created", bucket=bucket)

    s3.put_bucket_versioning(
        Bucket=bucket,
        VersioningConfiguration={"Status": "Enabled"},
    )

    s3.put_bucket_encryption(
        Bucket=bucket,
        ServerSideEncryptionConfiguration={
            "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]
        },
    )

    s3.put_public_access_block(
        Bucket=bucket,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        },
    )

    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket,
        LifecycleConfiguration={
            "Rules": [
                {
                    "ID": "transition-to-ia",
                    "Status": "Enabled",
                    "Filter": {"Prefix": "processed/"},
                    "Transitions": [
                        {"Days": 30, "StorageClass": "STANDARD_IA"},
                        {"Days": 90, "StorageClass": "GLACIER"},
                    ],
                    "NoncurrentVersionExpiration": {"NoncurrentDays": 30},
                }
            ]
        },
    )

    s3.put_bucket_tagging(
        Bucket=bucket,
        Tagging={"TagSet": tag_list(settings.default_tags)},
    )

    return bucket


def destroy_s3_bucket(session: boto3.Session, settings: InfraSettings) -> None:
    s3 = session.client("s3")
    bucket = _bucket_name(settings)

    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        logger.info("s3_bucket_not_found", bucket=bucket)
        return

    # Empty the bucket (including versions)
    s3r = session.resource("s3")
    bucket_resource = s3r.Bucket(bucket)
    bucket_resource.object_versions.delete()
    logger.info("s3_bucket_emptied", bucket=bucket)

    s3.delete_bucket(Bucket=bucket)
    logger.info("s3_bucket_deleted", bucket=bucket)
