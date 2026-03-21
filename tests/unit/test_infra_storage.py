from unittest.mock import MagicMock

from botocore.exceptions import ClientError

from infra.config import InfraSettings
from infra.storage import create_s3_bucket, destroy_s3_bucket


def test_create_s3_bucket_new():
    session = MagicMock()
    s3 = MagicMock()
    session.client.return_value = s3
    settings = InfraSettings()

    s3.head_bucket.side_effect = ClientError(
        {"Error": {"Code": "404", "Message": ""}}, "HeadBucket"
    )

    result = create_s3_bucket(session, settings)

    assert result == "financial-data-pipeline-dev-data-lake"
    s3.create_bucket.assert_called_once_with(Bucket="financial-data-pipeline-dev-data-lake")
    s3.put_bucket_versioning.assert_called_once()
    s3.put_bucket_encryption.assert_called_once()
    s3.put_public_access_block.assert_called_once()
    s3.put_bucket_lifecycle_configuration.assert_called_once()


def test_create_s3_bucket_exists():
    session = MagicMock()
    s3 = MagicMock()
    session.client.return_value = s3
    settings = InfraSettings()

    # head_bucket succeeds = bucket exists
    s3.head_bucket.return_value = {}

    result = create_s3_bucket(session, settings)

    assert result == "financial-data-pipeline-dev-data-lake"
    s3.create_bucket.assert_not_called()


def test_create_s3_bucket_non_us_east_1():
    session = MagicMock()
    s3 = MagicMock()
    session.client.return_value = s3
    settings = InfraSettings(aws_region="eu-west-1")

    s3.head_bucket.side_effect = ClientError(
        {"Error": {"Code": "404", "Message": ""}}, "HeadBucket"
    )

    create_s3_bucket(session, settings)

    call_kwargs = s3.create_bucket.call_args[1]
    assert call_kwargs["CreateBucketConfiguration"] == {"LocationConstraint": "eu-west-1"}


def test_destroy_s3_bucket():
    session = MagicMock()
    s3 = MagicMock()
    session.client.return_value = s3
    s3r = MagicMock()
    session.resource.return_value = s3r
    bucket_resource = MagicMock()
    s3r.Bucket.return_value = bucket_resource
    settings = InfraSettings()

    destroy_s3_bucket(session, settings)

    bucket_resource.object_versions.delete.assert_called_once()
    s3.delete_bucket.assert_called_once()
