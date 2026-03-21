from __future__ import annotations

from typing import Any

import boto3
import structlog

from infra.config import InfraSettings
from infra.helpers import poll_until_status

logger = structlog.get_logger(__name__)

TOPICS = [
    ("raw-financial-news", 6),
    ("raw-financial-news-dlq", 3),
    ("processed-financial-news", 6),
    ("llm-analysis-results", 6),
    ("aggregation-results", 3),
]


def _find_cluster(kafka_client: Any, name: str) -> dict[str, Any] | None:
    paginator = kafka_client.get_paginator("list_clusters_v2")
    for page in paginator.paginate():
        for cluster in page.get("ClusterInfoList", []):
            if cluster.get("ClusterName") == name and cluster["State"] in ("ACTIVE", "CREATING"):
                return cluster
    return None


def create_msk_cluster(
    session: boto3.Session,
    settings: InfraSettings,
    private_subnet_ids: list[str],
    sg_msk_id: str,
) -> dict[str, Any]:
    kafka_client = session.client("kafka")
    prefix = settings.resource_prefix
    cluster_name = f"{prefix}-kafka"

    existing = _find_cluster(kafka_client, cluster_name)
    if existing and existing["State"] == "ACTIVE":
        cluster_arn = existing["ClusterArn"]
        logger.info("msk_cluster_exists", cluster_name=cluster_name, arn=cluster_arn)
    elif existing and existing["State"] == "CREATING":
        cluster_arn = existing["ClusterArn"]
        logger.info("msk_cluster_creating_in_progress", cluster_name=cluster_name)
        poll_until_status(
            describe_fn=lambda: kafka_client.describe_cluster(ClusterArn=cluster_arn),
            status_path=["ClusterInfo", "State"],
            target_status="ACTIVE",
            poll_interval=30,
            max_wait=1800,
            resource_name=cluster_name,
        )
    else:
        resp = kafka_client.create_cluster(
            ClusterName=cluster_name,
            KafkaVersion="3.6.0",
            NumberOfBrokerNodes=settings.msk_broker_count,
            BrokerNodeGroupInfo={
                "InstanceType": settings.msk_instance_type,
                "ClientSubnets": private_subnet_ids,
                "SecurityGroups": [sg_msk_id],
                "StorageInfo": {"EbsStorageInfo": {"VolumeSize": settings.msk_volume_size}},
            },
            EncryptionInfo={
                "EncryptionInTransit": {
                    "ClientBroker": "TLS_PLAINTEXT",
                    "InCluster": True,
                },
            },
            Tags=settings.default_tags,
        )
        cluster_arn = resp["ClusterArn"]
        logger.info("msk_cluster_creating", cluster_name=cluster_name, arn=cluster_arn)

        poll_until_status(
            describe_fn=lambda: kafka_client.describe_cluster(ClusterArn=cluster_arn),
            status_path=["ClusterInfo", "State"],
            target_status="ACTIVE",
            poll_interval=30,
            max_wait=1800,
            resource_name=cluster_name,
        )
        logger.info("msk_cluster_active", cluster_name=cluster_name)

    brokers_resp = kafka_client.get_bootstrap_brokers(ClusterArn=cluster_arn)
    bootstrap = brokers_resp.get("BootstrapBrokerString") or brokers_resp.get(
        "BootstrapBrokerStringTls", ""
    )
    logger.info("msk_bootstrap_servers", servers=bootstrap)

    return {"cluster_arn": cluster_arn, "bootstrap_servers": bootstrap}


def create_msk_topics(bootstrap_servers: str) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing = set(admin.list_topics(timeout=10).topics.keys())

    new_topics = []
    for topic_name, partitions in TOPICS:
        if topic_name in existing:
            logger.info("msk_topic_exists", topic=topic_name)
        else:
            new_topics.append(NewTopic(topic_name, num_partitions=partitions, replication_factor=2))

    if new_topics:
        futures = admin.create_topics(new_topics)
        for topic_name, future in futures.items():
            future.result()
            logger.info("msk_topic_created", topic=topic_name)


def destroy_msk_cluster(session: boto3.Session, settings: InfraSettings) -> None:
    kafka_client = session.client("kafka")
    cluster_name = f"{settings.resource_prefix}-kafka"

    existing = _find_cluster(kafka_client, cluster_name)
    if not existing:
        logger.info("msk_cluster_not_found", cluster_name=cluster_name)
        return

    cluster_arn = existing["ClusterArn"]
    kafka_client.delete_cluster(ClusterArn=cluster_arn)
    logger.info("msk_cluster_deleting", cluster_name=cluster_name)

    poll_until_status(
        describe_fn=lambda: kafka_client.describe_cluster(ClusterArn=cluster_arn),
        status_path=["ClusterInfo", "State"],
        target_status="DELETED",
        poll_interval=30,
        max_wait=1800,
        resource_name=cluster_name,
    )
    logger.info("msk_cluster_deleted", cluster_name=cluster_name)
