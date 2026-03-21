from unittest.mock import MagicMock, patch

from infra.config import InfraSettings
from infra.kafka import create_msk_cluster, create_msk_topics


def test_create_msk_cluster_new():
    session = MagicMock()
    kafka = MagicMock()
    session.client.return_value = kafka
    settings = InfraSettings()

    # No existing cluster
    kafka.get_paginator.return_value.paginate.return_value = [{"ClusterInfoList": []}]
    kafka.create_cluster.return_value = {"ClusterArn": "arn:aws:kafka:us-east-1:123:cluster/test"}

    # Poll returns ACTIVE
    kafka.describe_cluster.return_value = {"ClusterInfo": {"State": "ACTIVE"}}
    kafka.get_bootstrap_brokers.return_value = {
        "BootstrapBrokerString": "b-1.kafka:9092,b-2.kafka:9092"
    }

    result = create_msk_cluster(session, settings, ["subnet-1", "subnet-2"], "sg-msk")

    kafka.create_cluster.assert_called_once()
    create_kwargs = kafka.create_cluster.call_args[1]
    assert create_kwargs["KafkaVersion"] == "3.6.0"
    assert create_kwargs["NumberOfBrokerNodes"] == 2
    assert result["bootstrap_servers"] == "b-1.kafka:9092,b-2.kafka:9092"


def test_create_msk_cluster_exists():
    session = MagicMock()
    kafka = MagicMock()
    session.client.return_value = kafka
    settings = InfraSettings()

    kafka.get_paginator.return_value.paginate.return_value = [
        {
            "ClusterInfoList": [
                {
                    "ClusterName": f"{settings.resource_prefix}-kafka",
                    "ClusterArn": "arn:existing",
                    "State": "ACTIVE",
                }
            ]
        }
    ]
    kafka.get_bootstrap_brokers.return_value = {"BootstrapBrokerString": "b-1:9092"}

    result = create_msk_cluster(session, settings, ["subnet-1"], "sg-msk")

    kafka.create_cluster.assert_not_called()
    assert result["cluster_arn"] == "arn:existing"


@patch("confluent_kafka.admin.AdminClient")
def test_create_msk_topics(mock_admin_cls):
    mock_admin = MagicMock()
    mock_admin_cls.return_value = mock_admin

    # No existing topics
    mock_admin.list_topics.return_value.topics = {}
    mock_admin.create_topics.return_value = {
        "raw-financial-news": MagicMock(),
        "raw-financial-news-dlq": MagicMock(),
        "processed-financial-news": MagicMock(),
        "llm-analysis-results": MagicMock(),
        "aggregation-results": MagicMock(),
    }
    # Mock future results
    for future in mock_admin.create_topics.return_value.values():
        future.result.return_value = None

    create_msk_topics("b-1:9092")

    mock_admin.create_topics.assert_called_once()
    new_topics = mock_admin.create_topics.call_args[0][0]
    assert len(new_topics) == 5


@patch("confluent_kafka.admin.AdminClient")
def test_create_msk_topics_already_exist(mock_admin_cls):
    mock_admin = MagicMock()
    mock_admin_cls.return_value = mock_admin

    # All topics exist
    mock_admin.list_topics.return_value.topics = {
        "raw-financial-news": None,
        "raw-financial-news-dlq": None,
        "processed-financial-news": None,
        "llm-analysis-results": None,
        "aggregation-results": None,
    }

    create_msk_topics("b-1:9092")

    mock_admin.create_topics.assert_not_called()
