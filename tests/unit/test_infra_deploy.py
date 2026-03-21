from unittest.mock import MagicMock, patch

from infra.config import InfraSettings
from infra.deploy import deploy


@patch("infra.deploy.create_ecs_services")
@patch("infra.deploy.create_ecs_cluster")
@patch("infra.deploy.register_task_definitions")
@patch("infra.deploy.create_log_groups")
@patch("infra.deploy.create_task_role")
@patch("infra.deploy.create_execution_role")
@patch("infra.deploy.create_msk_topics")
@patch("infra.deploy.update_database_secret")
@patch("infra.deploy.create_alb")
@patch("infra.deploy.create_msk_cluster")
@patch("infra.deploy.create_database")
@patch("infra.deploy.generate_db_password")
@patch("infra.deploy.create_secrets")
@patch("infra.deploy.create_s3_bucket")
@patch("infra.deploy.create_ecr_repositories")
@patch("infra.deploy.create_networking")
@patch("infra.deploy.get_boto3_session")
def test_deploy_calls_all_phases(
    mock_session,
    mock_net,
    mock_ecr,
    mock_s3,
    mock_secrets,
    mock_pw,
    mock_db,
    mock_msk,
    mock_alb,
    mock_update_secret,
    mock_topics,
    mock_exec_role,
    mock_task_role,
    mock_logs,
    mock_task_defs,
    mock_cluster,
    mock_services,
):
    settings = InfraSettings()
    mock_session.return_value = MagicMock()
    mock_net.return_value = {
        "vpc_id": "vpc-1",
        "public_subnet_ids": ["s1"],
        "private_subnet_ids": ["s2"],
        "sg_alb_id": "sg1",
        "sg_ecs_id": "sg2",
        "sg_db_id": "sg3",
        "sg_msk_id": "sg4",
    }
    mock_ecr.return_value = {"producer": "uri"}
    mock_s3.return_value = "my-bucket"
    mock_secrets.return_value = {"database-url": "arn:db"}
    mock_pw.return_value = "test-password"
    mock_db.return_value = {"endpoint": "db.example.com", "port": 5432, "instance_id": "db-1"}
    mock_msk.return_value = {"cluster_arn": "arn:msk", "bootstrap_servers": "b:9092"}
    mock_alb.return_value = {
        "alb_arn": "arn:alb",
        "alb_dns_name": "test.elb.com",
        "target_group_arn": "arn:tg",
    }
    mock_exec_role.return_value = "arn:exec"
    mock_task_role.return_value = "arn:task"
    mock_logs.return_value = {"producer": "/ecs/test/producer"}
    mock_task_defs.return_value = {"producer": "arn:td"}
    mock_cluster.return_value = "arn:cluster"
    mock_services.return_value = {"producer": "arn:svc"}

    result = deploy(settings)

    mock_net.assert_called_once()
    mock_ecr.assert_called_once()
    mock_s3.assert_called_once()
    mock_secrets.assert_called_once()
    mock_db.assert_called_once()
    mock_msk.assert_called_once()
    mock_alb.assert_called_once()
    mock_update_secret.assert_called_once()
    mock_exec_role.assert_called_once()
    mock_task_role.assert_called_once()
    mock_logs.assert_called_once()
    mock_task_defs.assert_called_once()
    mock_cluster.assert_called_once()
    mock_services.assert_called_once()

    assert "cluster_arn" in result
    assert result["s3_bucket"] == "my-bucket"


@patch("infra.deploy.create_ecs_services")
@patch("infra.deploy.create_ecs_cluster")
@patch("infra.deploy.register_task_definitions")
@patch("infra.deploy.create_log_groups")
@patch("infra.deploy.create_task_role")
@patch("infra.deploy.create_execution_role")
@patch("infra.deploy.create_msk_topics")
@patch("infra.deploy.update_database_secret")
@patch("infra.deploy.create_alb")
@patch("infra.deploy.create_msk_cluster")
@patch("infra.deploy.create_database")
@patch("infra.deploy.generate_db_password")
@patch("infra.deploy.create_secrets")
@patch("infra.deploy.create_s3_bucket")
@patch("infra.deploy.create_ecr_repositories")
@patch("infra.deploy.create_networking")
@patch("infra.deploy.get_boto3_session")
def test_deploy_topic_creation_failure_non_fatal(
    mock_session,
    mock_net,
    mock_ecr,
    mock_s3,
    mock_secrets,
    mock_pw,
    mock_db,
    mock_msk,
    mock_alb,
    mock_update_secret,
    mock_topics,
    mock_exec_role,
    mock_task_role,
    mock_logs,
    mock_task_defs,
    mock_cluster,
    mock_services,
):
    """Topic creation failure should not stop the deploy."""
    settings = InfraSettings()
    mock_session.return_value = MagicMock()
    mock_net.return_value = {
        "vpc_id": "vpc-1",
        "public_subnet_ids": ["s1"],
        "private_subnet_ids": ["s2"],
        "sg_alb_id": "sg1",
        "sg_ecs_id": "sg2",
        "sg_db_id": "sg3",
        "sg_msk_id": "sg4",
    }
    mock_ecr.return_value = {}
    mock_s3.return_value = "bucket"
    mock_secrets.return_value = {"database-url": "arn:db"}
    mock_pw.return_value = "pw"
    mock_db.return_value = {"endpoint": "db.com", "port": 5432, "instance_id": "db"}
    mock_msk.return_value = {"cluster_arn": "arn:msk", "bootstrap_servers": "b:9092"}
    mock_alb.return_value = {
        "alb_arn": "arn:alb",
        "alb_dns_name": "alb.com",
        "target_group_arn": "arn:tg",
    }
    mock_exec_role.return_value = "arn:exec"
    mock_task_role.return_value = "arn:task"
    mock_logs.return_value = {}
    mock_task_defs.return_value = {}
    mock_cluster.return_value = "arn:cluster"
    mock_services.return_value = {}

    mock_topics.side_effect = Exception("Cannot connect to MSK from local machine")

    # Should NOT raise
    result = deploy(settings)
    assert "cluster_arn" in result
