from unittest.mock import MagicMock

from botocore.exceptions import ClientError

from infra.config import InfraSettings
from infra.ecs import (
    create_ecs_cluster,
    create_ecs_services,
    create_execution_role,
    create_log_groups,
    create_task_role,
    register_task_definitions,
)


def test_create_execution_role_new():
    session = MagicMock()
    iam = MagicMock()
    session.client.return_value = iam
    settings = InfraSettings()

    iam.get_role.side_effect = ClientError(
        {"Error": {"Code": "NoSuchEntity", "Message": ""}}, "GetRole"
    )
    iam.create_role.return_value = {"Role": {"Arn": "arn:aws:iam::123:role/exec"}}

    arn = create_execution_role(session, settings, ["arn:secret1"])

    iam.create_role.assert_called_once()
    iam.attach_role_policy.assert_called_once()
    iam.put_role_policy.assert_called_once()
    assert arn == "arn:aws:iam::123:role/exec"


def test_create_task_role_policy():
    session = MagicMock()
    iam = MagicMock()
    session.client.return_value = iam
    settings = InfraSettings()

    iam.get_role.side_effect = ClientError(
        {"Error": {"Code": "NoSuchEntity", "Message": ""}}, "GetRole"
    )
    iam.create_role.return_value = {"Role": {"Arn": "arn:task-role"}}

    create_task_role(session, settings, "arn:aws:s3:::my-bucket", "arn:aws:kafka:cluster")

    iam.put_role_policy.assert_called_once()
    policy_doc = iam.put_role_policy.call_args[1]["PolicyDocument"]
    assert "s3:PutObject" in policy_doc
    assert "kafka-cluster:Connect" in policy_doc
    assert "arn:aws:s3:::my-bucket" in policy_doc


def test_create_log_groups():
    session = MagicMock()
    logs = MagicMock()
    session.client.return_value = logs
    settings = InfraSettings()

    logs.describe_log_groups.return_value = {"logGroups": []}

    groups = create_log_groups(session, settings)

    assert len(groups) == 5
    assert logs.create_log_group.call_count == 5
    assert logs.put_retention_policy.call_count == 5
    assert all(v.startswith("/ecs/") for v in groups.values())


def test_create_ecs_cluster_new():
    session = MagicMock()
    ecs = MagicMock()
    session.client.return_value = ecs
    settings = InfraSettings()

    ecs.describe_clusters.return_value = {"clusters": []}
    ecs.create_cluster.return_value = {"cluster": {"clusterArn": "arn:cluster"}}

    arn = create_ecs_cluster(session, settings)

    ecs.create_cluster.assert_called_once()
    assert arn == "arn:cluster"
    create_kwargs = ecs.create_cluster.call_args[1]
    assert "FARGATE_SPOT" in create_kwargs["capacityProviders"]


def test_create_ecs_cluster_exists():
    session = MagicMock()
    ecs = MagicMock()
    session.client.return_value = ecs
    settings = InfraSettings()

    ecs.describe_clusters.return_value = {
        "clusters": [{"clusterArn": "arn:existing", "status": "ACTIVE"}]
    }

    arn = create_ecs_cluster(session, settings)

    ecs.create_cluster.assert_not_called()
    assert arn == "arn:existing"


def test_register_task_definitions():
    session = MagicMock()
    ecs = MagicMock()
    session.client.return_value = ecs
    settings = InfraSettings()

    ecs.register_task_definition.return_value = {
        "taskDefinition": {"taskDefinitionArn": "arn:task-def"}
    }

    ecr_uris = {s: f"123.dkr.ecr.us-east-1.amazonaws.com/{s}" for s in settings.services}
    log_groups = {s: f"/ecs/test/{s}" for s in settings.services}
    secret_arns = {
        "database-url": "arn:db",
        "openai-api-key": "arn:openai",
        "anthropic-api-key": "arn:anthropic",
        "alpha-vantage-api-key": "arn:av",
    }

    result = register_task_definitions(
        session,
        settings,
        ecr_uris,
        "arn:exec-role",
        "arn:task-role",
        log_groups,
        secret_arns,
        "b-1:9092",
        "my-bucket",
    )

    assert len(result) == 5
    assert ecs.register_task_definition.call_count == 5

    # Verify API task has port mapping
    for call in ecs.register_task_definition.call_args_list:
        kwargs = call[1]
        if "api" in kwargs["family"]:
            container = kwargs["containerDefinitions"][0]
            assert container["portMappings"] == [{"containerPort": 8000, "protocol": "tcp"}]

    # Verify processor gets more resources
    for call in ecs.register_task_definition.call_args_list:
        kwargs = call[1]
        if "processor" in kwargs["family"]:
            assert kwargs["cpu"] == "512"
            assert kwargs["memory"] == "1024"


def test_create_ecs_services_creates_five():
    session = MagicMock()
    ecs = MagicMock()
    session.client.return_value = ecs
    settings = InfraSettings()

    # No existing services
    ecs.describe_services.return_value = {"services": []}
    ecs.create_service.return_value = {"service": {"serviceArn": "arn:svc"}}

    task_arns = {s: f"arn:task/{s}" for s in settings.services}

    result = create_ecs_services(
        session,
        settings,
        "arn:cluster",
        task_arns,
        ["subnet-1"],
        "sg-ecs",
        "arn:tg",
    )

    assert len(result) == 5
    assert ecs.create_service.call_count == 5

    # Verify API service has load balancer
    for call in ecs.create_service.call_args_list:
        kwargs = call[1]
        if "api" in kwargs["serviceName"]:
            assert "loadBalancers" in kwargs
            assert kwargs["loadBalancers"][0]["containerPort"] == 8000
        else:
            assert "loadBalancers" not in kwargs


def test_create_ecs_services_updates_existing():
    session = MagicMock()
    ecs = MagicMock()
    session.client.return_value = ecs
    settings = InfraSettings()

    # Service already exists
    ecs.describe_services.return_value = {
        "services": [{"serviceArn": "arn:existing-svc", "status": "ACTIVE"}]
    }

    task_arns = {s: f"arn:task/{s}" for s in settings.services}

    create_ecs_services(
        session,
        settings,
        "arn:cluster",
        task_arns,
        ["subnet-1"],
        "sg-ecs",
        "arn:tg",
    )

    assert ecs.update_service.call_count == 5
    ecs.create_service.assert_not_called()
