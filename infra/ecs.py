from __future__ import annotations

import json
from typing import Any

import boto3
import structlog
from botocore.exceptions import ClientError

from infra.config import InfraSettings
from infra.helpers import tag_list, tag_list_ecs

logger = structlog.get_logger(__name__)

ECS_TRUST_POLICY = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "ecs-tasks.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }
)


# ---------------------------------------------------------------------------
# IAM Roles
# ---------------------------------------------------------------------------


def create_execution_role(
    session: boto3.Session,
    settings: InfraSettings,
    secret_arns: list[str],
) -> str:
    iam = session.client("iam")
    role_name = f"{settings.resource_prefix}-ecs-execution"

    try:
        resp = iam.get_role(RoleName=role_name)
        arn = resp["Role"]["Arn"]
        logger.info("execution_role_exists", role=role_name)
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchEntity":
            raise
        resp = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=ECS_TRUST_POLICY,
            Tags=tag_list(settings.default_tags),
        )
        arn = resp["Role"]["Arn"]
        iam.attach_role_policy(
            RoleName=role_name,
            PolicyArn="arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy",
        )
        logger.info("execution_role_created", role=role_name)

    # Inline policy for Secrets Manager access
    secrets_policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["secretsmanager:GetSecretValue"],
                    "Resource": secret_arns,
                }
            ],
        }
    )
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="secrets-access",
        PolicyDocument=secrets_policy,
    )

    return arn


def create_task_role(
    session: boto3.Session,
    settings: InfraSettings,
    s3_bucket_arn: str,
    msk_cluster_arn: str,
) -> str:
    iam = session.client("iam")
    role_name = f"{settings.resource_prefix}-ecs-task"

    try:
        resp = iam.get_role(RoleName=role_name)
        arn = resp["Role"]["Arn"]
        logger.info("task_role_exists", role=role_name)
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchEntity":
            raise
        resp = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=ECS_TRUST_POLICY,
            Tags=tag_list(settings.default_tags),
        )
        arn = resp["Role"]["Arn"]
        logger.info("task_role_created", role=role_name)

    task_policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "S3Access",
                    "Effect": "Allow",
                    "Action": ["s3:PutObject", "s3:GetObject", "s3:ListBucket"],
                    "Resource": [s3_bucket_arn, f"{s3_bucket_arn}/*"],
                },
                {
                    "Sid": "MSKAccess",
                    "Effect": "Allow",
                    "Action": [
                        "kafka-cluster:Connect",
                        "kafka-cluster:ReadData",
                        "kafka-cluster:WriteData",
                        "kafka-cluster:DescribeTopic",
                        "kafka-cluster:CreateTopic",
                        "kafka-cluster:DescribeGroup",
                        "kafka-cluster:AlterGroup",
                    ],
                    "Resource": [msk_cluster_arn, f"{msk_cluster_arn}/*"],
                },
                {
                    "Sid": "BedrockAccess",
                    "Effect": "Allow",
                    "Action": ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"],
                    "Resource": "*",
                },
                {
                    "Sid": "CloudWatchLogs",
                    "Effect": "Allow",
                    "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
                    "Resource": "*",
                },
            ],
        }
    )
    iam.put_role_policy(
        RoleName=role_name, PolicyName="task-permissions", PolicyDocument=task_policy
    )

    return arn


# ---------------------------------------------------------------------------
# CloudWatch Log Groups
# ---------------------------------------------------------------------------


def create_log_groups(session: boto3.Session, settings: InfraSettings) -> dict[str, str]:
    logs = session.client("logs")
    prefix = settings.resource_prefix
    groups: dict[str, str] = {}

    for service in settings.services:
        group_name = f"/ecs/{prefix}/{service}"
        try:
            existing = logs.describe_log_groups(logGroupNamePrefix=group_name)
            if any(g["logGroupName"] == group_name for g in existing["logGroups"]):
                logger.info("log_group_exists", name=group_name)
                groups[service] = group_name
                continue
        except ClientError:
            pass

        logs.create_log_group(logGroupName=group_name, tags=settings.default_tags)
        logs.put_retention_policy(logGroupName=group_name, retentionInDays=14)
        logger.info("log_group_created", name=group_name)
        groups[service] = group_name

    return groups


# ---------------------------------------------------------------------------
# Service-Linked Role
# ---------------------------------------------------------------------------


def ensure_ecs_service_linked_role(session: boto3.Session) -> None:
    iam = session.client("iam")
    try:
        iam.create_service_linked_role(AWSServiceName="ecs.amazonaws.com")
        logger.info("ecs_service_linked_role_created")
    except ClientError as e:
        if e.response["Error"]["Code"] == "InvalidInput" and "has been taken" in str(e):
            logger.info("ecs_service_linked_role_exists")
        else:
            raise


# ---------------------------------------------------------------------------
# ECS Cluster
# ---------------------------------------------------------------------------


def create_ecs_cluster(session: boto3.Session, settings: InfraSettings) -> str:
    ecs = session.client("ecs")
    cluster_name = f"{settings.resource_prefix}-cluster"

    resp = ecs.describe_clusters(clusters=[cluster_name])
    active = [c for c in resp["clusters"] if c["status"] == "ACTIVE"]
    if active:
        arn = active[0]["clusterArn"]
        logger.info("ecs_cluster_exists", name=cluster_name, arn=arn)
        return arn

    resp = ecs.create_cluster(
        clusterName=cluster_name,
        capacityProviders=["FARGATE", "FARGATE_SPOT"],
        defaultCapacityProviderStrategy=[
            {"capacityProvider": "FARGATE_SPOT", "weight": 1},
        ],
        tags=tag_list_ecs(settings.default_tags),
        settings=[{"name": "containerInsights", "value": "enabled"}],
    )
    arn = resp["cluster"]["clusterArn"]
    logger.info("ecs_cluster_created", name=cluster_name, arn=arn)
    return arn


# ---------------------------------------------------------------------------
# Task Definitions
# ---------------------------------------------------------------------------


def _build_container_def(
    service: str,
    ecr_uri: str,
    image_tag: str,
    log_group: str,
    region: str,
    environment: list[dict[str, str]],
    secrets: list[dict[str, str]],
    port: int | None = None,
    command: list[str] | None = None,
) -> dict[str, Any]:
    container: dict[str, Any] = {
        "name": service,
        "image": f"{ecr_uri}:{image_tag}",
        "essential": True,
        "environment": environment,
        "secrets": secrets,
        "logConfiguration": {
            "logDriver": "awslogs",
            "options": {
                "awslogs-group": log_group,
                "awslogs-region": region,
                "awslogs-stream-prefix": "ecs",
            },
        },
    }
    if port:
        container["portMappings"] = [{"containerPort": port, "protocol": "tcp"}]
    if command:
        container["command"] = command
    return container


def _env(name: str, value: str) -> dict[str, str]:
    return {"name": name, "value": value}


def _secret(name: str, arn: str) -> dict[str, str]:
    return {"name": name, "valueFrom": arn}


def register_task_definitions(
    session: boto3.Session,
    settings: InfraSettings,
    ecr_uris: dict[str, str],
    execution_role_arn: str,
    task_role_arn: str,
    log_groups: dict[str, str],
    secret_arns: dict[str, str],
    bootstrap_servers: str,
    s3_bucket: str,
) -> dict[str, str]:
    ecs = session.client("ecs")
    prefix = settings.resource_prefix
    region = settings.aws_region
    tag = settings.ecr_image_tag
    task_arns: dict[str, str] = {}

    service_defs: dict[str, dict[str, Any]] = {
        "producer": {
            "cpu": str(settings.ecs_cpu),
            "memory": str(settings.ecs_memory),
            "container": _build_container_def(
                "producer",
                ecr_uris["producer"],
                tag,
                log_groups["producer"],
                region,
                environment=[
                    _env("KAFKA_BOOTSTRAP_SERVERS", bootstrap_servers),
                    _env("PRODUCER_TYPE", "all"),
                    _env("FAKE_PRODUCER_INTERVAL", "3"),
                    _env("WATCHLIST_TICKERS", "AAPL,MSFT,GOOGL,AMZN,TSLA"),
                    _env("ALPHA_VANTAGE_POLL_INTERVAL", "300"),
                    _env("SEC_EDGAR_POLL_INTERVAL", "600"),
                    _env("SEC_EDGAR_USER_AGENT", "FinancialDataPipeline/1.0 (manaydivatia@gmail.com)"),
                ],
                secrets=[_secret("ALPHA_VANTAGE_API_KEY", secret_arns["alpha-vantage-api-key"])],
            ),
        },
        "processor": {
            "cpu": str(settings.processor_cpu),
            "memory": str(settings.processor_memory),
            "container": _build_container_def(
                "processor",
                ecr_uris["processor"],
                tag,
                log_groups["processor"],
                region,
                environment=[
                    _env("KAFKA_BOOTSTRAP_SERVERS", bootstrap_servers),
                    _env("PROCESSOR_EMIT_INTERVAL", "300"),
                    _env("PROCESSOR_WINDOW_SECONDS", "3600"),
                ],
                secrets=[],
                command=["faust", "-A", "src.processor", "worker", "-l", "info"],
            ),
        },
        "consumer": {
            "cpu": str(settings.ecs_cpu),
            "memory": str(settings.ecs_memory),
            "container": _build_container_def(
                "consumer",
                ecr_uris["consumer"],
                tag,
                log_groups["consumer"],
                region,
                environment=[
                    _env("KAFKA_BOOTSTRAP_SERVERS", bootstrap_servers),
                    _env("CONSUMER_TYPE", "db_sink"),
                    _env("S3_BUCKET", s3_bucket),
                    _env("AWS_REGION", region),
                ],
                secrets=[_secret("DATABASE_URL", secret_arns["database-url"])],
            ),
        },
        "llm-analyzer": {
            "cpu": str(settings.ecs_cpu),
            "memory": str(settings.ecs_memory),
            "container": _build_container_def(
                "llm-analyzer",
                ecr_uris["llm-analyzer"],
                tag,
                log_groups["llm-analyzer"],
                region,
                environment=[
                    _env("KAFKA_BOOTSTRAP_SERVERS", bootstrap_servers),
                    _env("LLM_PROVIDER", "bedrock"),
                    _env("LLM_MODEL_ID", "us.anthropic.claude-3-5-haiku-20241022-v1:0"),
                    _env("LLM_REQUESTS_PER_MINUTE", "50"),
                    _env("LLM_MAX_RETRIES", "3"),
                    _env("LLM_CONTENT_MAX_CHARS", "3000"),
                    _env("AWS_REGION", region),
                ],
                secrets=[],
            ),
        },
        "api": {
            "cpu": str(settings.ecs_cpu),
            "memory": str(settings.ecs_memory),
            "container": _build_container_def(
                "api",
                ecr_uris["api"],
                tag,
                log_groups["api"],
                region,
                environment=[],
                secrets=[_secret("DATABASE_URL", secret_arns["database-url"])],
                port=8000,
            ),
        },
    }

    for service, definition in service_defs.items():
        family = f"{prefix}-{service}"
        resp = ecs.register_task_definition(
            family=family,
            networkMode="awsvpc",
            requiresCompatibilities=["FARGATE"],
            cpu=definition["cpu"],
            memory=definition["memory"],
            executionRoleArn=execution_role_arn,
            taskRoleArn=task_role_arn,
            containerDefinitions=[definition["container"]],
            tags=tag_list_ecs(settings.default_tags),
        )
        task_arn = resp["taskDefinition"]["taskDefinitionArn"]
        logger.info("task_definition_registered", family=family, arn=task_arn)
        task_arns[service] = task_arn

    return task_arns


# ---------------------------------------------------------------------------
# ECS Services
# ---------------------------------------------------------------------------


def create_ecs_services(
    session: boto3.Session,
    settings: InfraSettings,
    cluster_arn: str,
    task_def_arns: dict[str, str],
    private_subnet_ids: list[str],
    sg_ecs_id: str,
    target_group_arn: str,
) -> dict[str, str]:
    ecs = session.client("ecs")
    prefix = settings.resource_prefix
    service_arns: dict[str, str] = {}

    for service in settings.services:
        service_name = f"{prefix}-{service}"
        task_arn = task_def_arns[service]

        network_config: dict[str, Any] = {
            "awsvpcConfiguration": {
                "subnets": private_subnet_ids,
                "securityGroups": [sg_ecs_id],
                "assignPublicIp": "DISABLED",
            }
        }

        # Check if service already exists
        resp = ecs.describe_services(cluster=cluster_arn, services=[service_name])
        active = [s for s in resp["services"] if s["status"] == "ACTIVE"]

        if active:
            ecs.update_service(
                cluster=cluster_arn,
                service=service_name,
                taskDefinition=task_arn,
                desiredCount=settings.desired_count,
            )
            arn = active[0]["serviceArn"]
            logger.info("ecs_service_updated", service=service_name)
        else:
            create_kwargs: dict[str, Any] = {
                "cluster": cluster_arn,
                "serviceName": service_name,
                "taskDefinition": task_arn,
                "desiredCount": settings.desired_count,
                "launchType": "FARGATE",
                "networkConfiguration": network_config,
                "enableExecuteCommand": True,
                "tags": tag_list_ecs(settings.default_tags),
                "deploymentConfiguration": {
                    "minimumHealthyPercent": 0,
                    "maximumPercent": 200,
                },
            }

            if service == "api":
                create_kwargs["loadBalancers"] = [
                    {
                        "targetGroupArn": target_group_arn,
                        "containerName": "api",
                        "containerPort": 8000,
                    }
                ]
                create_kwargs["healthCheckGracePeriodSeconds"] = 60

            resp = ecs.create_service(**create_kwargs)
            arn = resp["service"]["serviceArn"]
            logger.info("ecs_service_created", service=service_name)

        service_arns[service] = arn

    return service_arns


# ---------------------------------------------------------------------------
# Destroy
# ---------------------------------------------------------------------------


def destroy_ecs_services(session: boto3.Session, settings: InfraSettings, cluster_arn: str) -> None:
    ecs = session.client("ecs")
    prefix = settings.resource_prefix

    for service in settings.services:
        service_name = f"{prefix}-{service}"
        try:
            ecs.update_service(cluster=cluster_arn, service=service_name, desiredCount=0)
            ecs.delete_service(cluster=cluster_arn, service=service_name, force=True)
            logger.info("ecs_service_deleted", service=service_name)
        except ClientError as e:
            if "ServiceNotFoundException" in str(e):
                logger.info("ecs_service_not_found", service=service_name)
            else:
                raise


def destroy_ecs_cluster(session: boto3.Session, settings: InfraSettings) -> None:
    ecs = session.client("ecs")
    cluster_name = f"{settings.resource_prefix}-cluster"
    try:
        ecs.delete_cluster(cluster=cluster_name)
        logger.info("ecs_cluster_deleted", name=cluster_name)
    except ClientError as e:
        if "ClusterNotFoundException" in str(e):
            logger.info("ecs_cluster_not_found", name=cluster_name)
        else:
            raise


def destroy_iam_roles(session: boto3.Session, settings: InfraSettings) -> None:
    iam = session.client("iam")
    prefix = settings.resource_prefix

    for role_name, managed_policies in [
        (
            f"{prefix}-ecs-execution",
            ["arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"],
        ),
        (f"{prefix}-ecs-task", []),
    ]:
        try:
            # Remove inline policies
            inline = iam.list_role_policies(RoleName=role_name)
            for policy_name in inline["PolicyNames"]:
                iam.delete_role_policy(RoleName=role_name, PolicyName=policy_name)
            # Detach managed policies
            for policy_arn in managed_policies:
                iam.detach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
            iam.delete_role(RoleName=role_name)
            logger.info("iam_role_deleted", role=role_name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchEntity":
                logger.info("iam_role_not_found", role=role_name)
            else:
                raise


def destroy_log_groups(session: boto3.Session, settings: InfraSettings) -> None:
    logs = session.client("logs")
    prefix = settings.resource_prefix

    for service in settings.services:
        group_name = f"/ecs/{prefix}/{service}"
        try:
            logs.delete_log_group(logGroupName=group_name)
            logger.info("log_group_deleted", name=group_name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                logger.info("log_group_not_found", name=group_name)
            else:
                raise
