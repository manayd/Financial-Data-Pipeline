"""Main orchestrator: provisions all AWS infrastructure in dependency order."""

from __future__ import annotations

import concurrent.futures
from typing import Any

import structlog

from infra.alb import create_alb
from infra.config import InfraSettings
from infra.database import create_database
from infra.ecr import create_ecr_repositories
from infra.ecs import (
    create_ecs_cluster,
    create_ecs_services,
    create_execution_role,
    create_log_groups,
    create_task_role,
    register_task_definitions,
)
from infra.helpers import get_boto3_session
from infra.kafka import create_msk_cluster, create_msk_topics
from infra.networking import create_networking
from infra.secrets import create_secrets, generate_db_password, update_database_secret
from infra.storage import create_s3_bucket

logger = structlog.get_logger(__name__)


def deploy(settings: InfraSettings | None = None) -> dict[str, Any]:
    if settings is None:
        settings = InfraSettings()

    session = get_boto3_session(settings)
    results: dict[str, Any] = {"settings": settings.resource_prefix}

    # ── Phase 1: Independent resources ──
    logger.info("phase_1_start", description="Independent resources")

    net = create_networking(session, settings)
    results["networking"] = net

    ecr_uris = create_ecr_repositories(session, settings)
    results["ecr_uris"] = ecr_uris

    bucket_name = create_s3_bucket(session, settings)
    results["s3_bucket"] = bucket_name

    secret_arns = create_secrets(session, settings)
    results["secret_arns"] = secret_arns

    db_password = generate_db_password()

    logger.info("phase_1_complete")

    # ── Phase 2: Long-running resources (parallel) ──
    logger.info("phase_2_start", description="Database + Kafka + ALB (parallel)")

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        db_future = executor.submit(
            create_database,
            session,
            settings,
            net["private_subnet_ids"],
            net["sg_db_id"],
            db_password,
        )
        msk_future = executor.submit(
            create_msk_cluster,
            session,
            settings,
            net["private_subnet_ids"],
            net["sg_msk_id"],
        )
        alb_future = executor.submit(
            create_alb,
            session,
            settings,
            net["public_subnet_ids"],
            net["sg_alb_id"],
            net["vpc_id"],
        )

        db_info = db_future.result()
        msk_info = msk_future.result()
        alb_info = alb_future.result()

    results["database"] = db_info
    results["kafka"] = msk_info
    results["alb"] = alb_info

    logger.info("phase_2_complete")

    # ── Phase 3: Post-dependency setup ──
    logger.info("phase_3_start", description="Secrets update, topics, IAM, log groups")

    update_database_secret(session, settings, db_info["endpoint"], db_password)

    try:
        create_msk_topics(msk_info["bootstrap_servers"])
    except Exception:
        logger.warning(
            "msk_topic_creation_skipped",
            reason="MSK may not be reachable from this machine. "
            "Topics will auto-create on first produce.",
        )

    all_secret_arns = list(secret_arns.values())
    execution_role_arn = create_execution_role(session, settings, all_secret_arns)
    results["execution_role_arn"] = execution_role_arn

    s3_bucket_arn = f"arn:aws:s3:::{bucket_name}"
    task_role_arn = create_task_role(session, settings, s3_bucket_arn, msk_info["cluster_arn"])
    results["task_role_arn"] = task_role_arn

    log_groups = create_log_groups(session, settings)
    results["log_groups"] = log_groups

    logger.info("phase_3_complete")

    # ── Phase 4: ECS ──
    logger.info("phase_4_start", description="Task definitions, cluster, services")

    task_def_arns = register_task_definitions(
        session,
        settings,
        ecr_uris,
        execution_role_arn,
        task_role_arn,
        log_groups,
        secret_arns,
        msk_info["bootstrap_servers"],
        bucket_name,
    )
    results["task_definitions"] = task_def_arns

    cluster_arn = create_ecs_cluster(session, settings)
    results["cluster_arn"] = cluster_arn

    service_arns = create_ecs_services(
        session,
        settings,
        cluster_arn,
        task_def_arns,
        net["private_subnet_ids"],
        net["sg_ecs_id"],
        alb_info["target_group_arn"],
    )
    results["services"] = service_arns

    logger.info("phase_4_complete")

    # ── Summary ──
    logger.info(
        "deploy_complete",
        alb_url=f"http://{alb_info['alb_dns_name']}",
        cluster=cluster_arn,
        rds_endpoint=db_info["endpoint"],
        msk_bootstrap=msk_info["bootstrap_servers"],
        s3_bucket=bucket_name,
    )

    return results


if __name__ == "__main__":
    deploy()
