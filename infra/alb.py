from __future__ import annotations

from typing import Any

import boto3
import structlog
from botocore.exceptions import ClientError

from infra.config import InfraSettings
from infra.helpers import tag_list, wait_with_log

logger = structlog.get_logger(__name__)


def create_alb(
    session: boto3.Session,
    settings: InfraSettings,
    public_subnet_ids: list[str],
    sg_alb_id: str,
    vpc_id: str,
) -> dict[str, Any]:
    elbv2 = session.client("elbv2")
    prefix = settings.resource_prefix
    alb_name = f"{prefix}-alb"
    tg_name = f"{prefix}-tg"

    # --- Load Balancer ---
    try:
        resp = elbv2.describe_load_balancers(Names=[alb_name])
        alb = resp["LoadBalancers"][0]
        alb_arn = alb["LoadBalancerArn"]
        alb_dns = alb["DNSName"]
        logger.info("alb_exists", name=alb_name, dns=alb_dns)
    except ClientError as e:
        if e.response["Error"]["Code"] != "LoadBalancerNotFound":
            raise
        resp = elbv2.create_load_balancer(
            Name=alb_name,
            Subnets=public_subnet_ids,
            SecurityGroups=[sg_alb_id],
            Scheme="internet-facing",
            Type="application",
            Tags=tag_list(settings.default_tags),
        )
        alb = resp["LoadBalancers"][0]
        alb_arn = alb["LoadBalancerArn"]
        alb_dns = alb["DNSName"]

        wait_with_log(
            elbv2,
            "load_balancer_available",
            "Waiting for ALB to become active...",
            LoadBalancerArns=[alb_arn],
        )
        logger.info("alb_created", name=alb_name, dns=alb_dns)

    # --- Target Group ---
    tg_arn = _find_target_group(elbv2, tg_name)
    if tg_arn:
        logger.info("target_group_exists", name=tg_name, arn=tg_arn)
    else:
        resp = elbv2.create_target_group(
            Name=tg_name,
            Protocol="HTTP",
            Port=8000,
            VpcId=vpc_id,
            TargetType="ip",
            HealthCheckProtocol="HTTP",
            HealthCheckPath="/api/v1/health",
            HealthCheckIntervalSeconds=30,
            HealthyThresholdCount=2,
            UnhealthyThresholdCount=3,
            Tags=tag_list(settings.default_tags),
        )
        tg_arn = resp["TargetGroups"][0]["TargetGroupArn"]
        logger.info("target_group_created", name=tg_name, arn=tg_arn)

    # --- Listener ---
    listeners = elbv2.describe_listeners(LoadBalancerArn=alb_arn)
    if not listeners["Listeners"]:
        elbv2.create_listener(
            LoadBalancerArn=alb_arn,
            Protocol="HTTP",
            Port=80,
            DefaultActions=[{"Type": "forward", "TargetGroupArn": tg_arn}],
        )
        logger.info("listener_created", port=80)
    else:
        logger.info("listener_exists", port=80)

    return {
        "alb_arn": alb_arn,
        "alb_dns_name": alb_dns,
        "target_group_arn": tg_arn,
    }


def destroy_alb(session: boto3.Session, settings: InfraSettings) -> None:
    elbv2 = session.client("elbv2")
    prefix = settings.resource_prefix
    alb_name = f"{prefix}-alb"
    tg_name = f"{prefix}-tg"

    # Delete ALB (listeners are deleted automatically)
    try:
        resp = elbv2.describe_load_balancers(Names=[alb_name])
        alb_arn = resp["LoadBalancers"][0]["LoadBalancerArn"]
        elbv2.delete_load_balancer(LoadBalancerArn=alb_arn)
        logger.info("alb_deleting", name=alb_name)
        wait_with_log(
            elbv2,
            "load_balancers_deleted",
            "Waiting for ALB deletion...",
            LoadBalancerArns=[alb_arn],
        )
        logger.info("alb_deleted", name=alb_name)
    except ClientError as e:
        if e.response["Error"]["Code"] != "LoadBalancerNotFound":
            raise
        logger.info("alb_not_found", name=alb_name)

    # Delete target group
    tg_arn = _find_target_group(elbv2, tg_name)
    if tg_arn:
        elbv2.delete_target_group(TargetGroupArn=tg_arn)
        logger.info("target_group_deleted", name=tg_name)


def _find_target_group(elbv2: Any, name: str) -> str | None:
    try:
        resp = elbv2.describe_target_groups(Names=[name])
        return resp["TargetGroups"][0]["TargetGroupArn"] if resp["TargetGroups"] else None
    except ClientError as e:
        if e.response["Error"]["Code"] == "TargetGroupNotFound":
            return None
        raise
