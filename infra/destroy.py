"""Teardown script: removes all AWS infrastructure in reverse dependency order."""

from __future__ import annotations

import sys

import structlog

from infra.alb import destroy_alb
from infra.config import InfraSettings
from infra.database import destroy_database
from infra.ecr import destroy_ecr_repositories
from infra.ecs import (
    destroy_ecs_cluster,
    destroy_ecs_services,
    destroy_iam_roles,
    destroy_log_groups,
)
from infra.helpers import get_boto3_session
from infra.kafka import destroy_msk_cluster
from infra.networking import destroy_networking
from infra.secrets import destroy_secrets
from infra.storage import destroy_s3_bucket

logger = structlog.get_logger(__name__)


def _discover_resource_ids(session, settings):  # type: ignore[no-untyped-def]
    """Discover existing resource IDs by name/tag for teardown."""
    ec2 = session.client("ec2")
    ecs = session.client("ecs")
    prefix = settings.resource_prefix

    ids: dict = {}

    # VPC
    resp = ec2.describe_vpcs(Filters=[{"Name": "tag:Name", "Values": [f"{prefix}-vpc"]}])
    vpcs = [v for v in resp["Vpcs"] if v["State"] == "available"]
    if vpcs:
        vpc_id = vpcs[0]["VpcId"]
        ids["vpc_id"] = vpc_id

        # Security groups
        for sg_name, sg_key in [
            (f"{prefix}-alb-sg", "sg_alb_id"),
            (f"{prefix}-ecs-sg", "sg_ecs_id"),
            (f"{prefix}-rds-sg", "sg_db_id"),
            (f"{prefix}-msk-sg", "sg_msk_id"),
        ]:
            resp = ec2.describe_security_groups(
                Filters=[
                    {"Name": "vpc-id", "Values": [vpc_id]},
                    {"Name": "group-name", "Values": [sg_name]},
                ]
            )
            if resp["SecurityGroups"]:
                ids[sg_key] = resp["SecurityGroups"][0]["GroupId"]

        # Subnets
        subnet_types = [
            ("public", "public_subnet_ids"),
            ("private", "private_subnet_ids"),
        ]
        for subnet_type, key in subnet_types:
            resp = ec2.describe_subnets(
                Filters=[
                    {"Name": "vpc-id", "Values": [vpc_id]},
                    {"Name": "tag:Name", "Values": [f"{prefix}-{subnet_type}-*"]},
                ]
            )
            ids[key] = [s["SubnetId"] for s in resp["Subnets"]]

        # IGW
        resp = ec2.describe_internet_gateways(
            Filters=[{"Name": "attachment.vpc-id", "Values": [vpc_id]}]
        )
        if resp["InternetGateways"]:
            ids["igw_id"] = resp["InternetGateways"][0]["InternetGatewayId"]

        # NAT
        resp = ec2.describe_nat_gateways(
            Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "state", "Values": ["available"]},
            ]
        )
        if resp["NatGateways"]:
            nat = resp["NatGateways"][0]
            ids["nat_gw_id"] = nat["NatGatewayId"]
            if nat["NatGatewayAddresses"]:
                ids["eip_alloc_id"] = nat["NatGatewayAddresses"][0]["AllocationId"]

        # Route tables
        for rt_name, rt_key in [
            (f"{prefix}-public-rt", "public_rt_id"),
            (f"{prefix}-private-rt", "private_rt_id"),
        ]:
            resp = ec2.describe_route_tables(Filters=[{"Name": "tag:Name", "Values": [rt_name]}])
            if resp["RouteTables"]:
                ids[rt_key] = resp["RouteTables"][0]["RouteTableId"]

    # ECS cluster ARN
    resp = ecs.describe_clusters(clusters=[f"{prefix}-cluster"])
    active = [c for c in resp["clusters"] if c["status"] == "ACTIVE"]
    if active:
        ids["cluster_arn"] = active[0]["clusterArn"]

    return ids


def destroy(settings: InfraSettings | None = None, confirm: bool = False) -> None:
    if settings is None:
        settings = InfraSettings()

    if not confirm:
        logger.error("destroy_requires_confirm", hint="Pass --confirm to proceed")
        sys.exit(1)

    session = get_boto3_session(settings)
    prefix = settings.resource_prefix
    logger.info("destroy_start", prefix=prefix)

    ids = _discover_resource_ids(session, settings)

    # Phase 1: ECS
    cluster_arn = ids.get("cluster_arn")
    if cluster_arn:
        destroy_ecs_services(session, settings, cluster_arn)
        destroy_ecs_cluster(session, settings)
    destroy_iam_roles(session, settings)
    destroy_log_groups(session, settings)

    # Phase 2: ALB, MSK, RDS
    destroy_alb(session, settings)
    destroy_msk_cluster(session, settings)
    destroy_database(session, settings)

    # Phase 3: Independent resources
    destroy_secrets(session, settings)
    destroy_s3_bucket(session, settings)
    destroy_ecr_repositories(session, settings)

    # Phase 4: Networking (last)
    if ids.get("vpc_id"):
        destroy_networking(session, settings, ids)

    logger.info("destroy_complete", prefix=prefix)


if __name__ == "__main__":
    destroy(confirm="--confirm" in sys.argv)
