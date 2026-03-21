from __future__ import annotations

from typing import Any

import boto3
import structlog

from infra.config import InfraSettings
from infra.helpers import tag_list, wait_with_log

logger = structlog.get_logger(__name__)


def _find_vpc(ec2: Any, name: str) -> str | None:
    resp = ec2.describe_vpcs(Filters=[{"Name": "tag:Name", "Values": [name]}])
    vpcs = [v for v in resp["Vpcs"] if v["State"] == "available"]
    return vpcs[0]["VpcId"] if vpcs else None


def _find_subnet(ec2: Any, name: str) -> str | None:
    resp = ec2.describe_subnets(Filters=[{"Name": "tag:Name", "Values": [name]}])
    return resp["Subnets"][0]["SubnetId"] if resp["Subnets"] else None


def _find_igw(ec2: Any, vpc_id: str) -> str | None:
    resp = ec2.describe_internet_gateways(
        Filters=[{"Name": "attachment.vpc-id", "Values": [vpc_id]}]
    )
    return resp["InternetGateways"][0]["InternetGatewayId"] if resp["InternetGateways"] else None


def _find_sg(ec2: Any, vpc_id: str, name: str) -> str | None:
    resp = ec2.describe_security_groups(
        Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "group-name", "Values": [name]},
        ]
    )
    return resp["SecurityGroups"][0]["GroupId"] if resp["SecurityGroups"] else None


def _get_availability_zones(session: boto3.Session, region: str) -> list[str]:
    ec2 = session.client("ec2", region_name=region)
    resp = ec2.describe_availability_zones(Filters=[{"Name": "state", "Values": ["available"]}])
    zones = sorted(z["ZoneName"] for z in resp["AvailabilityZones"])
    return zones[:2]


def create_networking(session: boto3.Session, settings: InfraSettings) -> dict[str, Any]:
    ec2 = session.client("ec2")
    prefix = settings.resource_prefix
    tags = settings.default_tags
    azs = _get_availability_zones(session, settings.aws_region)

    # --- VPC ---
    vpc_name = f"{prefix}-vpc"
    vpc_id = _find_vpc(ec2, vpc_name)
    if vpc_id:
        logger.info("vpc_exists", vpc_id=vpc_id)
    else:
        resp = ec2.create_vpc(
            CidrBlock=settings.vpc_cidr,
            TagSpecifications=[
                {
                    "ResourceType": "vpc",
                    "Tags": tag_list({**tags, "Name": vpc_name}),
                }
            ],
        )
        vpc_id = resp["Vpc"]["VpcId"]
        ec2.modify_vpc_attribute(VpcId=vpc_id, EnableDnsSupport={"Value": True})
        ec2.modify_vpc_attribute(VpcId=vpc_id, EnableDnsHostnames={"Value": True})
        logger.info("vpc_created", vpc_id=vpc_id)

    # --- Internet Gateway ---
    igw_id = _find_igw(ec2, vpc_id)
    if igw_id:
        logger.info("igw_exists", igw_id=igw_id)
    else:
        resp = ec2.create_internet_gateway(
            TagSpecifications=[
                {
                    "ResourceType": "internet-gateway",
                    "Tags": tag_list({**tags, "Name": f"{prefix}-igw"}),
                }
            ]
        )
        igw_id = resp["InternetGateway"]["InternetGatewayId"]
        ec2.attach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
        logger.info("igw_created", igw_id=igw_id)

    # --- Subnets ---
    public_subnet_ids: list[str] = []
    private_subnet_ids: list[str] = []

    for i, cidr in enumerate(settings.public_subnet_cidrs):
        name = f"{prefix}-public-{azs[i]}"
        sid = _find_subnet(ec2, name)
        if sid:
            logger.info("subnet_exists", name=name, subnet_id=sid)
        else:
            resp = ec2.create_subnet(
                VpcId=vpc_id,
                CidrBlock=cidr,
                AvailabilityZone=azs[i],
                TagSpecifications=[
                    {
                        "ResourceType": "subnet",
                        "Tags": tag_list({**tags, "Name": name}),
                    }
                ],
            )
            sid = resp["Subnet"]["SubnetId"]
            ec2.modify_subnet_attribute(SubnetId=sid, MapPublicIpOnLaunch={"Value": True})
            logger.info("subnet_created", name=name, subnet_id=sid)
        public_subnet_ids.append(sid)

    for i, cidr in enumerate(settings.private_subnet_cidrs):
        name = f"{prefix}-private-{azs[i]}"
        sid = _find_subnet(ec2, name)
        if sid:
            logger.info("subnet_exists", name=name, subnet_id=sid)
        else:
            resp = ec2.create_subnet(
                VpcId=vpc_id,
                CidrBlock=cidr,
                AvailabilityZone=azs[i],
                TagSpecifications=[
                    {
                        "ResourceType": "subnet",
                        "Tags": tag_list({**tags, "Name": name}),
                    }
                ],
            )
            sid = resp["Subnet"]["SubnetId"]
            logger.info("subnet_created", name=name, subnet_id=sid)
        private_subnet_ids.append(sid)

    # --- NAT Gateway ---
    nat_resp = ec2.describe_nat_gateways(
        Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "state", "Values": ["available", "pending"]},
        ]
    )
    if nat_resp["NatGateways"]:
        nat_gw_id = nat_resp["NatGateways"][0]["NatGatewayId"]
        eip_alloc_id = nat_resp["NatGateways"][0]["NatGatewayAddresses"][0]["AllocationId"]
        logger.info("nat_gateway_exists", nat_gw_id=nat_gw_id)
    else:
        eip_resp = ec2.allocate_address(
            Domain="vpc",
            TagSpecifications=[
                {
                    "ResourceType": "elastic-ip",
                    "Tags": tag_list({**tags, "Name": f"{prefix}-nat-eip"}),
                }
            ],
        )
        eip_alloc_id = eip_resp["AllocationId"]
        resp = ec2.create_nat_gateway(
            SubnetId=public_subnet_ids[0],
            AllocationId=eip_alloc_id,
            TagSpecifications=[
                {
                    "ResourceType": "natgateway",
                    "Tags": tag_list({**tags, "Name": f"{prefix}-nat"}),
                }
            ],
        )
        nat_gw_id = resp["NatGateway"]["NatGatewayId"]
        wait_with_log(
            ec2,
            "nat_gateway_available",
            "Waiting for NAT gateway...",
            NatGatewayIds=[nat_gw_id],
        )
        logger.info("nat_gateway_created", nat_gw_id=nat_gw_id)

    # --- Route Tables ---
    public_rt_id = _ensure_route_table(
        ec2, vpc_id, f"{prefix}-public-rt", tags, "0.0.0.0/0", GatewayId=igw_id
    )
    for sid in public_subnet_ids:
        _associate_route_table(ec2, public_rt_id, sid)

    private_rt_id = _ensure_route_table(
        ec2, vpc_id, f"{prefix}-private-rt", tags, "0.0.0.0/0", NatGatewayId=nat_gw_id
    )
    for sid in private_subnet_ids:
        _associate_route_table(ec2, private_rt_id, sid)

    # --- Security Groups ---
    sg_alb_id = _ensure_security_group(ec2, vpc_id, f"{prefix}-alb-sg", "ALB security group", tags)
    sg_ecs_id = _ensure_security_group(
        ec2, vpc_id, f"{prefix}-ecs-sg", "ECS tasks security group", tags
    )
    sg_db_id = _ensure_security_group(ec2, vpc_id, f"{prefix}-rds-sg", "RDS security group", tags)
    sg_msk_id = _ensure_security_group(ec2, vpc_id, f"{prefix}-msk-sg", "MSK security group", tags)

    _ensure_ingress(ec2, sg_alb_id, IpProtocol="tcp", FromPort=80, ToPort=80, CidrIp="0.0.0.0/0")
    _ensure_ingress(
        ec2,
        sg_ecs_id,
        IpProtocol="tcp",
        FromPort=8000,
        ToPort=8000,
        SourceSecurityGroupId=sg_alb_id,
    )
    _ensure_ingress(
        ec2, sg_ecs_id, IpProtocol="-1", FromPort=-1, ToPort=-1, SourceSecurityGroupId=sg_ecs_id
    )
    _ensure_ingress(
        ec2, sg_db_id, IpProtocol="tcp", FromPort=5432, ToPort=5432, SourceSecurityGroupId=sg_ecs_id
    )
    _ensure_ingress(
        ec2,
        sg_msk_id,
        IpProtocol="tcp",
        FromPort=9092,
        ToPort=9092,
        SourceSecurityGroupId=sg_ecs_id,
    )
    _ensure_ingress(
        ec2,
        sg_msk_id,
        IpProtocol="tcp",
        FromPort=9094,
        ToPort=9094,
        SourceSecurityGroupId=sg_ecs_id,
    )

    return {
        "vpc_id": vpc_id,
        "igw_id": igw_id,
        "public_subnet_ids": public_subnet_ids,
        "private_subnet_ids": private_subnet_ids,
        "nat_gw_id": nat_gw_id,
        "eip_alloc_id": eip_alloc_id,
        "public_rt_id": public_rt_id,
        "private_rt_id": private_rt_id,
        "sg_alb_id": sg_alb_id,
        "sg_ecs_id": sg_ecs_id,
        "sg_db_id": sg_db_id,
        "sg_msk_id": sg_msk_id,
    }


def destroy_networking(
    session: boto3.Session, settings: InfraSettings, ids: dict[str, Any]
) -> None:
    ec2 = session.client("ec2")
    vpc_id = ids.get("vpc_id")
    if not vpc_id:
        logger.warning("no_vpc_id_for_destroy")
        return

    # Security groups
    for sg_key in ("sg_alb_id", "sg_ecs_id", "sg_db_id", "sg_msk_id"):
        sg_id = ids.get(sg_key)
        if sg_id:
            _delete_sg_safe(ec2, sg_id)

    # NAT gateway
    nat_gw_id = ids.get("nat_gw_id")
    if nat_gw_id:
        ec2.delete_nat_gateway(NatGatewayId=nat_gw_id)
        logger.info("nat_gateway_deleting", nat_gw_id=nat_gw_id)
        ec2.get_waiter("nat_gateway_available")  # no delete waiter; poll manually
        import time

        for _ in range(60):
            resp = ec2.describe_nat_gateways(NatGatewayIds=[nat_gw_id])
            if resp["NatGateways"][0]["State"] == "deleted":
                break
            time.sleep(10)
        logger.info("nat_gateway_deleted", nat_gw_id=nat_gw_id)

    # EIP
    eip_alloc_id = ids.get("eip_alloc_id")
    if eip_alloc_id:
        ec2.release_address(AllocationId=eip_alloc_id)
        logger.info("eip_released", alloc_id=eip_alloc_id)

    # Route table associations and tables
    for rt_key in ("public_rt_id", "private_rt_id"):
        rt_id = ids.get(rt_key)
        if rt_id:
            _delete_route_table_safe(ec2, rt_id)

    # Internet gateway
    igw_id = ids.get("igw_id")
    if igw_id:
        ec2.detach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
        ec2.delete_internet_gateway(InternetGatewayId=igw_id)
        logger.info("igw_deleted", igw_id=igw_id)

    # Subnets
    for subnet_ids_key in ("public_subnet_ids", "private_subnet_ids"):
        for sid in ids.get(subnet_ids_key, []):
            ec2.delete_subnet(SubnetId=sid)
            logger.info("subnet_deleted", subnet_id=sid)

    # VPC
    ec2.delete_vpc(VpcId=vpc_id)
    logger.info("vpc_deleted", vpc_id=vpc_id)


# --- Private helpers ---


def _ensure_route_table(
    ec2: Any, vpc_id: str, name: str, tags: dict[str, str], dest_cidr: str, **route_target: Any
) -> str:
    resp = ec2.describe_route_tables(Filters=[{"Name": "tag:Name", "Values": [name]}])
    if resp["RouteTables"]:
        rt_id = resp["RouteTables"][0]["RouteTableId"]
        logger.info("route_table_exists", name=name, rt_id=rt_id)
        return rt_id

    resp = ec2.create_route_table(
        VpcId=vpc_id,
        TagSpecifications=[
            {
                "ResourceType": "route-table",
                "Tags": tag_list({**tags, "Name": name}),
            }
        ],
    )
    rt_id = resp["RouteTable"]["RouteTableId"]
    ec2.create_route(RouteTableId=rt_id, DestinationCidrBlock=dest_cidr, **route_target)
    logger.info("route_table_created", name=name, rt_id=rt_id)
    return rt_id


def _associate_route_table(ec2: Any, rt_id: str, subnet_id: str) -> None:
    resp = ec2.describe_route_tables(RouteTableIds=[rt_id])
    assocs = resp["RouteTables"][0].get("Associations", [])
    existing = {a["SubnetId"] for a in assocs if "SubnetId" in a}
    if subnet_id not in existing:
        ec2.associate_route_table(RouteTableId=rt_id, SubnetId=subnet_id)


def _ensure_security_group(
    ec2: Any, vpc_id: str, name: str, description: str, tags: dict[str, str]
) -> str:
    sg_id = _find_sg(ec2, vpc_id, name)
    if sg_id:
        logger.info("security_group_exists", name=name, sg_id=sg_id)
        return sg_id
    resp = ec2.create_security_group(
        GroupName=name,
        Description=description,
        VpcId=vpc_id,
        TagSpecifications=[
            {
                "ResourceType": "security-group",
                "Tags": tag_list({**tags, "Name": name}),
            }
        ],
    )
    sg_id = resp["GroupId"]
    logger.info("security_group_created", name=name, sg_id=sg_id)
    return sg_id


def _ensure_ingress(ec2: Any, sg_id: str, **kwargs: Any) -> None:
    ip_perm: dict[str, Any] = {
        "IpProtocol": kwargs["IpProtocol"],
        "FromPort": kwargs["FromPort"],
        "ToPort": kwargs["ToPort"],
    }
    if "CidrIp" in kwargs:
        ip_perm["IpRanges"] = [{"CidrIp": kwargs["CidrIp"]}]
    elif "SourceSecurityGroupId" in kwargs:
        ip_perm["UserIdGroupPairs"] = [{"GroupId": kwargs["SourceSecurityGroupId"]}]

    try:
        ec2.authorize_security_group_ingress(GroupId=sg_id, IpPermissions=[ip_perm])
    except ec2.exceptions.ClientError as e:
        if "InvalidPermission.Duplicate" in str(e):
            return
        raise


def _delete_sg_safe(ec2: Any, sg_id: str) -> None:
    try:
        # Revoke all ingress rules first
        resp = ec2.describe_security_groups(GroupIds=[sg_id])
        if resp["SecurityGroups"] and resp["SecurityGroups"][0]["IpPermissions"]:
            ec2.revoke_security_group_ingress(
                GroupId=sg_id, IpPermissions=resp["SecurityGroups"][0]["IpPermissions"]
            )
    except Exception:
        pass
    try:
        ec2.delete_security_group(GroupId=sg_id)
        logger.info("security_group_deleted", sg_id=sg_id)
    except Exception as e:
        logger.warning("security_group_delete_failed", sg_id=sg_id, error=str(e))


def _delete_route_table_safe(ec2: Any, rt_id: str) -> None:
    try:
        resp = ec2.describe_route_tables(RouteTableIds=[rt_id])
        for assoc in resp["RouteTables"][0].get("Associations", []):
            if not assoc.get("Main", False):
                ec2.disassociate_route_table(AssociationId=assoc["RouteTableAssociationId"])
        ec2.delete_route_table(RouteTableId=rt_id)
        logger.info("route_table_deleted", rt_id=rt_id)
    except Exception as e:
        logger.warning("route_table_delete_failed", rt_id=rt_id, error=str(e))
