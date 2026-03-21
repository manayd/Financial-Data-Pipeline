from unittest.mock import MagicMock, patch

from infra.config import InfraSettings
from infra.networking import create_networking


@patch("infra.networking._get_availability_zones", return_value=["us-east-1a", "us-east-1b"])
def test_create_networking_idempotent(mock_azs):
    """When all resources exist, create_networking returns their IDs without creating new ones."""
    session = MagicMock()
    ec2 = MagicMock()
    session.client.return_value = ec2
    settings = InfraSettings()

    # VPC exists
    ec2.describe_vpcs.return_value = {"Vpcs": [{"VpcId": "vpc-123", "State": "available"}]}
    # IGW exists
    ec2.describe_internet_gateways.return_value = {
        "InternetGateways": [{"InternetGatewayId": "igw-123"}]
    }
    # Subnets exist
    ec2.describe_subnets.return_value = {"Subnets": [{"SubnetId": "subnet-pub"}]}
    # NAT exists
    ec2.describe_nat_gateways.return_value = {
        "NatGateways": [
            {
                "NatGatewayId": "nat-123",
                "State": "available",
                "NatGatewayAddresses": [{"AllocationId": "eip-123"}],
            }
        ]
    }
    # Route tables exist
    ec2.describe_route_tables.return_value = {
        "RouteTables": [
            {
                "RouteTableId": "rtb-pub",
                "Associations": [{"SubnetId": "subnet-pub", "Main": False}],
            }
        ]
    }
    # Security groups exist
    ec2.describe_security_groups.return_value = {"SecurityGroups": [{"GroupId": "sg-123"}]}

    result = create_networking(session, settings)

    assert result["vpc_id"] == "vpc-123"
    assert result["igw_id"] == "igw-123"
    assert result["nat_gw_id"] == "nat-123"
    # No create calls made
    ec2.create_vpc.assert_not_called()
    ec2.create_internet_gateway.assert_not_called()
    ec2.create_nat_gateway.assert_not_called()


@patch("infra.networking._get_availability_zones", return_value=["us-east-1a", "us-east-1b"])
def test_create_networking_creates_vpc(mock_azs):
    """When VPC doesn't exist, it creates one."""
    session = MagicMock()
    ec2 = MagicMock()
    session.client.return_value = ec2
    settings = InfraSettings()

    # VPC does not exist
    ec2.describe_vpcs.return_value = {"Vpcs": []}
    ec2.create_vpc.return_value = {"Vpc": {"VpcId": "vpc-new"}}

    # Everything else exists
    ec2.describe_internet_gateways.return_value = {
        "InternetGateways": [{"InternetGatewayId": "igw-123"}]
    }
    ec2.describe_subnets.return_value = {"Subnets": [{"SubnetId": "subnet-1"}]}
    ec2.describe_nat_gateways.return_value = {
        "NatGateways": [
            {
                "NatGatewayId": "nat-1",
                "State": "available",
                "NatGatewayAddresses": [{"AllocationId": "eip-1"}],
            }
        ]
    }
    ec2.describe_route_tables.return_value = {
        "RouteTables": [{"RouteTableId": "rtb-1", "Associations": []}]
    }
    ec2.describe_security_groups.return_value = {"SecurityGroups": [{"GroupId": "sg-1"}]}

    result = create_networking(session, settings)

    ec2.create_vpc.assert_called_once()
    assert result["vpc_id"] == "vpc-new"


@patch("infra.networking._get_availability_zones", return_value=["us-east-1a", "us-east-1b"])
def test_security_group_count(mock_azs):
    """Creates 4 security groups: ALB, ECS, RDS, MSK."""
    session = MagicMock()
    ec2 = MagicMock()
    session.client.return_value = ec2
    settings = InfraSettings()

    ec2.describe_vpcs.return_value = {"Vpcs": [{"VpcId": "vpc-1", "State": "available"}]}
    ec2.describe_internet_gateways.return_value = {
        "InternetGateways": [{"InternetGatewayId": "igw-1"}]
    }
    ec2.describe_subnets.return_value = {"Subnets": [{"SubnetId": "subnet-1"}]}
    ec2.describe_nat_gateways.return_value = {
        "NatGateways": [
            {
                "NatGatewayId": "nat-1",
                "State": "available",
                "NatGatewayAddresses": [{"AllocationId": "eip-1"}],
            }
        ]
    }
    ec2.describe_route_tables.return_value = {
        "RouteTables": [{"RouteTableId": "rtb-1", "Associations": []}]
    }
    # SGs don't exist
    ec2.describe_security_groups.return_value = {"SecurityGroups": []}
    ec2.create_security_group.return_value = {"GroupId": "sg-new"}

    result = create_networking(session, settings)

    assert ec2.create_security_group.call_count == 4
    for key in ("sg_alb_id", "sg_ecs_id", "sg_db_id", "sg_msk_id"):
        assert key in result
