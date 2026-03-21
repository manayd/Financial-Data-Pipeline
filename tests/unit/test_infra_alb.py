from unittest.mock import MagicMock

from botocore.exceptions import ClientError

from infra.alb import create_alb, destroy_alb
from infra.config import InfraSettings


def test_create_alb_new():
    session = MagicMock()
    elbv2 = MagicMock()
    session.client.return_value = elbv2
    settings = InfraSettings()

    # ALB does not exist
    elbv2.describe_load_balancers.side_effect = ClientError(
        {"Error": {"Code": "LoadBalancerNotFound", "Message": ""}}, "Describe"
    )
    elbv2.create_load_balancer.return_value = {
        "LoadBalancers": [
            {
                "LoadBalancerArn": "arn:alb",
                "DNSName": "test-alb.elb.amazonaws.com",
            }
        ]
    }
    # Target group does not exist
    elbv2.describe_target_groups.side_effect = ClientError(
        {"Error": {"Code": "TargetGroupNotFound", "Message": ""}}, "Describe"
    )
    elbv2.create_target_group.return_value = {"TargetGroups": [{"TargetGroupArn": "arn:tg"}]}
    # No existing listeners
    elbv2.describe_listeners.return_value = {"Listeners": []}

    result = create_alb(session, settings, ["subnet-1", "subnet-2"], "sg-alb", "vpc-1")

    elbv2.create_load_balancer.assert_called_once()
    assert result["alb_dns_name"] == "test-alb.elb.amazonaws.com"
    assert result["target_group_arn"] == "arn:tg"

    # Health check path
    tg_kwargs = elbv2.create_target_group.call_args[1]
    assert tg_kwargs["HealthCheckPath"] == "/api/v1/health"
    assert tg_kwargs["TargetType"] == "ip"
    assert tg_kwargs["Port"] == 8000

    elbv2.create_listener.assert_called_once()


def test_create_alb_idempotent():
    session = MagicMock()
    elbv2 = MagicMock()
    session.client.return_value = elbv2
    settings = InfraSettings()

    elbv2.describe_load_balancers.return_value = {
        "LoadBalancers": [{"LoadBalancerArn": "arn:existing-alb", "DNSName": "existing.elb.com"}]
    }
    elbv2.describe_target_groups.return_value = {
        "TargetGroups": [{"TargetGroupArn": "arn:existing-tg"}]
    }
    elbv2.describe_listeners.return_value = {"Listeners": [{"ListenerArn": "arn:listener"}]}

    result = create_alb(session, settings, ["subnet-1"], "sg-alb", "vpc-1")

    elbv2.create_load_balancer.assert_not_called()
    elbv2.create_target_group.assert_not_called()
    elbv2.create_listener.assert_not_called()
    assert result["alb_dns_name"] == "existing.elb.com"


def test_destroy_alb():
    session = MagicMock()
    elbv2 = MagicMock()
    session.client.return_value = elbv2
    settings = InfraSettings()

    elbv2.describe_load_balancers.return_value = {"LoadBalancers": [{"LoadBalancerArn": "arn:alb"}]}
    elbv2.describe_target_groups.return_value = {"TargetGroups": [{"TargetGroupArn": "arn:tg"}]}

    destroy_alb(session, settings)

    elbv2.delete_load_balancer.assert_called_once()
    elbv2.delete_target_group.assert_called_once()
