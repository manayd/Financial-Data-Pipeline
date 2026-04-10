from __future__ import annotations

import time
from typing import Any

import boto3
import structlog
from botocore.exceptions import ClientError

from infra.config import InfraSettings

logger = structlog.get_logger(__name__)


def get_boto3_session(settings: InfraSettings) -> boto3.Session:
    return boto3.Session(region_name=settings.aws_region)


def tag_list(tags: dict[str, str]) -> list[dict[str, str]]:
    return [{"Key": k, "Value": v} for k, v in tags.items()]


def tag_list_ecs(tags: dict[str, str]) -> list[dict[str, str]]:
    """ECS APIs require lowercase key/value in tag objects."""
    return [{"key": k, "value": v} for k, v in tags.items()]


def tag_dict(tags: list[dict[str, str]]) -> dict[str, str]:
    return {t["Key"]: t["Value"] for t in tags}


def resource_exists(
    check_fn: Any,
    not_found_codes: tuple[str, ...] = ("NotFoundException",),
) -> Any | None:
    try:
        return check_fn()
    except ClientError as e:
        if e.response["Error"]["Code"] in not_found_codes:
            return None
        raise


def wait_with_log(
    client: Any,
    waiter_name: str,
    log_message: str = "Waiting for resource...",
    delay: int = 15,
    max_attempts: int = 60,
    **kwargs: Any,
) -> None:
    logger.info("waiter_started", waiter=waiter_name, message=log_message)
    waiter = client.get_waiter(waiter_name)
    waiter.wait(WaiterConfig={"Delay": delay, "MaxAttempts": max_attempts}, **kwargs)
    logger.info("waiter_completed", waiter=waiter_name)


def poll_until_status(
    describe_fn: Any,
    status_path: list[str],
    target_status: str,
    poll_interval: int = 30,
    max_wait: int = 1800,
    resource_name: str = "resource",
) -> dict[str, Any]:
    elapsed = 0
    while elapsed < max_wait:
        response = describe_fn()
        current = response
        for key in status_path:
            current = current[key]
        logger.info(
            "polling_status",
            resource=resource_name,
            status=current,
            elapsed_seconds=elapsed,
        )
        if current == target_status:
            return response
        if current in ("FAILED", "DELETING", "DELETE_IN_PROGRESS"):
            msg = f"{resource_name} entered terminal state: {current}"
            raise RuntimeError(msg)
        time.sleep(poll_interval)
        elapsed += poll_interval
    msg = f"Timed out waiting for {resource_name} to reach {target_status} after {max_wait}s"
    raise TimeoutError(msg)
