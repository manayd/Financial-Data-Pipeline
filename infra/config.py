from pydantic_settings import BaseSettings, SettingsConfigDict


class InfraSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="INFRA_", env_file=".env.infra", env_file_encoding="utf-8", extra="ignore"
    )

    project_name: str = "financial-data-pipeline"
    environment: str = "dev"
    aws_region: str = "us-east-1"

    # Networking
    vpc_cidr: str = "10.0.0.0/16"
    public_subnet_cidrs: list[str] = ["10.0.1.0/24", "10.0.2.0/24"]
    private_subnet_cidrs: list[str] = ["10.0.10.0/24", "10.0.11.0/24"]

    # Database
    db_instance_class: str = "db.t3.micro"
    db_name: str = "financial_data"
    db_username: str = "pipeline"
    db_allocated_storage: int = 20

    # MSK
    msk_instance_type: str = "kafka.t3.small"
    msk_broker_count: int = 2
    msk_volume_size: int = 20  # GB per broker

    # ECS
    ecs_cpu: int = 256
    ecs_memory: int = 512
    processor_cpu: int = 512
    processor_memory: int = 1024
    desired_count: int = 1
    ecr_image_tag: str = "latest"

    # Services
    services: list[str] = ["producer", "processor", "consumer", "llm-analyzer", "api"]

    @property
    def resource_prefix(self) -> str:
        return f"{self.project_name}-{self.environment}"

    @property
    def default_tags(self) -> dict[str, str]:
        return {
            "Project": self.project_name,
            "Environment": self.environment,
            "ManagedBy": "boto3",
        }
