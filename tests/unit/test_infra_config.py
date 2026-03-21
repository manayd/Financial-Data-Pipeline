from infra.config import InfraSettings


def test_defaults():
    settings = InfraSettings()
    assert settings.project_name == "financial-data-pipeline"
    assert settings.environment == "dev"
    assert settings.aws_region == "us-east-1"
    assert settings.db_instance_class == "db.t3.micro"
    assert settings.ecs_cpu == 256
    assert settings.desired_count == 1


def test_resource_prefix():
    settings = InfraSettings()
    assert settings.resource_prefix == "financial-data-pipeline-dev"


def test_resource_prefix_custom():
    settings = InfraSettings(project_name="test-proj", environment="staging")
    assert settings.resource_prefix == "test-proj-staging"


def test_default_tags():
    settings = InfraSettings()
    tags = settings.default_tags
    assert tags["Project"] == "financial-data-pipeline"
    assert tags["Environment"] == "dev"
    assert tags["ManagedBy"] == "boto3"


def test_services_list():
    settings = InfraSettings()
    assert "producer" in settings.services
    assert "api" in settings.services
    assert len(settings.services) == 5


def test_processor_sizing():
    settings = InfraSettings()
    assert settings.processor_cpu == 512
    assert settings.processor_memory == 1024
