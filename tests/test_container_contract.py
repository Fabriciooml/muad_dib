from pathlib import Path

import yaml


def test_portainer_stack_runs_migration_before_single_tracker():
    stack = yaml.safe_load(Path("portainer-stack.yaml").read_text())
    services = stack["services"]

    assert set(services) == {"migrate", "tracker"}
    assert services["migrate"]["image"] == services["tracker"]["image"]
    assert services["migrate"]["command"] == ["alembic", "upgrade", "head"]
    assert services["tracker"]["depends_on"]["migrate"]["condition"] == (
        "service_completed_successfully"
    )
    assert services["tracker"]["command"][-2:] == ["--workers", "1"]
    assert "DATABASE_URL" in services["migrate"]["environment"]
    assert "KAFKA_BOOTSTRAP_SERVERS" in services["tracker"]["environment"]
    assert "ADMIN_API_KEY" in services["tracker"]["environment"]
