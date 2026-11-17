import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "pipeline.yml"
SYNC = ROOT / "scripts" / "sync-k8s-secrets.sh"


def test_workflow_builds_prs_and_deploys_only_by_manual_dispatch() -> None:
    workflow = WORKFLOW.read_text()

    assert "pull_request:" in workflow
    assert "workflow_dispatch:" in workflow
    assert "push:" not in workflow
    assert "github.event_name == 'workflow_dispatch'" in workflow
    assert "environment: sandbox" in workflow
    assert "imageDigest" in workflow
    assert "ALB_SECURITY_GROUP_ID: ${{ vars.ALB_SECURITY_GROUP_ID }}" in workflow
    assert workflow.index("Validate internal ALB configuration") < workflow.index(
        "k8s/deployment.yaml"
    )
    assert "k8s/ingress.yaml" in workflow


def test_manual_deploy_requires_and_validates_execution_resources() -> None:
    workflow = WORKFLOW.read_text()

    assert "DYNAMODB_TABLE: ${{ vars.DYNAMODB_TABLE }}" in workflow
    assert "aws dynamodb describe-table" in workflow
    assert "aws sqs get-queue-url" in workflow
    assert "sandbox-execution-service-events" in workflow
    assert 'DYNAMODB_TABLE: \\"$DYNAMODB_TABLE\\"' in workflow
    assert "k8s/worker-deployment.yaml" in workflow
    assert "rollout status deployment/execution-service-worker" in workflow


def test_secret_sync_refreshes_aws_and_new_relic_without_echoing_values(
    tmp_path: Path,
) -> None:
    log = tmp_path / "kubectl.log"
    kubectl = tmp_path / "kubectl"
    kubectl.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\\0' "$@" >> "${FAKE_KUBECTL_LOG}"
if [[ " $* " == *" create secret "* ]]; then
  printf '%s\\n' 'apiVersion: v1' 'kind: Secret' 'metadata: {name: fake}'
else
  cat >/dev/null
fi
"""
    )
    kubectl.chmod(0o755)
    secrets = {
        "NEW_RELIC_LICENSE_KEY": "new-relic-test-secret",
        "AWS_ACCESS_KEY_ID": "access-test-secret",
        "AWS_SECRET_ACCESS_KEY": "aws-secret-test-secret",
        "AWS_SESSION_TOKEN": "session-test-secret",
    }
    env = {
        **os.environ,
        **secrets,
        "PATH": f"{tmp_path}:{os.environ['PATH']}",
        "FAKE_KUBECTL_LOG": str(log),
    }

    result = subprocess.run(
        ["bash", str(SYNC)], env=env, text=True, capture_output=True, check=False
    )
    arguments = log.read_bytes().split(b"\0") if log.exists() else []

    assert result.returncode == 0
    for name, value in secrets.items():
        assert f"--from-literal={name}={value}".encode() in arguments
        assert value not in result.stdout
        assert value not in result.stderr


def test_secret_sync_rejects_missing_required_value(tmp_path: Path) -> None:
    result = subprocess.run(
        ["bash", str(SYNC)],
        env={"PATH": os.environ["PATH"]},
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 2
    assert "Missing required deployment configuration" in result.stderr
