from types import SimpleNamespace


def test_runtime_factory_uses_environment_for_local_dynamodb(monkeypatch):
    monkeypatch.setenv("DYNAMODB_ENDPOINT_URL", "http://dynamodb-local:8000")
    monkeypatch.setenv("DYNAMODB_TABLE", "execution-bdd")
    monkeypatch.setenv("AWS_REGION", "us-east-2")
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    calls = []

    def fake_client(service, **options):
        calls.append((service, options))
        return SimpleNamespace()

    monkeypatch.setattr("boto3.client", fake_client)
    from src.api.runtime import create_runtime_app

    app = create_runtime_app()

    assert calls == [
        (
            "dynamodb",
            {
                "region_name": "us-east-2",
                "endpoint_url": "http://dynamodb-local:8000",
                "aws_access_key_id": "local",
                "aws_secret_access_key": "local",
            },
        )
    ]
    assert app.state.repository._table_name == "execution-bdd"
