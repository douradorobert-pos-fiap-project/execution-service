# Execution Service

Microsserviço proprietário de diagnóstico, fila, reparo e conclusão. Usa DynamoDB com atualizações condicionais e Inbox/Outbox transacionais; testes locais usam DynamoDB Local.

## Executar e validar

```bash
poetry install
poetry run uvicorn src.api.runtime:create_runtime_app --factory --reload --port 8003
poetry run pytest --cov=src --cov-report=term-missing --cov-fail-under=80
bash scripts/quality.sh
```

A aplicação é criada com repository DynamoDB pela factory/runtime. Swagger: `http://localhost:8003/docs`; OpenAPI: `/openapi.json`; health: `/health`. As rotas `/api/v1/executions` exigem `X-Event-ID` nos comandos e propagam `X-Correlation-ID`.

Use DynamoDB Local no desenvolvimento/CI e `k8s/secret.example.yaml` somente como modelo. O pipeline independente bloqueia imagem/deploy quando qualquer gate falha e usa tag imutável do SHA.
