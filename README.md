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

Use DynamoDB Local no desenvolvimento/CI e `k8s/secret.example.yaml` somente como modelo. Pull requests executam qualidade e build Docker local, sem publicar ou acessar a AWS. O workflow publica a imagem por SHA e resolve seu digest, valida DynamoDB e SQS, atualiza configuração e Secret e faz o rollout somente quando iniciado manualmente por `workflow_dispatch` no environment `sandbox`.

## Configuração do sandbox

GitHub Variables:

```text
AWS_REGION
EKS_CLUSTER_NAME
K8S_DEPLOYMENT
DYNAMODB_TABLE
```

Para a infraestrutura padrão, use `K8S_DEPLOYMENT=execution-service` e `DYNAMODB_TABLE=sandbox-execution-service`. O deploy também verifica a fila `sandbox-execution-service-events` antes de alterar o cluster.

GitHub Secrets:

```text
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_SESSION_TOKEN
NEW_RELIC_LICENSE_KEY
```

`scripts/sync-k8s-secrets.sh` atualiza `execution-service-secrets` sem gravar o manifesto renderizado. Isso renova no pod as credenciais temporárias necessárias para DynamoDB e SQS. Nunca habilite `set -x` nesse fluxo.
