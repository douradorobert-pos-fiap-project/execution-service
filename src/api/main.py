from fastapi import FastAPI
from fastapi.responses import JSONResponse

from src.api.routes import executions
from src.domain.execution import InvalidExecutionTransition
from src.infrastructure.database.dynamodb_repository import (
    DynamoDBExecutionRepository,
)


def create_app(repository: DynamoDBExecutionRepository) -> FastAPI:
    app = FastAPI(title="Execution Service", version="0.1.0")
    app.state.repository = repository
    app.include_router(executions.router)

    @app.exception_handler(InvalidExecutionTransition)
    async def invalid_transition_handler(request, exc):
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    @app.get("/health", tags=["health"])
    def health() -> dict[str, str]:
        return {"status": "ok"}

    return app
