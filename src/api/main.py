from time import perf_counter
from uuid import UUID, uuid4

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from src.api.routes import executions
from src.domain.execution import InvalidExecutionTransition
from src.infrastructure.database.dynamodb_repository import (
    DynamoDBExecutionRepository,
)
from src.infrastructure.observability.logging import bind_context, log_event
from src.infrastructure.observability.telemetry import record_event


def create_app(repository: DynamoDBExecutionRepository) -> FastAPI:
    app = FastAPI(title="Execution Service", version="0.1.0")
    app.state.repository = repository
    app.include_router(executions.router)

    @app.middleware("http")
    async def observability_middleware(request: Request, call_next):
        raw_value = request.headers.get("X-Correlation-ID")
        try:
            correlation_id = UUID(raw_value) if raw_value else uuid4()
        except ValueError:
            correlation_id = uuid4()
        started = perf_counter()
        with bind_context(correlation_id=correlation_id):
            response = await call_next(request)
            duration_ms = round((perf_counter() - started) * 1000, 2)
            attributes = {
                "service": "execution-service",
                "correlation_id": str(correlation_id),
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
                "duration_ms": duration_ms,
            }
            log_event("http.request", **attributes)
            record_event("HttpRequest", attributes)
        response.headers["X-Correlation-ID"] = str(correlation_id)
        return response

    @app.exception_handler(InvalidExecutionTransition)
    async def invalid_transition_handler(request, exc):
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    @app.get("/health", tags=["health"])
    def health() -> dict[str, str]:
        return {"status": "ok"}

    return app
