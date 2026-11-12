FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 POETRY_VIRTUALENVS_CREATE=false
WORKDIR /app
RUN pip install --no-cache-dir poetry==2.2.1
COPY pyproject.toml poetry.lock ./
RUN poetry install --only main --no-root --no-interaction
COPY src ./src
RUN useradd --create-home --uid 10001 appuser && chown -R appuser:appuser /app
USER 10001
EXPOSE 8000
CMD ["newrelic-admin", "run-program", "uvicorn", "src.api.runtime:create_runtime_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
