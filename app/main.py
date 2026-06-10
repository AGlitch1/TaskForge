from fastapi import FastAPI

from app.api.jobs import router as jobs_router
from app.api.queue import router as queue_router
from app.api.workers import router as workers_router
from app.core.config import get_settings


settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    debug=settings.debug,
)


@app.get("/health")
def health_check() -> dict[str, str]:
    return {
        "status": "ok",
        "service": settings.app_name,
        "environment": settings.app_env,
    }


app.include_router(jobs_router)
app.include_router(queue_router)
app.include_router(workers_router)