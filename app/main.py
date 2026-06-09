from fastapi import FastAPI

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