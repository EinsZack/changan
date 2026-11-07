"""Internal gateway: session/attempt-scoped routes, no management endpoints."""
from fastapi import FastAPI
from .engine import Engine
from .container_execution import router
engine=Engine()
app=FastAPI(title='Container result gateway',docs_url=None,redoc_url=None,openapi_url=None)
app.include_router(router)
@app.get('/health')
def health():return {'status':'ok','service':'container-gateway'}

from .session_execution import router as session_router
app.include_router(session_router)
