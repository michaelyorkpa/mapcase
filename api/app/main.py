from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.routers import health
from app.routers import weather
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

import os

@asynccontextmanager
async def lifespan(app: FastAPI):
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        #fail FAST so you see it immediately in logs (isntead of Swagger 500s)
        raise RuntimeError("DATABASE_URL is not set inside the container")
    
    engine = create_async_engine(database_url, pool_pre_ping=True)
    app.state.engine = engine
    yield
    await engine.dispose()

app = FastAPI(
	title="Mapcase API",
	root_path=os.getenv("ROOT_PATH", ""),
	# (these are the  defaults, but keeping them explicit is nice)
	docs_url="/docs",
	openapi_url="/openapi.json",
    lifespan=lifespan,
)

app.include_router(health.router)
app.include_router(weather.router)

@app.get("/db-check")
async def db_check():
    if engine is None:
        return {"ok": False, "error": "DATABASE_URL not set"}

    async with engine.connect() as conn:
        result = await conn.execute(text("select 1 as one;"))
        row = result.first()
    return {"ok": True, "result": row.one}
