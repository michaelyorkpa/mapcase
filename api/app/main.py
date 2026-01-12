from fastapi import FastAPI
from app.routers import health
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

import os

app = FastAPI(
	title="Mapcase API",
	root_path=os.getenv("ROOT_PATH", ""),
	# (these are the  defaults, but keeping them explicit is nice)
	docs_url="/docs",
	openapi_url="/openapi.json",
)

DATABASE_URL = os.getenv("DATABASE_URL", "")
engine = create_async_engine(DATABASE_URL, pool_pre_ping=True) if DATABASE_URL else None

app.include_router(health.router)

@app.get("/db-check")
async def db_check():
    if engine is None:
        return {"ok": False, "error": "DATABASE_URL not set"}

    async with engine.connect() as conn:
        result = await conn.execute(text("select 1 as one;"))
        row = result.first()
    return {"ok": True, "result": row.one}
