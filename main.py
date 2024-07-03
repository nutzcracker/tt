import uvicorn
import settings
from fastapi import FastAPI
from fastapi import APIRouter
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from pathlib import Path

from api.admin_handlers import admin_router

app = FastAPI(
    title="Metrics urls",
    redoc_url=None,
    docs_url=None,
)

app.mount("/static", StaticFiles(directory=Path(__file__).parent.absolute() / "static"), name="static")

# CORS

origins = [
    "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

main_api_router = APIRouter()
main_api_router.include_router(admin_router, prefix="/admin")

app.include_router(main_api_router)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=settings.APP_PORT, reload=True)
