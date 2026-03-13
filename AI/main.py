"""
ATLAS BI — FastAPI Backend
"""
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from api.query import router as query_router
from api.filters import router as filters_router
from api.chat import router as chat_router

load_dotenv()

app = FastAPI(title="ATLAS BI API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://atlas-bi-ui.vercel.app",
        "https://*.vercel.app",
        "http://localhost:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(query_router)
app.include_router(filters_router)
app.include_router(chat_router)


@app.get("/")
def root():
    return {"status": "ok", "service": "ATLAS BI API", "version": "2.0.0"}


@app.get("/health")
def health():
    return {"status": "healthy"}
