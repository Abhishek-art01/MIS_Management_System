import os
from pathlib import Path
from contextlib import asynccontextmanager
from sqlalchemy import text
from sqlmodel import Session

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware

# Internal Imports
from database import create_db_and_tables, engine
from admin import setup_admin
from api import cleaner_api, locality_api, page_route_api, download_api, gps_api, b2b_api

# --- 1. CONFIGURATION & PATHS ---
BASE_DIR = Path(__file__).resolve().parent
CLIENT_DIR = BASE_DIR.parent / "client"

DIRS = {
    "home": CLIENT_DIR / "HomePage",
    "login": CLIENT_DIR / "LoginPage",
    "cleaner": CLIENT_DIR / "DataCleaner",
    "operation-manager": CLIENT_DIR / "OperationManager",
    "locality": CLIENT_DIR / "LocalityCorner",
    "components": CLIENT_DIR / "Components",
    "gps": CLIENT_DIR / "GPSCorner",
    "b2b": CLIENT_DIR / "B2BCorner"
}

# --- 2. LIFESPAN (Startup & Sequence Fix) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    
    # Auto-fix sequence for Address Table
    try:
        with Session(engine) as session:
            session.exec(text("SELECT setval(pg_get_serial_sequence('t3_address_locality', 'id'), coalesce(max(id),0) + 1, false) FROM t3_address_locality;"))
            session.commit()
            print("✅ Address Table Sequence Sync Completed.")
    except Exception:
        pass 
        
    yield

app = FastAPI(lifespan=lifespan)

# --- 3. MIDDLEWARE ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://localhost:5000",
        "https://aitarowdatacleaner.onrender.com"
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["Authorization", "Content-Type"],
)

on_render = os.environ.get("RENDER") is not None
app.add_middleware(
    SessionMiddleware,
    secret_key="super_secret_static_key",
    max_age=3600, 
    https_only=on_render, 
    same_site="lax"
)

# --- 4. ROUTERS ---
app.include_router(page_route_api.router)
app.include_router(cleaner_api.router)
app.include_router(locality_api.router)
app.include_router(download_api.router)
app.include_router(gps_api.router)
app.include_router(b2b_api.router)

# --- 5. STATIC FILES ---
app.mount("/home-static", StaticFiles(directory=DIRS["home"]), name="home_static")
app.mount("/login-static", StaticFiles(directory=DIRS["login"]), name="login_static")
app.mount("/cleaner-static", StaticFiles(directory=DIRS["cleaner"]), name="cleaner_static")
app.mount("/locality-static", StaticFiles(directory=DIRS["locality"]), name="locality_static")
app.mount("/operation-manager-static", StaticFiles(directory=DIRS["operation-manager"]), name="operation-manager_static")
app.mount("/components-static", StaticFiles(directory=DIRS["components"]), name="components_static")
app.mount("/Components", StaticFiles(directory=DIRS["components"]), name="components_root")

app.mount("/b2b-static", StaticFiles(directory=DIRS["b2b"]), name="b2b_static")
app.mount("/gps-static", StaticFiles(directory=DIRS["gps"]), name="gps_static") 

setup_admin(app)