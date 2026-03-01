import os
import io
from sqlalchemy import text
import pandas as pd
from pathlib import Path
from contextlib import asynccontextmanager
from typing import List, Optional
from datetime import datetime
from fastapi import APIRouter, Depends, Request, Form, Response, UploadFile, File, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse, FileResponse, JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from pydantic import BaseModel

# SQLModel & Admin
from sqlmodel import select, Session, desc, col, update, SQLModel
from sqladmin import Admin, ModelView
from sqladmin.authentication import AuthenticationBackend

# --- INTERNAL IMPORTS ---
from auth import verify_password, get_password_hash
from database import create_db_and_tables, get_session, engine
from models import User, ClientData, RawTripData, OperationData, TripData, T3AddressLocality, T3LocalityZone, T3ZoneKm, BARowData
from cleaner.mis_data_cleaner import process_client_data, process_raw_data,process_ba_row_data
from cleaner.fastag_data_cleaner import process_fastag_data
from cleaner.operation_data_cleaner import process_operation_app_data

BASE_DIR = Path(__file__).resolve().parent.parent.parent
CLIENT_DIR = BASE_DIR / "client"

# 2. Define Templates
templates = {
    "home": Jinja2Templates(directory=str(CLIENT_DIR / "HomePage")),
    "login": Jinja2Templates(directory=str(CLIENT_DIR / "LoginPage")),
}

router = APIRouter()

# --- 7. PAGE ROUTES ---
@router.get("/")
async def read_root(request: Request):
    user = request.session.get("user")
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    return templates["home"].TemplateResponse("homepage.html", {"request": request, "user": user})

@router.get("/login")
async def login_page(request: Request):
    if request.session.get("user"):
        return RedirectResponse(url="/", status_code=303)
    return templates["login"].TemplateResponse("login.html", {"request": request})

@router.post("/login")
async def login_user(request: Request, username: str = Form(...), password: str = Form(...), session: Session = Depends(get_session)):
    user = session.exec(select(User).where(User.username == username)).first()
    if user and verify_password(password, user.password_hash):
        request.session["user"] = user.username
        return RedirectResponse(url="/", status_code=303)
    return templates["login"].TemplateResponse("login.html", {"request": request, "error": "Invalid credentials"})

@router.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/")




@router.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return Response(status_code=204) 

from fastapi import APIRouter
from fastapi.responses import FileResponse
import os

router = APIRouter()

# Define the path to your client folder
CLIENT_DIR = os.path.join(os.getcwd(), "..", "client")

@router.get("/audit/mcd")
async def serve_mcd():
    return FileResponse(os.path.join(CLIENT_DIR, "AuditCorner", "audit_mcd.html"))

@router.get("/audit/alt-vehicle")
async def serve_alt():
    return FileResponse(os.path.join(CLIENT_DIR, "AuditCorner", "audit_alt_vehicle.html"))

@router.get("/audit/toll")
async def serve_toll():
    return FileResponse(os.path.join(CLIENT_DIR, "AuditCorner", "audit_toll.html"))

@router.get("/audit/b2b")
async def serve_b2b():
    return FileResponse(os.path.join(CLIENT_DIR, "AuditCorner", "audit_b2b.html"))

@router.get("/audit/vehicle")
async def serve_veh():
    return FileResponse(os.path.join(CLIENT_DIR, "AuditCorner", "audit_vehicle.html"))

@router.get("/audit/incomplete")
async def serve_incomplete():
    return FileResponse(os.path.join(CLIENT_DIR, "AuditCorner", "audit_incomplete.html"))