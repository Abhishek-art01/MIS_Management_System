from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session, select, col
from database import get_session
from models import TripData
from datetime import datetime
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

# 1. Setup Path to the specific folder for this API
BASE_DIR = Path(__file__).resolve().parent.parent.parent
CLIENT_DIR = BASE_DIR / "client"

# 2. Initialize the specific template for GPS
# Note: We name it 'templates' so your existing return statement works, 
# but we remove the ["gps"] part.
templates = Jinja2Templates(directory=str(CLIENT_DIR / "GPSCorner"))
router = APIRouter()


# ==========================================
# GPS CORNER API 
# ==========================================

@router.get("/gps-corner")
async def gps_page(request: Request):
    if not request.session.get("user"): return RedirectResponse(url="/login", status_code=303)
    return templates.TemplateResponse("gps_corner.html", {"request": request, "user": request.session.get("user")})


# 1. THE GET ROUTE
@router.get("/api/gps_trips", response_model=List[TripData])
def read_gps_trips(
    date: str = None, 
    vehicle: str = None, 
    trip_direction: str = None,
    session: Session = Depends(get_session)
):
    # Base query
    query = select(TripData)
    
    # FILTER 1: Exclude 'Pay' status
    if hasattr(TripData, "clubbing_status"):
        query = query.where(col(TripData.clubbing_status).ilike("%not pay%"))

    # FILTER 2: Date 
    if date:
        try:
            # Parse HTML 'YYYY-MM-DD' -> Convert to DB 'DD-MM-YYYY'
            date_obj = datetime.strptime(date, "%Y-%m-%d")
            formatted_date = date_obj.strftime("%d-%m-%Y")
            
            print(f"🔍 Searching for date: {formatted_date}") 
            query = query.where(TripData.shift_date.contains(formatted_date))
        except ValueError:
            # Fallback for simple string match
            query = query.where(TripData.shift_date.contains(date))

    # FILTER 3: Vehicle
    if vehicle:
        query = query.where(TripData.cab_reg_no.contains(vehicle))

    results = session.exec(query).all()
    return results

# 2. THE UPDATE ROUTE (Using Unique ID)
# ---------------------------------------------------------
# ROBUST UPDATE ROUTE (Fixes Key Mismatch & Scientific Notation)
# ---------------------------------------------------------
@router.post("/api/update_gps/{unique_id}")
def update_gps_data(unique_id: str, payload: dict, session: Session = Depends(get_session)):
    print(f"🔥 DEBUG: Request for Unique ID: {unique_id}")
    print(f"📦 DEBUG: Data Received: {payload}")

    # 1. Find the trip
    # We strip whitespace just in case
    clean_id = str(unique_id).strip()
    statement = select(TripData).where(col(TripData.unique_id) == clean_id)
    trip = session.exec(statement).first()
    
    if not trip:
        print(f"❌ DEBUG: Unique ID '{clean_id}' not found.")
        raise HTTPException(status_code=404, detail="Trip not found")

    # 2. UPDATE FIELDS (Checking BOTH possible key names)
    
    # Start Location
    if "journey_start_location" in payload:
        trip.journey_start_location = payload["journey_start_location"]
    elif "journey_start" in payload:
        trip.journey_start_location = payload["journey_start"]
    elif "start" in payload:
        trip.journey_start_location = payload["start"]

    # End Location
    if "journey_end_location" in payload:
        trip.journey_end_location = payload["journey_end_location"]
    elif "journey_end" in payload:
        trip.journey_end_location = payload["journey_end"]
    elif "end" in payload:
        trip.journey_end_location = payload["end"]

    # Remarks
    if "gps_remark" in payload:
        trip.gps_remark = payload["gps_remark"]
    elif "remark" in payload:
        trip.gps_remark = payload["remark"]

    # GPS Time
    if "gps_time" in payload:
        trip.gps_time = payload["gps_time"]

    # 3. Save to DB
    session.add(trip)
    session.commit()
    session.refresh(trip)
    
    print(f"✅ DEBUG: Saved to DB! Start={trip.journey_start_location}, End={trip.journey_end_location}")
    return {"status": "success", "data": trip}