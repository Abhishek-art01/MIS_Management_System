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
# AUDIT TRIPS API 
# ==========================================

from fastapi import APIRouter, HTTPException
import pandas as pd
from database import engine  # Assuming your database connection

router = APIRouter(prefix="/api/audit", tags=["Audit"])

@router.get("/{audit_type}")
async def get_audit_data(audit_type: str):
    """
    Returns filtered trip data for the 6 specific audit types.
    """
    query = ""

    if audit_type == "mcd":
        query = "SELECT trip_id, cab_reg_no, shift_date FROM trips WHERE ba_remark LIKE '%MCD%'"
    
    elif audit_type == "alt_vehicle":
        query = "SELECT trip_id, employee_name, mis_remark FROM trips WHERE mis_remark = 'ALT VEH'"
    
    elif audit_type == "toll":
        query = "SELECT travel_date_time, vehicle_no, plaza_name, tag_debit_credit FROM fastag_data"
    
    elif audit_type == "b2b":
        query = "SELECT * FROM trips WHERE in_app_extra = 'B2B'"
        
    elif audit_type == "vehicle":
        query = "SELECT DISTINCT cab_reg_no, vendor, office FROM trips"
        
    elif audit_type == "incomplete":
        query = "SELECT trip_id, employee_id FROM trips WHERE trip_id IS NULL OR employee_id IS NULL"
    
    else:
        raise HTTPException(status_code=400, detail="Invalid Audit Type")

    try:
        df = pd.read_sql(query, engine)
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))