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


BASE_DIR = Path(__file__).resolve().parent.parent.parent
CLIENT_DIR = BASE_DIR / "client"
GENERATED_DIR = BASE_DIR / "client" / "DataCleaner" / "generated"

templates = Jinja2Templates(directory=str(CLIENT_DIR / "OperationManager"))
router = APIRouter()

# ==========================================
# 4. UNIVERSAL DOWNLOAD ENDPOINTS
# ==========================================
@router.get("/operation-manager")
async def operation_manager_page(request: Request):
    if not request.session.get("user"): return RedirectResponse(url="/login", status_code=303)
    return templates.TemplateResponse(
    "operation_manager.html", 
    {"request": request}
)

@router.get("/download/{filename}")
async def download_file(filename: str, request: Request):
    if not request.session.get("user"):
        return Response("Unauthorized", status_code=401)
    
    file_path = GENERATED_DIR / filename
    if not file_path.exists():
        return Response("File not found", status_code=404)
        
    return FileResponse(
        path=file_path, 
        filename=filename, 
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
@router.get("/api/{table_type}/download")
def download_specific_table(table_type: str, session: Session = Depends(get_session)):
    model_map = {
        "operation": OperationData,
        "client": ClientData,
        "raw": RawTripData,
        "trip_data": TripData
    }
    
    if table_type not in model_map:
        return {"status": "error", "message": "Invalid table type selected."}
    
    model_class = model_map[table_type]
    statement = select(model_class)
    results = session.exec(statement).all()
    
    if not results:
        return {"status": "error", "message": f"No data found in {table_type} table."}
    
    data = [row.model_dump() for row in results]
    df = pd.DataFrame(data)
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Report')
    output.seek(0)
    
    filename = f"{table_type.capitalize()}_Export.xlsx"
    headers = {'Content-Disposition': f'attachment; filename="{filename}"'}
    return StreamingResponse(
        output, 
        headers=headers, 
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

@router.post("/api/operation/upload")
async def upload_operation_data(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents))
        
        save_path = CLIENT_DIR / "OperationManager" / "processed_db_mock.csv"
        # Ensure dir exists
        os.makedirs(save_path.parent, exist_ok=True)
        df.to_csv(save_path, index=False)

        return JSONResponse(
            content={
                "status": "success", 
                "message": f"Successfully processed {len(df)} rows and updated Database."
            }
        )
    except Exception as e:
        print(f"Error processing file: {e}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": str(e)}
        )

