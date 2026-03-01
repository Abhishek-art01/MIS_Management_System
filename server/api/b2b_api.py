import pandas as pd
from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session, select
from pydantic import BaseModel
from typing import List, Dict, Any

from database import get_session
from models import TripData

router = APIRouter(prefix="/api/b2b", tags=["B2B Maker"])

class MarkB2BRequest(BaseModel):
    drop_id: str
    pickup_id: str
    is_b2b: bool

@router.get("/potential_pairs")
def get_potential_b2b_pairs(session: Session = Depends(get_session)):
    # Step 1: Filter billable count 1
    trips = session.exec(select(TripData).where(TripData.billable_count == 1)).all()
    if not trips:
        return []

    # Convert to DataFrame for easier vectorized calculation
    df = pd.DataFrame([t.model_dump() for t in trips])
    
    # Clean up dates and times for Step 3 calculation
    df['parsed_dt'] = pd.to_datetime(df['shift_date'].astype(str) + ' ' + df['shift_time'].astype(str), errors='coerce')
    
    # Step 2: Sort by Cab No (Column M) and DateTime (Column AC)
    df = df.sort_values(by=['cab_reg_no', 'parsed_dt'])
    
    # Shift columns to compare with previous row
    df['prev_uid'] = df['unique_id'].shift(1)
    df['prev_cab'] = df['cab_reg_no'].shift(1)
    df['prev_dir'] = df['trip_direction'].shift(1)
    df['prev_dt'] = df['parsed_dt'].shift(1)
    df['prev_marshall'] = df['marshall'].shift(1)

    # Step 3 & 4: Time difference <= 4.5 hours (Time is in seconds, so / 3600 = hours)
    df['time_diff'] = (df['parsed_dt'] - df['prev_dt']).dt.total_seconds() / 3600.0

    # Step 4: Condition (Previous = DROP, Current = PICKUP, Same Cab)
    valid_pair_cond = (
        (df['cab_reg_no'] == df['prev_cab']) &
        (df['prev_dir'] == 'DROP') &
        (df['trip_direction'] == 'PICKUP') &
        (df['time_diff'] >= 0) & 
        (df['time_diff'] <= 4.5)
    )

    # Step 8: Check if Guard/Marshall is present in either trip
    has_guard_cond = (
        df['marshall'].fillna('').astype(str).str.contains('GUARD', case=False, na=False) |
        df['prev_marshall'].fillna('').astype(str).str.contains('GUARD', case=False, na=False)
    )

    # Apply all filters
    bb_candidates = df[valid_pair_cond & has_guard_cond]

    # Re-map back to the SQLAlchemy objects to send to the frontend
    pairs = []
    for _, row in bb_candidates.iterrows():
        drop_trip = next((t for t in trips if t.unique_id == row['prev_uid']), None)
        pickup_trip = next((t for t in trips if t.unique_id == row['unique_id']), None)
        
        if drop_trip and pickup_trip:
            # Only show if not already marked
            if drop_trip.ba_remark != "BB" and pickup_trip.ba_remark != "BB":
                pairs.append({
                    "drop": drop_trip.model_dump(),
                    "pickup": pickup_trip.model_dump()
                })

    return pairs

@router.post("/mark")
def mark_b2b_pair(payload: MarkB2BRequest, session: Session = Depends(get_session)):
    drop_trip = session.exec(select(TripData).where(TripData.unique_id == payload.drop_id)).first()
    pickup_trip = session.exec(select(TripData).where(TripData.unique_id == payload.pickup_id)).first()

    if not drop_trip or not pickup_trip:
        raise HTTPException(status_code=404, detail="Trips not found")

    # If user clicks "Mark as BB", update Column Y (ba_remark) to 'BB'
    if payload.is_b2b:
        drop_trip.ba_remark = "BB"
        pickup_trip.ba_remark = "BB"
    else:
        drop_trip.ba_remark = "Not BB"
        pickup_trip.ba_remark = "Not BB"

    session.add(drop_trip)
    session.add(pickup_trip)
    session.commit()

    return {"message": "Success"}