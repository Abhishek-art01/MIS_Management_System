import pandas as pd
from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session, select
from pydantic import BaseModel

from database import get_session
from models import TripData

router = APIRouter(prefix="/api/b2b", tags=["B2B Maker"])

class MarkB2BRequest(BaseModel):
    drop_id: str
    pickup_id: str
    is_b2b: bool

@router.get("/potential_pairs")
def get_potential_b2b_pairs(session: Session = Depends(get_session)):
    # STEP 1: Fetch data. Note: Sometimes Excel imports '1' as a string or float.
    # We fetch everything that isn't empty, then filter strictly in pandas to be safe.
    trips = session.exec(select(TripData)).all()
    
    if not trips:
        print("DEBUG: Database is completely empty.")
        return []

    df = pd.DataFrame([t.model_dump() for t in trips])
    
    # -- BULLETPROOF CLEANING --
    # Convert billable count to numeric safely
    df['billable_count_clean'] = pd.to_numeric(df['billable_count'], errors='coerce')
    
    # Clean strings: uppercase and strip hidden spaces
    df['dir_clean'] = df['trip_direction'].astype(str).str.strip().str.upper()
    df['cab_clean'] = df['cab_reg_no'].astype(str).str.strip().str.upper()
    df['remark_clean'] = df['ba_remark'].astype(str).str.strip().str.upper()
    
    # Filter Step 1: Billable count == 1 AND Not already marked BB
    df = df[(df['billable_count_clean'] == 1) & (df['remark_clean'] != 'BB')]
    print(f"DEBUG: Trips after Billable Count=1 & Not BB: {len(df)}")

    if df.empty:
        return []

    # -- DATETIME PARSING --
    # Combine date and time, coerce errors to NaT (Not a Time)
    df['datetime_str'] = df['shift_date'].astype(str).str.strip() + ' ' + df['shift_time'].astype(str).str.strip()
    df['parsed_dt'] = pd.to_datetime(df['datetime_str'], errors='coerce', dayfirst=True) # dayfirst handles DD/MM/YYYY
    
    print(f"DEBUG: Rows with valid parsed dates: {df['parsed_dt'].notna().sum()} / {len(df)}")

    # STEP 2: Sort by Cab No and DateTime
    df = df.sort_values(by=['cab_clean', 'parsed_dt'])
    
    # STEP 3: Shift columns to compare with previous row (previous trip of the same cab)
    df['prev_uid'] = df['unique_id'].shift(1)
    df['prev_cab'] = df['cab_clean'].shift(1)
    df['prev_dir'] = df['dir_clean'].shift(1)
    df['prev_dt'] = df['parsed_dt'].shift(1)
    df['prev_marshall'] = df['marshall'].shift(1)

    # Time difference in hours
    df['time_diff'] = (df['parsed_dt'] - df['prev_dt']).dt.total_seconds() / 3600.0

    # STEP 4: Core Logic
    # 1. Cab is the same
    # 2. Previous direction was DROP, current is PICKUP
    # 3. Time difference is between 0 and 4.5 hours
    valid_pair_cond = (
        (df['cab_clean'] == df['prev_cab']) &
        (df['prev_dir'] == 'DROP') &
        (df['dir_clean'] == 'PICKUP') &
        (df['time_diff'] >= 0) & 
        (df['time_diff'] <= 4.5)
    )

    # STEP 5: Guard condition
    has_guard_cond = (
        df['marshall'].astype(str).str.contains('GUARD', case=False, na=False) |
        df['prev_marshall'].astype(str).str.contains('GUARD', case=False, na=False)
    )

    # Apply final filters
    bb_candidates = df[valid_pair_cond & has_guard_cond]
    print(f"DEBUG: Final B2B Candidates found: {len(bb_candidates)}")

    # Re-map to final output
    pairs = []
    for _, row in bb_candidates.iterrows():
        # Find the original dicts using unique_id
        drop_trip = next((t for t in trips if t.unique_id == row['prev_uid']), None)
        pickup_trip = next((t for t in trips if t.unique_id == row['unique_id']), None)
        
        if drop_trip and pickup_trip:
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