import pandas as pd
from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session, select
from pydantic import BaseModel
from typing import List

from database import get_session
from models import TripData, TollData

router = APIRouter(prefix="/api/toll", tags=["Toll Audit"])

class MarkTollRequest(BaseModel):
    trip_unique_id: str
    selected_toll_ids: List[str]

@router.get("/potential_matches")
def get_potential_tolls(session: Session = Depends(get_session)):
    # 1. Fetch Trips (Only those that don't already have tolls assigned)
    trips = session.exec(select(TripData).where(
        (TripData.unique_toll_id == None) | (TripData.unique_toll_id == "")
    )).all()
    
    # 2. Fetch Tolls (Only those not yet assigned to a trip based on your new model)
    tolls = session.exec(select(TollData).where(
        (TollData.unique_id == None) | (TollData.unique_id == "")
    )).all()

    if not trips or not tolls:
        return []

    # 3. Convert to DataFrames safely
    df_trips = pd.DataFrame([t.model_dump() for t in trips])
    df_tolls = pd.DataFrame([t.model_dump() for t in tolls])

    # 4. Clean Vehicle Numbers for exact matching
    df_trips['cab_clean'] = df_trips['cab_reg_no'].astype(str).str.strip().str.upper()
    df_tolls['veh_clean'] = df_tolls['veh'].astype(str).str.strip().str.upper()

    # 5. Parse DateTimes to calculate the 1.5 hour window
    df_trips['trip_dt'] = pd.to_datetime(
        df_trips['shift_date'].astype(str) + ' ' + df_trips['shift_time'].astype(str), 
        errors='coerce', dayfirst=True
    )
    df_tolls['toll_dt'] = pd.to_datetime(df_tolls['travel_date_time'], errors='coerce', dayfirst=True)

    matches = []

    # 6. Find Matches
    for _, trip in df_trips.iterrows():
        if pd.isna(trip['trip_dt']) or pd.isna(trip['cab_clean']):
            continue
            
        # Filter tolls by matching Cab Number
        veh_tolls = df_tolls[df_tolls['veh_clean'] == trip['cab_clean']]
        
        if veh_tolls.empty:
            continue
            
        # Calculate time difference in hours (Positive = After Trip, Negative = Before Trip)
        time_diffs = (veh_tolls['toll_dt'] - trip['trip_dt']).dt.total_seconds() / 3600.0
        
        # Keep tolls within ± 1.5 hours
        valid_tolls = veh_tolls[(time_diffs >= -1.5) & (time_diffs <= 1.5)]
        
        if not valid_tolls.empty:
            toll_list = valid_tolls.to_dict(orient='records')
            
            # Clean up non-serializable datetime objects before returning JSON
            trip_dict = trip.to_dict()
            trip_dict.pop('trip_dt', None)
            for t in toll_list:
                t.pop('toll_dt', None)
                
            matches.append({
                "trip": trip_dict,
                "tolls": toll_list
            })

    return matches


@router.post("/mark")
def mark_toll_trips(payload: MarkTollRequest, session: Session = Depends(get_session)):
    # 1. Fetch the Trip
    trip = session.exec(select(TripData).where(TripData.unique_id == payload.trip_unique_id)).first()
    if not trip:
        raise HTTPException(status_code=404, detail="Trip not found")

    total_amount = 0.0
    toll_names = []

    # 2. Fetch and Update the Selected Tolls
    for toll_id in payload.selected_toll_ids:
        toll = session.exec(select(TollData).where(TollData.id == toll_id)).first()
        if toll:
            # Map the Trip's Unique ID into the Toll Data table
            toll.unique_id = trip.unique_id
            
            # Aggregate data for the Trip table
            total_amount += float(toll.amount or 0)
            toll_names.append(str(toll.transaction_description or 'Unknown Toll'))
            
            session.add(toll)

    # 3. Update the Trip Data
    trip.unique_toll_id = ",".join(payload.selected_toll_ids) # This requires unique_toll_id to be a string!
    trip.toll_amount = total_amount
    trip.toll_name = " | ".join(toll_names)
    
    session.add(trip)
    session.commit()

    return {"message": "Tolls successfully linked to trip!"}