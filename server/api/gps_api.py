from fastapi import APIRouter, Depends, HTTPException, Query
from sqlmodel import Session, select
from pydantic import BaseModel
from typing import Optional

# Internal Imports
from database import get_session
from models import TripData  # Assuming TripData is the model holding these fields

router = APIRouter(prefix="/api", tags=["GPS"])

# --- PYDANTIC MODEL FOR INCOMING DATA ---
class GPSUpdatePayload(BaseModel):
    journey_start: Optional[str] = None
    journey_end: Optional[str] = None
    gps_time: Optional[str] = None
    gps_remark: Optional[str] = None

# --- 1. FETCH TRIPS API ---
@router.get("/gps_trips")
def get_gps_trips(
    date: Optional[str] = Query(None),
    vehicle: Optional[str] = Query(None),
    trip_direction: Optional[str] = Query(None),
    trip_id: Optional[str] = Query(None),
    session: Session = Depends(get_session)
):
    # Start with a base query
    statement = select(TripData)

    # Apply filters dynamically if they exist in the URL
    if date:
        statement = statement.where(TripData.trip_date == date)
    if vehicle:
        statement = statement.where(TripData.cab_reg_no.icontains(vehicle))
    if trip_direction:
        statement = statement.where(TripData.trip_direction.icontains(trip_direction))
    if trip_id:
        statement = statement.where(TripData.trip_id.icontains(trip_id))

    # Execute query
    trips = session.exec(statement).all()
    
    return trips

# --- 2. UPDATE GPS DATA API ---
@router.post("/update_gps/{unique_id}")
def update_gps_data(
    unique_id: str,
    payload: GPSUpdatePayload,
    session: Session = Depends(get_session)
):
    # Find the specific trip by its unique ID
    trip = session.exec(select(TripData).where(TripData.unique_id == unique_id)).first()
    
    if not trip:
        raise HTTPException(status_code=404, detail="Trip not found")

    # Update the fields
    trip.journey_start_location = payload.journey_start
    trip.journey_end_location = payload.journey_end
    trip.gps_time = payload.gps_time
    trip.gps_remark = payload.gps_remark

    # Save to database
    session.add(trip)
    session.commit()
    session.refresh(trip)

    return {"message": "GPS data updated successfully", "status": "success"}