import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlmodel import Session, select
from pydantic import BaseModel
from typing import List

from database import get_session
from models import TripData, TollData

router = APIRouter(prefix="/api/toll", tags=["Toll Audit"])


class MarkTollRequest(BaseModel):
    trip_unique_id: str
    selected_toll_ids: List[str]


# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────

def clean_val(v):
    try:
        if v is None:
            return None
        if isinstance(v, float) and pd.isna(v):
            return None
        if isinstance(v, pd.Timestamp) and pd.isna(v):
            return None
        return v
    except Exception:
        return v


def clean_dict(d: dict) -> dict:
    return {k: clean_val(v) for k, v in d.items()}


def is_blank(val) -> bool:
    if val is None:
        return True
    s = str(val).strip().lower()
    return s in ("", "none", "nan", "nat", "0")


def parse_trip_dt(row):
    for fmt in ["{date} {time}", "{date}T{time}"]:
        try:
            s = fmt.format(
                date=str(row["shift_date"]).strip(),
                time=str(row["shift_time"]).strip()
            )
            return pd.to_datetime(s, dayfirst=True)
        except Exception:
            pass
    try:
        return pd.to_datetime(str(row["shift_date"]).strip(), dayfirst=True)
    except Exception:
        return pd.NaT


# ─────────────────────────────────────────────
#  GET AVAILABLE SHIFT DATES  (for filter dropdown)
# ─────────────────────────────────────────────

@router.get("/available_dates")
def get_available_dates(session: Session = Depends(get_session)):
    all_trips = session.exec(select(TripData)).all()
    if not all_trips:
        return []
    dates = set()
    for t in all_trips:
        val = str(t.shift_date or "").strip()
        if val and val.lower() not in ("none", "nan", "nat", ""):
            dates.add(val)

    def norm(d):
        try:
            return pd.to_datetime(d, dayfirst=True).strftime("%Y-%m-%d")
        except Exception:
            return d

    return sorted(dates, key=norm, reverse=True)  # most recent first


# ─────────────────────────────────────────────
#  GET POTENTIAL MATCHES
#  - shift_date: required (selected from filter)
#  - time_gap_hours: ±window in hours (1.0 to 4.0, step 0.333 i.e. 20 min)
#  - only returns trips that have at least one matching toll
#    (updated trips are always returned regardless)
# ─────────────────────────────────────────────

@router.get("/potential_matches")
def get_potential_tolls(
    shift_date: str = Query(..., description="Shift date e.g. 2024-05-10"),
    time_gap_hours: float = Query(1.5, description="±match window in hours (1.0–4.0)"),
    session: Session = Depends(get_session),
):
    # Clamp time gap to valid range 1h–4h
    time_gap_hours = max(1.0, min(4.0, time_gap_hours))
    print(f"\n[TollAudit] shift_date={shift_date!r}  gap=±{time_gap_hours}h")

    # 1. Trips for this date only
    all_trips = session.exec(select(TripData)).all()
    trips_for_date = [
        t for t in all_trips
        if str(t.shift_date or "").strip() == shift_date.strip()
    ]
    if not trips_for_date:
        print(f"[TollAudit] No trips for {shift_date!r}")
        return []

    print(f"[TollAudit] Trips: {len(trips_for_date)}")

    # 2. All tolls
    all_tolls = session.exec(select(TollData)).all()
    print(f"[TollAudit] Tolls in DB: {len(all_tolls)}")

    # 3. DataFrames
    df_trips = pd.DataFrame([t.model_dump() for t in trips_for_date])
    df_tolls = (
        pd.DataFrame([t.model_dump() for t in all_tolls])
        if all_tolls else pd.DataFrame()
    )

    # 4. Clean cab / vehicle numbers
    df_trips["cab_clean"] = (
        df_trips["cab_reg_no"].astype(str)
        .str.strip().str.upper().str.replace(r"\s+", "", regex=True)
    )
    if not df_tolls.empty:
        veh_col = next(
            (c for c in ("veh", "vehicle_number", "vehicle_no") if c in df_tolls.columns),
            None,
        )
        if veh_col is None:
            print("[TollAudit] WARNING: no vehicle column in TollData!")
            df_tolls["veh_clean"] = ""
        else:
            df_tolls["veh_clean"] = (
                df_tolls[veh_col].astype(str)
                .str.strip().str.upper().str.replace(r"\s+", "", regex=True)
            )

    # 5. Parse datetimes
    df_trips["trip_dt"] = df_trips.apply(parse_trip_dt, axis=1)
    nat_trips = df_trips["trip_dt"].isna().sum()
    if nat_trips:
        print(f"[TollAudit] NaT trip_dt: {nat_trips}/{len(df_trips)}")

    if not df_tolls.empty:
        dt_col = next(
            (c for c in ("travel_date_time", "travel_datetime", "datetime")
             if c in df_tolls.columns), None,
        )
        if dt_col:
            df_tolls["toll_dt"] = pd.to_datetime(
                df_tolls[dt_col], errors="coerce", dayfirst=True
            )
        else:
            print("[TollAudit] WARNING: no datetime column in TollData!")
            df_tolls["toll_dt"] = pd.NaT

    # 6. Narrow tolls to date window (big speed boost)
    if not df_tolls.empty and df_tolls["toll_dt"].notna().any():
        try:
            target_dt = pd.to_datetime(shift_date, dayfirst=True)
            df_tolls = df_tolls[
                df_tolls["toll_dt"].isna() |
                (
                    (df_tolls["toll_dt"] >= target_dt - pd.Timedelta(hours=time_gap_hours + 1))
                    & (df_tolls["toll_dt"] <= target_dt + pd.Timedelta(hours=24 + time_gap_hours + 1))
                )
            ].copy()
            print(f"[TollAudit] Tolls after window filter: {len(df_tolls)}")
        except Exception as e:
            print(f"[TollAudit] Window filter error: {e}")

    # 7. Linked-toll lookup  (trip unique_id → toll rows)
    linked_tolls_map: dict = {}
    if not df_tolls.empty and "unique_id" in df_tolls.columns:
        for _, trow in df_tolls.iterrows():
            uid = str(trow.get("unique_id") or "").strip()
            if not is_blank(uid):
                td = clean_dict(trow.to_dict())
                td.pop("toll_dt", None); td.pop("veh_clean", None)
                linked_tolls_map.setdefault(uid, []).append(td)

    # 8. Unassigned toll pool
    if not df_tolls.empty and "unique_id" in df_tolls.columns:
        df_unassigned = df_tolls[df_tolls["unique_id"].apply(is_blank)].copy()
    elif not df_tolls.empty:
        df_unassigned = df_tolls.copy()
    else:
        df_unassigned = pd.DataFrame()

    print(f"[TollAudit] Unassigned tolls: {len(df_unassigned)}")

    # 9. Match loop
    matches = []

    for _, trip in df_trips.iterrows():
        trip_dict = clean_dict(trip.to_dict())
        trip_dict.pop("trip_dt", None); trip_dict.pop("cab_clean", None)

        trip_uid  = str(trip.get("unique_id") or "").strip()
        toll_name = str(trip.get("toll_name") or "").strip()
        is_updated = not is_blank(toll_name)

        if is_updated:
            # Only include if linked tolls actually exist for this trip
            toll_list = linked_tolls_map.get(trip_uid, [])
            if not toll_list:
                continue
            matches.append({"trip": trip_dict, "tolls": toll_list})
            continue

        # Pending: only include if toll matches found
        if df_unassigned.empty or pd.isna(trip["trip_dt"]) or is_blank(trip["cab_clean"]):
            continue   # ← skip, no tolls possible

        veh_tolls = df_unassigned[df_unassigned["veh_clean"] == trip["cab_clean"]]
        veh_tolls = veh_tolls[veh_tolls["toll_dt"].notna()]
        if veh_tolls.empty:
            continue   # ← skip, no vehicle match

        time_diffs  = (veh_tolls["toll_dt"] - trip["trip_dt"]).dt.total_seconds() / 3600.0
        valid_tolls = veh_tolls[
            (time_diffs >= -time_gap_hours) & (time_diffs <= time_gap_hours)
        ]

        if valid_tolls.empty:
            continue   # ← skip, nothing in time window

        toll_list = []
        for t in valid_tolls.to_dict(orient="records"):
            td = clean_dict(t)
            td.pop("toll_dt", None); td.pop("veh_clean", None)
            toll_list.append(td)

        matches.append({"trip": trip_dict, "tolls": toll_list})

    print(f"[TollAudit] Matches returned: {len(matches)}\n")
    return matches


# ─────────────────────────────────────────────
#  MARK / LINK TOLLS TO TRIP
# ─────────────────────────────────────────────

@router.post("/mark")
def mark_toll_trips(payload: MarkTollRequest, session: Session = Depends(get_session)):
    trip = session.exec(
        select(TripData).where(TripData.unique_id == payload.trip_unique_id)
    ).first()
    if not trip:
        raise HTTPException(status_code=404, detail=f"Trip not found: {payload.trip_unique_id}")

    total_amount = 0.0
    toll_names   = []

    for toll_id_str in payload.selected_toll_ids:
        toll = None
        try:
            toll = session.exec(select(TollData).where(TollData.id == int(toll_id_str))).first()
        except (ValueError, TypeError):
            pass
        if toll is None:
            toll = session.exec(select(TollData).where(TollData.id == toll_id_str)).first()

        if toll:
            toll.unique_id  = trip.unique_id
            total_amount   += float(toll.amount or 0)
            toll_names.append(str(toll.transaction_description or "Unknown Toll"))
            session.add(toll)
        else:
            print(f"[TollAudit] WARNING: Toll id={toll_id_str} not found.")

    if not toll_names:
        raise HTTPException(status_code=400, detail="None of the toll IDs were found.")

    trip.unique_toll_id = ",".join(payload.selected_toll_ids)
    trip.toll_amount    = total_amount
    trip.toll_name      = " | ".join(toll_names)
    session.add(trip)
    session.commit()

    return {
        "message"       : "Tolls linked!",
        "trip_unique_id": payload.trip_unique_id,
        "toll_count"    : len(toll_names),
        "total_amount"  : total_amount,
    }