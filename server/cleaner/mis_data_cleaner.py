cdimport pandas as pd
import numpy as np
import pdfplumber
import io
import re
import xlrd
import hashlib
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
import traceback
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from datetime import datetime, timedelta
from .cleaner_helper import (
    get_mandatory_columns, 
    get_xls_style_data, 
    standardize_dataframe, 
    format_excel_sheet,
    clean_columns,
    clean_address,
    create_styled_excel,
    sync_addresses_to_t3
)


# ==========================================
# 1. CLIENT DATA CLEANER (No DB Upload)
# ==========================================
def process_client_data(file_content):
    """
    Cleans Client Data (CSV/Excel) and returns a formatted Excel file.
    Does NOT save to the database.
    """
    try:
        # 1. Define Columns to Drop
        DROP_COLS = [
            'Bunit ID', 'Cycle Start', 'Cycle End', 'Shift Date', 'Project', 'Cost Center', 
            'Department', 'Planned Emp Count', 'Travelled Emp Count', 'Billable Emp Count', 
            'No Show', 'Planned Escort', 'Actual Escort', 'Emp km', 'Trip Cost', 
            'Trip AC Cost', 'Per Emp Cost', 'Escort Cost', 'Penalty', 'Vendor Penalty', 
            'Total Cost', 'Assigned Contract', 'Cab Contract', 'Billing Zone', 
            'Trip Billing Zone', 'Emp Sigin Type', 'Escort ID', 'Toll Cost', 
            'State Tax Co   st', 'Parking Or Toll Cost', 'Per Employee Overhead Cost', 
            'Trip Source', 'Extra Kms Based On Billable Employee Count', 'Billing Kms', 
            'Actual Kms At Employee Level', 'Grid Km', 'Employee Adjustment Distance', 
            'Trip Adjustment', 'Total Distance', 'State Tax Cost', 'Flight Category', 
            'Flight Route', 'Flight Type', 'Cab Type', 'Airport Name'
        ]

        # 2. Define Mapping (CSV Headers -> DB Headers)
        COLUMN_MAPPING = {
            "Trip ID": "trip_id",
            "Billing period": "shift_date", 
            "Employee ID": "employee_id",
            "Gender": "gender",
            "Employee Name": "employee_name",
            "Shift Time": "shift_time",
            "Pickup Time": "pickup_time",
            "Drop time": "drop_time",
            "Trip direction": "trip_direction",
            "Cab reg no": "cab_reg_no",
            "Vendor": "vendor",
            "Office": "office",
            "Landmark": "landmark",
            "Address": "address",
            "Flight Number": "flight_number",
        }

        # 3. Read File (Try CSV, fallback to Excel)
        try:
            df = pd.read_csv(io.BytesIO(file_content))
            print("🔹 Processed as CSV")
        except:
            try:
                df = pd.read_excel(io.BytesIO(file_content), dtype=str)
                print("🔹 Processed as Excel")
            except Exception as e:
                print(f"❌ Error reading file: {e}")
                return None, None, None

        # 4. Drop & Rename
        df = df.drop(columns=[c for c in DROP_COLS if c in df.columns], errors='ignore')
        df = df.rename(columns=COLUMN_MAPPING)

        # 5. Add Missing Mandatory Headers (Dynamic from Helper)
        mandatory_cols = get_mandatory_columns()
        
        # Handle if helper returns Dict or List
        if isinstance(mandatory_cols, dict):
            target_cols = mandatory_cols.values()
        else:
            target_cols = mandatory_cols 

        for db_col in target_cols:
            if db_col not in df.columns:
                df[db_col] = "" # Fill missing columns with empty string

        # 6. Cleaning Logic
        # Clean Cab Reg No
        if "cab_reg_no" in df.columns:
            df["cab_reg_no"] = (
                df["cab_reg_no"]
                .astype(str)
                .str.replace("-", "", regex=False)
                .str.replace(" ", "", regex=False)
                .str.upper()
                .replace("NAN", "")
            )

        # Clean Trip Direction
        if "trip_direction" in df.columns:
            df["trip_direction"] = (
                df["trip_direction"]
                .astype(str)
                .str.strip()
                .str.title()
                .replace({"Login": "Pickup", "Logout": "Drop"})
            )

        # 7. Generate Unique ID (MD5 Hash)
        def generate_unique_id(row):
            t_id = str(row.get('trip_id', '')).strip()
            e_id = str(row.get('employee_id', '')).strip()
            
            if t_id.lower() == 'nan': t_id = ''
            if e_id.lower() == 'nan': e_id = ''

            base = f"{t_id}_{e_id}"
            return hashlib.md5(base.encode()).hexdigest()

        df["unique_id"] = df.apply(generate_unique_id, axis=1)
        df["data_source"] = "CLIENT"

        # 8. Standardize & Export
        # Note: We are returning the Excel file directly. 
        # We are NOT calling 'bulk_save_unique' or any DB function here.
        df_db = standardize_dataframe(df)
        
        if df_db is None: 
            return None, None, None

        print(f"✅ Cleaning Complete. Returning Excel with {len(df_db)} rows.")
        return create_styled_excel(df_db, "Client_Cleaned")

    except Exception as e:
        print(f"❌ Client Cleaner Error: {e}")
        traceback.print_exc()
        return None, None, None

# ==========================================
# 2. RAW DATA CLEANER
# ==========================================
def _clean_single_raw_df(df):
    try:
        # Standardize NaN values
        df = df.replace({np.nan: None, "nan": None})
        
        # Trip ID Logic
        df["Trip_ID"] = np.where(df.iloc[:, 10].astype(str).str.startswith("T"), df.iloc[:, 10], np.nan)
        df["Trip_ID"] = df["Trip_ID"].ffill()

        # Identify Row Types
        is_header = df.iloc[:, 1].astype(str).str.contains("UNITED FACILITIES", na=False)
        is_passenger = df.iloc[:, 0].astype(str).str.match(r"^[0-9]+$") 

        # Extraction Maps
        h_map = {0: 'TRIP_DATE', 1: 'AGENCY_NAME', 2: 'D_LOGIN', 3: 'VEHICLE_NO', 4: 'DRIVER_NAME', 6: 'DRIVER_MOBILE', 7: 'MARSHALL', 8: 'DISTANCE', 9: 'EMP_COUNT', 10: 'TRIP_COUNT'}
        p_map = {0: 'PAX_NO', 1: 'REPORTING_TIME', 2: 'EMPLOYEE_ID', 3: 'EMPLOYEE_NAME', 4: 'GENDER', 5: 'EMP_CATEGORY', 6: 'FLIGHT_NO.', 7: 'ADDRESS', 8: 'REPORTING_LOCATION', 9: 'LANDMARK', 10: 'PASSENGER_MOBILE'}

        df_h = df[is_header].rename(columns=h_map)
        df_p = df[is_passenger].rename(columns=p_map)
        
        if df_h.empty or df_p.empty:
            return pd.DataFrame()

        cols_h = [c for c in h_map.values() if c in df_h.columns] + ['Trip_ID']
        cols_p = [c for c in p_map.values() if c in df_p.columns] + ['Trip_ID']
        
        merged = pd.merge(df_p[cols_p], df_h[cols_h], on='Trip_ID', how='left')

        # Cleaning Logic
        if 'Trip_ID' in merged.columns:
            merged['TRIP_ID'] = merged['Trip_ID'].astype(str).str.replace('T', '', regex=False)
            merged = merged.drop(columns=['Trip_ID'])

        if 'AGENCY_NAME' in merged.columns:
            merged['AGENCY_NAME'] = merged['AGENCY_NAME'].apply(lambda x: "UNITED FACILITIES" if "UNITED FACILITIES" in str(x).upper() else x)

        if 'VEHICLE_NO' in merged.columns:
            merged['VEHICLE_NO'] = merged['VEHICLE_NO'].astype(str).str.replace('-', '', regex=False)

        if 'D_LOGIN' in merged.columns:
            login = merged['D_LOGIN'].astype(str).str.strip().str.split(' ', n=1, expand=True)
            if len(login.columns) > 0: merged['DIRECTION'] = login[0].str.upper().replace({'LOGIN': 'PICKUP', 'LOGOUT': 'DROP'})
            if len(login.columns) > 1: merged['SHIFT_TIME'] = login[1]

        # Explicit Removal
        cols_to_remove = ['PAX_NO', 'D_LOGIN', 'MARSHALL', 'DISTANCE', 'EMP_COUNT', 'TRIP_COUNT']
        merged = merged.drop(columns=cols_to_remove, errors='ignore')

        # Clean string columns
        for col in merged.select_dtypes(include=['object']):
            merged[col] = merged[col].astype(str).str.upper().str.strip()

        return merged
    except Exception as e:
        print(f"Error in _clean_single_raw_df: {e}")
        return pd.DataFrame()

def process_raw_data(file_list_bytes):
    all_dfs = []
    for filename, content in file_list_bytes:
        try:
            print(f"Processing file: {filename}")
            # Ensure we read as string to preserve IDs
            df_raw = pd.read_excel(io.BytesIO(content), header=None, dtype=str).dropna(how="all").reset_index(drop=True)
            cleaned = _clean_single_raw_df(df_raw)
            if not cleaned.empty: 
                all_dfs.append(cleaned)
        except Exception as e:
            print(f"FAILED processing file {filename}: {e}")
            continue

    if not all_dfs: 
        return None, None, None
        
    final_df = pd.concat(all_dfs, ignore_index=True)

    DB_MAP = {
        'TRIP_DATE': 'shift_date', 'TRIP_ID': 'trip_id', 'AGENCY_NAME': 'vendor', 
        'FLIGHT_NO.': 'flight_number', 'EMPLOYEE_ID': 'employee_id', 'EMPLOYEE_NAME': 'employee_name', 
        'GENDER': 'gender', 'EMP_CATEGORY': 'emp_category', 'ADDRESS': 'address', 
        'PASSENGER_MOBILE': 'passenger_mobile', 'LANDMARK': 'landmark', 'VEHICLE_NO': 'cab_reg_no',
        'DRIVER_NAME': 'driver_name', 'DRIVER_MOBILE': 'driver_mobile', 'DIRECTION': 'trip_direction',
        'SHIFT_TIME': 'shift_time', 'REPORTING_TIME': 'pickup_time', 'REPORTING_LOCATION': 'office'    
    }

    final_db = final_df.rename(columns=DB_MAP).fillna("")
    
    # Date/Time Formatting
    if 'shift_date' in final_db.columns:
        final_db['shift_date'] = pd.to_datetime(final_db['shift_date'], errors='coerce').dt.strftime('%d-%m-%Y')
        final_db['trip_date'] = final_db['shift_date']
    
    if 'shift_time' in final_db.columns:
        # format='mixed' handles various Excel time strings
        final_db['shift_time'] = pd.to_datetime(final_db['shift_time'], errors='coerce', format='mixed').dt.strftime('%H:%M')

    # 3. Final Step: Use imported helpers
    final_db = standardize_dataframe(final_db)
    return create_styled_excel(final_db, "Raw_Cleaned")


# ==========================================
# 4. BAROW DATA CLEANER
# ==========================================
def process_ba_row_data(file_content):
    try:
        print("🔹 Starting BA Row Data Processing...")
        
        # 1. READ CSV
        df = pd.read_csv(io.BytesIO(file_content), low_memory=False)
        print(f"🔹 CSV Loaded. Columns: {list(df.columns[:5])}...")

        df.columns = df.columns.str.strip()
        print(f"🔹 CSV Loaded. Found Columns: {list(df.columns)}")

        # 2. FILTER & TRANSFORM
        if "Trip Id" in df.columns:
            df["Trip Id"] = pd.to_numeric(df["Trip Id"], errors='coerce').fillna(0)
            df = df[df["Trip Id"] != 0]

        # Shift Time Logic
        if "Trip Type" in df.columns:
            df["Shift Time"] = df["Trip Type"].astype(str)
            mask_log = df["Shift Time"].str.contains("LOGIN|LOGOUT", case=False, na=False)
            df.loc[mask_log, "Shift Time"] = "00:00"
        else:
            df["Shift Time"] = "00:00"

        # Trip Direction
        if "Direction" in df.columns:
            df["Trip Direction"] = df["Direction"].str.upper().map({
                "LOGIN": "PICKUP", "LOGOUT": "DROP"
            })
        else:
            df["Trip Direction"] = ""

        # Safe Column Access for Pickup/Drop
        df["Pickup Time"] = df.get("Duty Start", "")
        df["Drop Time"] = df.get("Duty End", "")

        # Registration Cleaning
        if "Registration" in df.columns:
            df["Registration"] = (
                df["Registration"].astype(str)
                .str.replace("-", "", regex=False)
                .str.replace(" ", "", regex=False)
                .str.upper()
            )

        # Location Logic
        is_drop = df["Trip Direction"] == "DROP"
        is_pickup = df["Trip Direction"] == "PICKUP"
        
        start_addr = df.get("Start Location Address", "")
        end_addr = df.get("End Location Address", "")
        start_land = df.get("Start Location Landmark", "")
        end_land = df.get("End Location Landmark", "")

        df["Airport Name"] = np.where(is_drop, start_addr, end_addr)
        df["Address"] = np.where(is_pickup, start_addr, end_addr)
        df["Landmark"] = np.where(is_pickup, start_land, end_land)

        if "Leg Date" in df.columns:
            # Standard case
            df["Trip Date"] = df["Leg Date"].astype(str) + " " + df["Shift Time"].astype(str)
        elif "Date" in df.columns:
            # Fallback to 'Date' column
            df["Trip Date"] = df["Date"].astype(str) + " " + df["Shift Time"].astype(str)
        elif "Pickup Time" in df.columns:
            # Fallback: Extract Date from Duty Start (e.g., '2026-01-01 18:00:00')
            print("⚠️ 'Leg Date' missing. Extracting date from 'Pickup Time'.")
            df["Temp_Date"] = pd.to_datetime(df["Pickup Time"], errors='coerce').dt.strftime('%d-%m-%Y')
            df["Trip Date"] = df["Temp_Date"].astype(str) + " " + df["Shift Time"].astype(str)
            df["Leg Date"] = df["Temp_Date"] # Fill Leg Date so it's not empty in DB
        else:
            # Worst case: No date found
            print("❌ ERROR: Could not find 'Leg Date', 'Date', or 'Pickup Time'. Trip Date will be empty.")
            df["Leg Date"] = ""
            df["Trip Date"] = ""

        if "Trip Date" in df.columns:
            df["Shift Date"] = df["Trip Date"]
        else:
            df["Shift Date"] = ""

        df["In App/ Extra"] = "BA Row Data"
        df["BA REMARK"] = df.get("Trip Status", "")
        df["MiS Remark"] = df.get("Comments", "")

        # 3. PREPARE DATABASE MAPPING (Title Case -> snake_case)
        DB_MAP = {
            "Leg Date": "leg_date",
            "Trip Id": "trip_id",
            "Employee ID": "employee_id",
            "Gender": "gender",
            "EMP_CATEGORY": "emp_category",
            "Employee Name": "employee_name",
            "Shift Time": "shift_time",
            "Pickup Time": "pickup_time",
            "Drop Time": "drop_time",
            "Trip Direction": "trip_direction",
            "Registration": "cab_reg_no",
            "Cab Type": "cab_type",
            "Vendor": "vendor",
            "Office": "office",
            "Airport Name": "airport_name",
            "Landmark": "landmark",
            "Address": "address",
            "Flight Number": "flight_number",
            "Flight Category": "flight_category",
            "Flight Route": "flight_route",
            "Flight Type": "flight_type",
            "Trip Date": "trip_date",
            "MiS Remark": "mis_remark",
            "In App/ Extra": "in_app_extra",
            "Traveled Employee Count": "traveled_emp_count",
            "UNA2": "una2",
            "UNA": "una",
            "BA REMARK": "ba_remark",
            "Route Status": "route_status",
            "Clubbing Status": "clubbing_status",
            "GPS TIME": "gps_time",
            "GPS REMARK": "gps_remark",
            "Billing Zone Name": "billing_zone_name",
            "Leg Type": "leg_type",
            "Trip Source": "trip_source",
            "Trip Type": "trip_type",
            "Leg Start": "leg_start",
            "Leg End": "leg_end",
            "Audit Results": "audit_results",
            "Audit Done By": "audit_done_by",
            "Trip Audited": "trip_audited"
        }

        # 4. FIX: ENSURE ALL COLUMNS EXIST
        # This loop prevents the KeyError by creating missing columns
        for col in DB_MAP.keys():
            if col not in df.columns:
                df[col] = ""

        # 5. SELECT AND RENAME
        # Now it is safe to select because we guaranteed they exist
        df_final = df[list(DB_MAP.keys())].copy()
        df_final.rename(columns=DB_MAP, inplace=True)

        print(f"🔹 Data Transformed. Renamed columns to: {list(df_final.columns[:5])}...")
        print("🔹 Calling Standardizer...")

        # 6. STANDARDIZE
        if 'standardize_dataframe' in globals():
            df_final = standardize_dataframe(df_final)
            
            if df_final is None:
                print("❌ Standardization failed. Check if DB model matches these columns.")
                return None, None, None
        else:
            print("⚠️ 'standardize_dataframe' function not found. Skipping.")

        # 7. EXPORT
        print("🔹 Generating Excel...")
        return create_styled_excel(df_final, "BA_Row_Data_Cleaned")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"❌ BA Row Data Cleaner Error: {e}")
        return None, None, None


