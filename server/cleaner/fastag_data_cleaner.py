import pandas as pd
import numpy as np
import pdfplumber
import io
import re
import xlrd
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
import traceback
from .cleaner_helper import create_styled_excel

# ==========================================
# HELPER: ICICI SPECIFIC CLEANER
# ==========================================
def _process_icici(pdf_obj):
    all_rows = []
    for page in pdf_obj.pages:
        tables = page.extract_tables()
        for table in tables:
            if table:
                all_rows.extend(table)
    
    if not all_rows: return pd.DataFrame()

    df = pd.DataFrame(all_rows)

    # 1. Find Header (Look for "Date & Time" or "Transaction Description")
    header_idx = -1
    for i in range(min(20, len(df))):
        row_str = " ".join([str(x).lower() for x in df.iloc[i] if x])
        if "date" in row_str and "description" in row_str:
            header_idx = i
            break
    
    if header_idx == -1: return pd.DataFrame()

    # 2. Set Header & Remove Empty Columns
    df.columns = df.iloc[header_idx]
    df = df.iloc[header_idx+1:].reset_index(drop=True)
    df.columns = [str(c).replace("\n", " ").strip() for c in df.columns]
    df = df.loc[:, df.columns != 'nan'] # Remove empty columns
    
    # 3. Rename to User's Desired Format
    col_map = {}
    for col in df.columns:
        c_low = col.lower()
        if "unique" in c_low and "id" in c_low:
            col_map[col] = "Unique Transaction ID"
        elif "date" in c_low:
            col_map[col] = "Travel Date Time"
        elif "activity" in c_low:
            col_map[col] = "Activity"
        elif "description" in c_low: 
            col_map[col] = "Plaza Name"  # User requested this mapping for ICICI
        elif "vehicle" in c_low:
            col_map[col] = "Vehicle No"
        elif "amount" in c_low and "dr" in c_low:
            col_map[col] = "Tag Dr/Cr"
        elif "plaza" in c_low and "id" in c_low:
            col_map[col] = "Plaza ID"

    df.rename(columns=col_map, inplace=True)
    return df


# ==========================================
# HELPER: CLEANING UTILS
# ==========================================
# ==========================================
# HELPER: CLEANING UTILS (Fixed)
# ==========================================
def clean_multiline_cells(df):
    """
    Fixes cells where text is split across lines (e.g. 'New\nDelhi' -> 'New Delhi').
    Collapses multiple spaces into one.
    """
    for col in df.columns:
        # Only clean string (object) columns
        if df[col].dtype == object:
            df[col] = (
                df[col].astype(str)
                .str.replace(r"[\n\t]", " ", regex=True) # Replace newline/tab with space
                .str.replace(r"\s+", " ", regex=True)    # Merge multiple spaces
                .str.strip()                             # Trim edges
            )
            # Restore true NaNs if we created "nan" strings
            df[col] = df[col].replace(["nan", "None", ""], np.nan)
    return df

def _clean_columns(columns):
    """Standardizes column names to snake_case (User's Logic)"""
    cleaned = (
        columns
        .astype(str)
        .str.replace(r"\n", " ", regex=True)        # remove line breaks
        .str.replace(r"\t", " ", regex=True)        # remove tabs
        .str.replace(r"\s+", " ", regex=True)       # normalize spaces
        .str.strip()                                # trim edges
        .str.lower()                                # lowercase
        .str.replace(r"[^\w\s]", "", regex=True)    # remove special chars
        .str.replace(" ", "_")                      # snake_case
    )
    return cleaned

def _clean_cell_value(x):
    """Normalizes spaces and handles None/NaN"""
    if isinstance(x, str):
        x = x.replace("\n", " ").replace("\t", " ")
        x = re.sub(r"\s+", " ", x).strip()
        if x.lower() in ["na", "n/a", "null", "none", ""]:
            return np.nan
        return x
    return x

# ==========================================
# HELPER: ICICI SPECIFIC CLEANER (YOUR PERFECT CODE)
# ==========================================
def _process_icici(pdf_obj):
    all_tables = []
    
    # 1. Extract Tables
    for page in pdf_obj.pages:
        tables = page.extract_tables()
        for table in tables:
            if table:
                all_tables.append(pd.DataFrame(table))
    
    if not all_tables: return pd.DataFrame()

    # 2. Merge
    df = pd.concat(all_tables, ignore_index=True)

    # 3. Hardcoded Drop (0-11) as requested
    # We check length first to avoid errors on empty files
    if len(df) > 12:
        df = df.drop(index=[0,1,2,3,4,5,6,7,8,9,10,11]).reset_index(drop=True)
    else:
        return pd.DataFrame()

    # 4. Set Header
    df.columns = df.iloc[0]
    df = df[1:].reset_index(drop=True) # Remove header row from data

    # 5. Clean Columns
    df.columns = _clean_columns(df.columns)
    df.columns = df.columns.str.strip()
    
    # Specific rename from your script
    df = df.rename(columns={"date__time": "date_time"})

    # 6. Extract Vehicle Number
    # Logic: First row, date_time column, split by space
    if not df.empty and "date_time" in df.columns:
        try:
            raw_val = str(df.loc[0, "date_time"])
            vehicle_no = raw_val.split(" ")[0]
            df["vehicle_no"] = vehicle_no
            
            # Remove that row used for extraction
            df = df.drop(index=[0]).reset_index(drop=True)
        except:
            df["vehicle_no"] = ""
    else:
        df["vehicle_no"] = ""

    # 7. Add Plaza ID placeholder
    df["plaza_id"] = ""

    # 8. Standardize Column Names (Replacements)
    replacements = {
        "drcr": "debit_credit",
        "rscr": "rupees_credit",
        "rsdr": "rupees_debit",
        "rs": "rupees",
        "amt": "amount",
        "bal": "balance"
    }
    for k, v in replacements.items():
        df.columns = df.columns.str.replace(k, v, regex=False)

    # 9. Drop unwanted columns
    df = df.drop(columns=["nan", "amount_rupees_credit"], errors="ignore")

    # 10. Final Rename Map
    rename_map = {
        "transaction_description": "plaza_name",
        "date_time": "travel_date_time",
        "vehicle_no": "vehicle_number",
        "amount_rupees_debit": "tag_debit_credit"
    }
    df = df.rename(columns=rename_map)

    # 11. Drop Empty Rows based on critical columns
    subset_cols = [
        "vehicle_number", "travel_date_time", "unique_transaction_id",
        "plaza_name", "activity", "tag_debit_credit"
    ]
    # Only drop if columns exist
    existing_subset = [c for c in subset_cols if c in df.columns]
    if existing_subset:
        df = df.dropna(subset=existing_subset)

    # 12. Final Column Selection
    final_columns = [
        "vehicle_number",
        "travel_date_time",
        "unique_transaction_id",
        "plaza_name",
        "plaza_id",
        "activity",
        "tag_debit_credit"
    ]
    
    # Add missing columns if any
    for col in final_columns:
        if col not in df.columns:
            df[col] = ""

    df = df[final_columns]

    # 13. Filter out repeated headers
    if "plaza_name" in df.columns:
        df = df[df["plaza_name"].astype(str).str.contains("transaction description", case=False, na=False) == False]

    df = clean_multiline_cells(df)


    # Map to Title Case for Final Output consistency
    final_title_map = {
        "vehicle_number": "Vehicle No",
        "travel_date_time": "Travel Date Time",
        "unique_transaction_id": "Unique Transaction ID",
        "plaza_name": "Plaza Name",
        "plaza_id": "Plaza ID",
        "activity": "Activity",
        "tag_debit_credit": "Tag Dr/Cr"
    }
    df.rename(columns=final_title_map, inplace=True)

    return df

# ==========================================
# HELPER: IDFC SPECIFIC CLEANER (YOUR PREVIOUS PERFECT CODE)
# ==========================================
def _clean_columns(columns):
    """Standardizes column names to snake_case"""
    cleaned = (
        columns
        .astype(str)
        .str.replace(r"\n", " ", regex=True)
        .str.replace(r"\t", " ", regex=True)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
        .str.lower()
        .str.replace(r"[^\w\s]", "", regex=True)
        .str.replace(" ", "_")
    )
    return cleaned

def _clean_cell_value(x):
    """Normalizes spaces and handles None/NaN"""
    if isinstance(x, str):
        x = x.replace("\n", " ").replace("\t", " ")
        x = re.sub(r"\s+", " ", x).strip()
        if x.lower() in ["na", "n/a", "null", "none", ""]:
            return np.nan
        return x
    return x

def _clean_datetime(x):
    """Fixes broken years (2 025) and time spacing"""
    if not isinstance(x, str):
        return x
    x = re.sub(r"\s+", " ", x).strip()
    # Fix broken year (2 025 -> 2025)
    x = re.sub(r"(\d{2})-(\d)\s(\d{3})", r"\1-\2\3", x)
    # Fix time spacing (23:3 2:46 -> 23:32:46)
    x = re.sub(r"(\d{2}):(\d)\s(\d):(\d{2})", r"\1:\2\3:\4", x)
    return x

def _clean_reference_id(x):
    if not isinstance(x, str):
        return x
    return x.replace(" ", "")

def _clean_vehicle_no(x):
    if isinstance(x, str):
        return x.replace(" ", "").strip()
    return x

# ==========================================
# HELPER: IDFC SPECIFIC CLEANER
# ==========================================
def _process_idfc(pdf_obj):
    all_tables = []
    for page in pdf_obj.pages:
        tables = page.extract_tables()
        for table in tables:
            if table:
                all_tables.append(pd.DataFrame(table))
    
    if not all_tables:
        print("⚠️ IDFC: No tables found.")
        return pd.DataFrame()

    df = pd.concat(all_tables, ignore_index=True)

    # 1. Drop known junk rows
    if len(df) > 5:
        df = df.drop(index=[0,1,2,3,4]).reset_index(drop=True)
    else:
        return pd.DataFrame()

    # 2. Set Headers
    df.columns = df.iloc[0]
    df = df[1:].reset_index(drop=True)
    df.columns = _clean_columns(df.columns)

    # 3. Clean Headers & Values
    cols_to_drop = ["processed_date_time", "pool_drcr", "closing_pool_balance_rs", "closing_tag_balance_rs"]
    df = df.drop(columns=cols_to_drop, errors="ignore")

    replacements = {"drcr": "debit_credit", "rs": "rupees", "amt": "amount", "bal": "balance"}
    for k, v in replacements.items():
        df.columns = df.columns.str.replace(k, v, regex=False)

    for col in df.columns:
        df[col] = df[col].apply(_clean_cell_value)

    # 4. 🔥 REPAIR SPLIT ROWS (Merging wrapped IDs)
    if "travel_date_time" in df.columns and "unique_transaction_id" in df.columns:
        rows_to_drop = []
        for i in range(1, len(df)):
            curr_date = str(df.loc[i, "travel_date_time"])
            curr_id_frag = str(df.loc[i, "unique_transaction_id"])
            
            is_invalid_date = (curr_date == "" or curr_date.lower() == "nan" or "nan" in curr_date.lower())
            has_fragment = (curr_id_frag != "" and curr_id_frag.lower() != "nan")
            
            if is_invalid_date and has_fragment:
                prev_idx = i - 1
                while prev_idx in rows_to_drop and prev_idx > 0:
                    prev_idx -= 1
                
                if prev_idx >= 0:
                    current_val = str(df.loc[prev_idx, "unique_transaction_id"])
                    if "HR" not in curr_id_frag and "DL" not in curr_id_frag: 
                         df.at[prev_idx, "unique_transaction_id"] = current_val + curr_id_frag
                         rows_to_drop.append(i)

        if rows_to_drop:
            df = df.drop(rows_to_drop).reset_index(drop=True)

    # 5. Extract Vehicle No from Header Rows
    if "travel_date_time" in df.columns:
        if "vehicle_number" not in df.columns:
            df["vehicle_number"] = None

        current_vehicle = None
        rows_to_drop = []

        for idx, row in df.iterrows():
            val = str(row["travel_date_time"]).strip()
            match = re.search(r'([A-Z]{2}[0-9]{1,2}[A-Z]{0,3}[0-9]{4})', val.replace(" ", ""))
            is_date = re.search(r'\d{2}-\d{2}-\d{4}', val)
            
            if match and not is_date:
                current_vehicle = match.group(1)
                rows_to_drop.append(idx)
            else:
                existing_veh = str(row.get("vehicle_number", "")).strip()
                if current_vehicle and (existing_veh == "" or existing_veh.lower() == "nan"):
                    df.at[idx, "vehicle_number"] = current_vehicle

        if rows_to_drop:
            df = df.drop(rows_to_drop).reset_index(drop=True)

    # 6. Cleaning Helpers
    def _clean_vehicle_no(x):
        return x.replace(" ", "").strip() if isinstance(x, str) else x

    def _clean_datetime(x):
        if not isinstance(x, str): return x
        x = re.sub(r"\s+", " ", x).strip()
        x = re.sub(r"(\d{2})-(\d)\s(\d{3})", r"\1-\2\3", x)
        x = re.sub(r"(\d{2}):(\d)\s(\d):(\d{2})", r"\1:\2\3:\4", x)
        return x

    def _clean_reference_id(x):
        return x.replace(" ", "") if isinstance(x, str) else x

    if "vehicle_number" in df.columns:
        df["vehicle_number"] = df["vehicle_number"].apply(_clean_vehicle_no)
    
    if "travel_date_time" in df.columns:
        df["travel_date_time"] = df["travel_date_time"].apply(_clean_datetime)
    
    if "unique_transaction_id" in df.columns:
        df["unique_transaction_id"] = df["unique_transaction_id"].apply(_clean_reference_id)
        def safe_convert(x):
            try:
                if pd.isna(x): return ""
                x_str = str(x).strip()
                if x_str.replace('.', '', 1).isdigit():
                    return format(float(x_str), ".0f")
                return x_str
            except:
                return str(x)
        df["unique_transaction_id"] = df["unique_transaction_id"].apply(safe_convert)

    if "activity" in df.columns:
        df["activity"] = df["activity"].astype(str).str.strip()
        df = df[~df["activity"].str.lower().isin(["recharge", "", "nan", "none"])]

    df = clean_multiline_cells(df)


    # ----------------------------------------------------------------------
    # 🔥 FIX: SMART COLUMN MAPPING (CATCHES PLAZA NAME & ID)
    # ----------------------------------------------------------------------
    final_map = {}
    
    for col in df.columns:
        c = col.lower()
        
        if "vehicle" in c:
            final_map[col] = "Vehicle No"
        elif "date" in c and "time" in c:
            final_map[col] = "Travel Date Time"
        elif "unique" in c or ("transaction" in c and "id" in c):
            final_map[col] = "Unique Transaction ID"
        elif "activity" in c:
            final_map[col] = "Activity"
        elif "debit" in c or "amount" in c:
            final_map[col] = "Tag Dr/Cr"
            
        # --- Plaza Logic ---
        # 1. Plaza ID: Look for "plaza" + "id" OR "lane" + "id"
        elif ("plaza" in c and "id" in c) or ("lane" in c and "id" in c):
             final_map[col] = "Plaza ID"
             
        # 2. Plaza Name: Look for "plaza" (without ID) OR "description" OR "toll"
        elif "plaza" in c or "description" in c or "toll" in c:
             # Ensure we don't accidentally map the ID column if logic overlapped
             if "id" not in c:
                 final_map[col] = "Plaza Name"

    df.rename(columns=final_map, inplace=True)

    return df

# ==========================================
# HELPER: IDFCB SPECIFIC CLEANER (Variable Method)
# ==========================================
def _process_idfcb(pdf_obj):
    """
    Cleaner for 'IDFCB' variant.
    Extracts Vehicle No into a variable and applies it to the final dataframe.
    """
    all_tables = []
    for page in pdf_obj.pages:
        tables = page.extract_tables()
        for table in tables:
            if table:
                all_tables.append(pd.DataFrame(table))
    
    if not all_tables: 
        return pd.DataFrame()

    df = pd.concat(all_tables, ignore_index=True)

    # 1. REMOVE EMPTY ROWS
    df = df.dropna(how="all").reset_index(drop=True)

    # 2. EXTRACT VEHICLE NUMBER (Save to variable)
    # We grab it from (Row 1, Col 3) before doing any drops
    vehicle_val = ""
    try:
        if df.shape[0] > 1 and df.shape[1] > 3:
            raw_val = str(df.iat[1, 3])
            if raw_val and raw_val.lower() != 'nan':
                vehicle_val = raw_val.replace("\n", "").replace(" ", "").strip()
                print(f"   ✅ IDFCB Vehicle Found: {vehicle_val}")
    except Exception as e:
        print(f"   ⚠️ IDFCB Vehicle Extraction Failed: {e}")

    # 3. DROP HEADER JUNK (Rows 0-4)
    if len(df) > 5:
        df = df.drop(index=[0,1,2,3,4]).reset_index(drop=True)
    else:
        return pd.DataFrame()

    # 4. SET HEADER (Row 0 is now the header)
    df.columns = df.iloc[0]
    df = df[1:].reset_index(drop=True) # Remove header row from data

    # 5. CLEAN COLUMNS
    df.columns = _clean_columns(df.columns) 
    
    # 6. RENAME COLUMNS (Snake Case)
    rename_map = {
        "reader_date_time": "travel_date_time",
        "debit": "tag_debit_credit",
        "activity": "activity",
        "description": "plaza_name", 
        "transaction_description": "plaza_name",
        "sequence_no": "unique_transaction_id", 
        "urn": "unique_transaction_id" 
    }
    
    final_rename = {}
    for col in df.columns:
        for k, v in rename_map.items():
            if k in col.lower():
                final_rename[col] = v
    df.rename(columns=final_rename, inplace=True)

    # 7. STANDARDIZE VALUES
    cols_to_clean = [c for c in df.columns if "amount" in c or "balance" in c or "debit" in c]
    for c in cols_to_clean:
        df[c] = df[c].astype(str).str.replace("Dr", "", regex=False)\
                                 .str.replace("Cr", "", regex=False)\
                                 .str.replace(",", "", regex=False)\
                                 .str.strip() 

    # 8. FILTER JUNK
    if "activity" in df.columns:
        # Normalize text first
        df["activity"] = df["activity"].astype(str).str.strip()
        
        # Create a lowercase version for checking
        act_lower = df["activity"].str.lower()
        
        # Filter Logic:
        # 1. Contains "recharge" (covers UPI, BBPS, CCAVENUE, etc.)
        # 2. Contains "rec harge" (covers the broken PDF text you saw)
        # 3. Exact matches for "none" or "nan"
        mask_junk = (
            act_lower.str.contains("recharge", na=False) | 
            act_lower.str.contains("ccavenue", na=False) | 
            act_lower.str.contains("rec harge", na=False) |
            act_lower.isin(["none", "nan", ""])
        )
        
        # Keep rows that are NOT junk
        df = df[~mask_junk]

    df = clean_multiline_cells(df)


    # 9. FINAL RENAME TO TITLE CASE (For Main Processor)
    final_title_map = {
        "travel_date_time": "Travel Date Time",
        "unique_transaction_id": "Unique Transaction ID",
        "plaza_name": "Plaza Name",
        "plaza_id": "Plaza ID",
        "activity": "Activity",
        "tag_debit_credit": "Tag Dr/Cr"
    }
    df.rename(columns=final_title_map, inplace=True)

    # 10. ASSIGN VEHICLE NUMBER (The Fix)
    # We assign it here to the final column name directly.
    # This guarantees the column exists and is filled.
    df["Vehicle No"] = vehicle_val
    df = df[df.isna().sum(axis=1) <= 2]


    return df

# ==========================================
# HELPER: INDUS SPECIFIC CLEANER (New)
# ==========================================
def _process_indus(pdf_obj):
    """
    Cleaner for 'INDUS' variant.
    Handles split rows for AM/PM dates and multi-line plaza names.
    """
    all_tables = []
    for page in pdf_obj.pages:
        tables = page.extract_tables()
        for table in tables:
            if table:
                all_tables.append(pd.DataFrame(table))
    
    if not all_tables: 
        return pd.DataFrame()

    df = pd.concat(all_tables, ignore_index=True)

    # 1. Remove fully empty rows
    df = df.dropna(how="all")

    # 2. Set Header (Row 0)
    df.columns = df.iloc[0]
    df = df[1:].reset_index(drop=True)

    # 3. Clean Column Names
    def clean_columns(columns):
        return (
            columns.astype(str)
            .str.replace(r"\n", " ", regex=True)
            .str.replace(r"\t", " ", regex=True)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
            .str.lower()
            .str.replace(r"[^\w\s]", "", regex=True)
            .str.replace(" ", "_")
        )

    df.columns = clean_columns(df.columns)
    df.columns = df.columns.str.strip().str.lower()

    # 4. Rename Columns
    rename_map = {
        "transaction_datetime": "travel_date_time",
        "transactiondtstamp": "unique_transaction_id",
        "type": "activity",
        "description": "plaza_name",
        "debit": "tag_debit_credit",
        "vehiclenumber": "vehicle_number",
    }
    df = df.rename(columns=rename_map)

    # 5. Drop Unwanted Columns
    df = df.drop(columns=["credit", "balance"], errors="ignore")

    # 6. Add Missing Columns
    if "plaza_id" not in df.columns:
        df["plaza_id"] = ""

    # 7. Select Final Columns
    final_cols = [
        "vehicle_number", "travel_date_time", "unique_transaction_id",
        "plaza_name", "plaza_id", "activity", "tag_debit_credit"
    ]
    # Keep only what exists for now
    df = df[[c for c in final_cols if c in df.columns]]

    # 8. Filter Junk
    if "activity" in df.columns:
        df["activity"] = df["activity"].astype(str).str.strip()
        df = df[~df["activity"].str.lower().isin(["recharge", "type", "none", "nan"])]

    # 9. ROW MERGING LOGIC (Your Core Logic)
    df = df.replace(r"^\s*$", np.nan, regex=True)
    df = df.reset_index(drop=True)

    # A) Merge AM/PM split rows
    if "travel_date_time" in df.columns:
        for i in range(1, len(df)):
            val = str(df.at[i, "travel_date_time"]) if pd.notna(df.at[i, "travel_date_time"]) else ""
            if val.lower() in ["am", "pm"]:
                # Append to previous row
                prev_val = str(df.at[i - 1, "travel_date_time"]) if pd.notna(df.at[i - 1, "travel_date_time"]) else ""
                df.at[i - 1, "travel_date_time"] = (prev_val + " " + val).strip()
                # Clear current row
                df.at[i, "travel_date_time"] = np.nan

    # B) Merge Split Plaza Names
    important_cols = ["vehicle_number", "travel_date_time", "unique_transaction_id", "activity", "tag_debit_credit"]
    existing_important = [c for c in important_cols if c in df.columns]
    
    if "plaza_name" in df.columns and existing_important:
        for i in range(1, len(df)):
            # If current row has plaza name but other important cols are empty -> it's a spillover
            if pd.notna(df.at[i, "plaza_name"]) and df.loc[i, existing_important].isna().all():
                # Merge upward
                prev_plaza = str(df.at[i - 1, "plaza_name"]) if pd.notna(df.at[i - 1, "plaza_name"]) else ""
                curr_plaza = str(df.at[i, "plaza_name"])
                df.at[i - 1, "plaza_name"] = (prev_plaza + " " + curr_plaza).strip()
                df.at[i, "plaza_name"] = np.nan

    # 10. Final Cleanup
    df = df.dropna(how="all").reset_index(drop=True)

    # 11. Format Date
    if "travel_date_time" in df.columns:
        df["travel_date_time"] = (
            df["travel_date_time"]
            .astype(str)
            .str.replace(r"\s*\n\s*", " ", regex=True)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )
        # Convert to datetime string format for consistency
        # We don't convert to object here, we keep it string until final export
        # But let's standardize format if possible
        df["travel_date_time"] = pd.to_datetime(df["travel_date_time"], dayfirst=True, errors="coerce")

    # 12. Format Plaza Name
    if "plaza_name" in df.columns:
        df["plaza_name"] = (
            df["plaza_name"]
            .astype(str)
            .str.replace(r"\s*\n\s*", " ", regex=True)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )

    # 13. Rename to Title Case (For Main Processor)
    final_title_map = {
        "vehicle_number": "Vehicle No",
        "travel_date_time": "Travel Date Time",
        "unique_transaction_id": "Unique Transaction ID",
        "plaza_name": "Plaza Name",
        "plaza_id": "Plaza ID",
        "activity": "Activity",
        "tag_debit_credit": "Tag Dr/Cr"
    }
    df.rename(columns=final_title_map, inplace=True)

    return df
# ==========================================
# HELPER: SBI SPECIFIC CLEANER (Fixed Date Join)
# ==========================================



# ==========================================
# 4. MAIN FASTAG DATA CLEANER (PDF) - UPDATED
# ==========================================
def process_fastag_data(file_data_list):
    """
    file_data_list: List of tuples -> [(filename, bytes), (filename, bytes)]
    """
    try:
        print(f"🔹 Starting Fastag Processing for {len(file_data_list)} files...")
        
        processed_dfs = []

        for filename, content in file_data_list:
            try:
                fname_lower = filename.lower()
                
                with pdfplumber.open(io.BytesIO(content)) as pdf:
                    df_temp = None
                    
                    if "idfc.pdf" in fname_lower:
                        print(f"🔹 File '{filename}' -> Detected IDFC Logic")
                        df_temp = _process_idfc(pdf)
                    
                    elif "icici.pdf" in fname_lower:
                        print(f"🔹 File '{filename}' -> Detected ICICI Logic")
                        df_temp = _process_icici(pdf)
                    
                    elif "idfcb.pdf" in fname_lower:
                        print(f"🔹 File '{filename}' -> Detected IDFCB Logic")
                        df_temp = _process_idfcb(pdf)
                    
                    elif "indus.pdf" in fname_lower:
                        print(f"🔹 File '{filename}' -> Detected INDUS Logic")
                        df_temp = _process_indus(pdf)

                    elif "sbi.pdf" in fname_lower: # <--- Added SBI
                        print(f"🔹 File '{filename}' -> Detected SBI Logic")
                        df_temp = _process_sbi(pdf)
                    
                    else:
                        print(f"⚠️ File '{filename}' -> No Bank Name found. Defaulting to ICICI.")
                        df_temp = _process_icici(pdf) 
                    

                    if df_temp is not None and not df_temp.empty:
                        processed_dfs.append(df_temp)

            except Exception as e:
                print(f"⚠️ Error reading file {filename}: {e}")
                continue

        
        if not processed_dfs:
            print("❌ No valid data extracted.")
            return None, None, None

        # 🔥 FIX: Ensure no duplicate columns in any DF before concat
        cleaned_dfs = []
        for d in processed_dfs:
            # Remove duplicate columns
            d = d.loc[:, ~d.columns.duplicated()]
            # Ensure index is unique
            d = d.reset_index(drop=True)
            cleaned_dfs.append(d)

        # Merge
        final_df = pd.concat(cleaned_dfs, ignore_index=True)

        # Enforce Columns
        desired_columns = [
            "Vehicle No", "Travel Date Time", "Unique Transaction ID", 
            "Activity", "Plaza Name", "Tag Dr/Cr" 
        ]

        for col in desired_columns:
            if col not in final_df.columns:
                final_df[col] = ""

        final_df = final_df[desired_columns]

        # --- 1. CLEAN TRANSACTION ID (TRIM + REMOVE NEWLINES) ---
        if "Unique Transaction ID" in final_df.columns:
            final_df["Unique Transaction ID"] = (
                final_df["Unique Transaction ID"]
                .astype(str)
                .str.replace("\n", "", regex=False)
                .str.replace(" ", "", regex=False)
                .str.strip()
            )
            
            # Remove rows where Transaction ID is empty/nan
            final_df = final_df[
                (final_df["Unique Transaction ID"] != "") & 
                (final_df["Unique Transaction ID"].str.lower() != "nan")
            ]

        # --- 2. CLEAN DATE (Format: 03/02/2026 & Standard) ---
        if "Travel Date Time" in final_df.columns:
            # Clean junk text
            mask = final_df["Travel Date Time"].astype(str).str.lower().str.contains("date|total|page", na=False)
            final_df = final_df[~mask]

            # 🔥 FIX: Handle slash dates (03/02/2026) by replacing / with -
            # This helps pandas parser recognize them correctly as dates
            final_df["Travel Date Time"] = final_df["Travel Date Time"].astype(str).str.replace("/", "-", regex=False)

            # Convert to Datetime
            final_df["Travel Date Time"] = pd.to_datetime(
                final_df["Travel Date Time"], 
                dayfirst=True, 
                errors='coerce'
            )
            
            # Format consistently
            final_df["Travel Date Time"] = final_df["Travel Date Time"].dt.strftime('%d-%m-%Y %H:%M:%S')

        # --- 3. CLEAN VEHICLE NO ---
        final_df["Vehicle No"] = (
            final_df["Vehicle No"].astype(str)
            .str.replace(" ", "", regex=False)
            .str.replace("-", "", regex=False)
            .str.upper()
            .replace("NAN", "")
            .replace("NONE", "")
        )

        # --- 4. CLEAN AMOUNT (Fix Negatives) ---
        final_df = final_df.rename(columns={"Tag Dr/Cr": "Amount"})

        final_df["Amount"] = pd.to_numeric(
            final_df["Amount"].astype(str).str.replace(",", ""), errors='coerce'
        ).fillna(0)

        # Convert to Absolute Value
        final_df["Amount"] = final_df["Amount"].abs()

        final_df = final_df.fillna("")

        print(f"🔹 Processing complete. Final shape: {final_df.shape}")
        
        return create_styled_excel(final_df, "Fastag_Cleaned")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"❌ Fastag Cleaner Error: {e}")
        return None, None, None