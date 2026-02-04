import os
import pandas as pd
from datetime import datetime

# =====================================================
# CONFIG
# =====================================================
INPUT_DIR = r"D:\my_projects\air-india-data\data-jan-2026\manual_operation_data\processed"
OUTPUT_FILE = r"D:\my_projects\air-india-data\data-jan-2026\manual_operation_data\TRG_PICKUP_ALL.xlsx"

# =====================================================
# HELPERS
# =====================================================
def clean_columns(columns):
    return (
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

def clean_address(series: pd.Series) -> pd.Series:
    return (
        series.fillna("")
        .astype(str)
        .str.replace("-", " ", regex=False)
        .str.replace(",", " ", regex=False)
        .str.replace("/", " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
        .str.upper()
    )

# =====================================================
# MAIN PROCESS
# =====================================================
all_dfs = []

for file_name in os.listdir(INPUT_DIR):
    if not file_name.lower().endswith(".xlsx"):
        continue

    file_path = os.path.join(INPUT_DIR, file_name)
    print(f"Processing: {file_name}")

    df = pd.read_excel(file_path)
    df = df.dropna(how="all")

    # Clean headers FIRST
    df.columns = clean_columns(df.columns)

    # Push header row as data
    df = pd.concat(
        [pd.DataFrame([df.columns.tolist()], columns=df.columns), df],
        ignore_index=True
    )

    # Extract date from filename
    date_str = file_name.split()[-1].replace(".xlsx", "")
    file_date = datetime.strptime(date_str, "%d-%m-%Y").date()
    df["date"] = file_date

    # Ensure required columns exist BEFORE usage
    required_cols = [
        "emp_name",
        "route_no",
        "employee_address_to_aita_sec_75_gurgaon"
    ]

    for col in required_cols:
        if col not in df.columns:
            df[col] = ""

    # Extract reporting location SAFELY
    mask = df["emp_name"] == "EMP NAME"

    df.loc[mask, "reporting_location"] = (
        df.loc[mask, "employee_address_to_aita_sec_75_gurgaon"]
        .astype(str)
        .str.extract(r'"([^"]+)"', expand=False)
    )

    df["reporting_location"] = df["reporting_location"].ffill()
    df["route_no"] = df["route_no"].ffill()

    df["direction"] = "Pickup"

    # Remove header rows
    df = df[df["emp_name"] != "EMP NAME"]

    # Rename columns
    column_mapping = {
        "route_no": "ROUTE NO",
        "trg_type": "TRG TYPE",
        "emp_id": "EMP ID",
        "emp_name": "EMP NAME",
        "employee_address_to_aita_sec_75_gurgaon": "EMPLOYEE ADDRESS",
        "contact_no": "CONTACT NO",
        "cab_no": "CAB NO",
        "pickup_time": "PICKUP TIME",
        "reporting_time": "REPORTING TIME",
        "remarks": "REMARK",
        "date": "DATE",
        "reporting_location": "REPORTING AREA",
        "direction": "DUTY TYPE"
    }

    df = df.rename(columns=column_mapping)

    # Final required columns
    final_columns = [
        "DATE", "DUTY TYPE", "ROUTE NO", "TRG TYPE", "EMP ID", "TEAM TYPE",
        "GENDER", "EMP NAME", "EMPLOYEE ADDRESS", "LOCATION", "CONTACT NO",
        "PICKUP TIME", "REPORTING TIME", "VENDOR", "REPORTING AREA",
        "CAB TYPE", "CAB NO", "CAB REG NO", "SHIFT TIME", "TRIP DATE", "ZONE",
        "GUARD", "BILLABLE COUNT", "ONE SIDE KM", "RATE", "GUARD RATE", "TOTAL",
        "REMARK", "GPS REMARK", "TOLL REMARK", "TOLL AMOUNT"
    ]

    for col in final_columns:
        if col not in df.columns:
            df[col] = ""

    df = df.reindex(columns=final_columns)

    # Uppercase headers and string values
    df.columns = df.columns.str.upper()
    df = df.applymap(lambda x: x.upper() if isinstance(x, str) else x)

    # Address cleanup
    df["EMPLOYEE ADDRESS"] = clean_address(df["EMPLOYEE ADDRESS"])

    all_dfs.append(df)

# =====================================================
# EXPORT
# =====================================================
final_df = pd.concat(all_dfs, ignore_index=True)

final_df.to_excel(
    OUTPUT_FILE,
    index=False,
    engine="openpyxl"
)

print("\n✅ All files processed successfully")
print(f"📁 Output file: {OUTPUT_FILE}")


