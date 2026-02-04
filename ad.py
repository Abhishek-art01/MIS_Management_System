import pandas as pd

# ==========================================
# CONFIGURATION
# ==========================================
INPUT_FILE = "t3_address _locality.xlsx"
OUTPUT_FILE = "output_addresses_fixed.xlsx"

ADDRESS_COL = "address"
LOCALITY_COL = "locality"
KM_COL = "km"


# ==========================================
# FUNCTION: NORMALIZE LOCALITY BY MAX KM
# ==========================================
def normalize_locality_by_max_km(df):
    # Ensure KM is numeric
    df[KM_COL] = pd.to_numeric(df[KM_COL], errors="coerce")

    # Sort so highest KM comes first per address
    df_sorted = df.sort_values(
        by=[ADDRESS_COL, KM_COL],
        ascending=[True, False]
    )

    # Address → locality mapping (max KM)
    address_locality_map = (
        df_sorted
        .drop_duplicates(subset=ADDRESS_COL, keep="first")
        .set_index(ADDRESS_COL)[LOCALITY_COL]
    )

    # Apply corrected locality to all rows
    df_sorted[LOCALITY_COL] = df_sorted[ADDRESS_COL].map(address_locality_map)

    return df_sorted


# ==========================================
# MAIN EXECUTION
# ==========================================
def main():
    print("Reading input file...")
    df = pd.read_excel(INPUT_FILE)

    print(f"Total rows: {len(df)}")

    print("Normalizing locality based on highest KM...")
    df_cleaned = normalize_locality_by_max_km(df)

    # Validation: each address must have only one locality
    max_locality_count = (
        df_cleaned
        .groupby(ADDRESS_COL)[LOCALITY_COL]
        .nunique()
        .max()
    )

    if max_locality_count != 1:
        raise ValueError("Validation failed: Some addresses still have multiple localities")

    print("Validation passed: One locality per address")

    print("Saving output file...")
    df_cleaned.to_excel(OUTPUT_FILE, index=False)

    print(f"Done. Output saved as: {OUTPUT_FILE}")


# ==========================================
# ENTRY POINT
# ==========================================
if __name__ == "__main__":
    main()
