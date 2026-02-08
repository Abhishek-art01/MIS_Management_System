import json
from typing import Generator
from sqlmodel import SQLModel, create_engine, Session
import pandas as pd
import os
import json
from sqlmodel import SQLModel, create_engine, Session

# --- 1. SETUP DATABASE URL ---


DATABASE_URL = os.environ.get("DATABASE_URL")

# If NOT on Render, try to load from local secrets.json
if not DATABASE_URL:
    try:
        with open("secrets.json") as f:
            all_secrets = json.load(f)
            # Switch this to "local" or "supabase" as needed for local testing
            secrets = all_secrets["supabase"] 
            
            DATABASE_URL = (
                f"postgresql://{secrets['DB_USER']}:{secrets['DB_PASSWORD']}"
                f"@{secrets['DB_HOST']}:{secrets.get('DB_PORT', 5432)}/{secrets['DB_NAME']}"
            )
    except FileNotFoundError:
        print("⚠️  WARNING: No secrets.json found and no DATABASE_URL set.")
        DATABASE_URL = "sqlite:///./test.db" # Fallback to a temporary local file

# --- 2. CREATE ENGINE ---
# Supabase/Postgres requires SSL. SQLite (fallback) does not.
connect_args = {}
if "postgresql" in DATABASE_URL:
    connect_args = {"sslmode": "require"}

engine = create_engine(DATABASE_URL, echo=False, pool_pre_ping=True, connect_args=connect_args)



# --- 3. HELPER FUNCTIONS ---

def create_db_and_tables():
    """
    Creates tables based on imported SQLModel classes.
    Call this from main.py lifespan/startup.
    """
    SQLModel.metadata.create_all(engine)

def get_session() -> Generator[Session, None, None]:
    """
    Dependency to yield a database session per request.
    """
    with Session(engine) as session:
        yield session

# In server/database.py

def bulk_save_client_data(df: pd.DataFrame):
    try:
        # 1. Validate DataFrame is not empty
        if df.empty:
            return {"status": "skipped", "message": "No data to save"}

        # 2. 🔥 THE FIX IS HERE: Select the column, then call unique()
        if "unique_id" not in df.columns:
            print("❌ Error: 'unique_id' column missing in DataFrame")
            return

        # Get list of unique_ids from the *DataFrame*
        new_ids = df["unique_id"].unique().tolist()

        with Session(engine) as session:
            # 3. Find which IDs already exist in DB to avoid duplicates
            statement = select(ClientData.unique_id).where(ClientData.unique_id.in_(new_ids))
            existing_ids = session.exec(statement).all()
            existing_set = set(existing_ids)

            # 4. Filter out existing records
            # We keep rows where unique_id is NOT in the database
            df_to_save = df[~df["unique_id"].isin(existing_set)]

            if df_to_save.empty:
                print("🔹 No new records to save.")
                return {"status": "success", "new_records": 0}

            # 5. Convert to Objects and Bulk Insert
            # to_dict('records') converts DF to list of dicts: [{'col': val}, ...]
            records = df_to_save.to_dict(orient='records')
            
            # Map dicts to the SQLModel class
            db_objects = [ClientData(**row) for row in records]

            session.add_all(db_objects)
            session.commit()
            
            count = len(db_objects)
            print(f"✅ Successfully saved {count} new Client Data records.")
            return {"status": "success", "new_records": count}

    except Exception as e:
        print(f"❌ Database Error in bulk_save_client_data: {e}")
        # Re-raise so the server logs the full traceback
        raise e