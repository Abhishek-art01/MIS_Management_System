# reset_db.py
from sqlmodel import SQLModel
from database import engine
import models  # This registers all your tables

def force_rebuild():
    print("🗑️  Dropping all existing tables...")
    SQLModel.metadata.drop_all(engine)
    
    print("🏗️  Creating all tables from models.py...")
    SQLModel.metadata.create_all(engine)
    
    print("✅ Database reset complete!")

if __name__ == "__main__":
    force_rebuild()