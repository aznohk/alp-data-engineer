# databaseConfig.py

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

load_dotenv()

# Define and create the engine and session factory at the module level
try:
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_NAME = os.getenv("DB_NAME")

    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
    # These objects are now defined globally in this file
    engine = create_engine(DATABASE_URL)
    Base = declarative_base()
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    
    print("SQLAlchemy database engine and session configured.")
    
except Exception as e:
    print(f"Error configuring database with SQLAlchemy: {e}")
    engine = None
    Base = None
    SessionLocal = None

def get_db_session():
    """Returns a new session instance from the global factory."""
    if SessionLocal:
        try:
            return SessionLocal()
        except Exception as e:
            print(f"Error creating a database session: {e}")
            return None
    return None