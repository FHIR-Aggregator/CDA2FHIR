import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from cda2fhir.cdamodels import Base
from pathlib import Path
import importlib.resources
from sqlalchemy.engine import Engine
from sqlalchemy import event


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=OFF")
    cursor.execute("PRAGMA journal_mode=MEMORY")
    cursor.close()


DATABASE_PATH = str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'cda_data.db'))
DATABASE_URL = f'sqlite:////{DATABASE_PATH}'

engine = create_engine(DATABASE_URL, echo=True)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    data_dir = Path(DATABASE_PATH).parent
    data_dir.mkdir(parents=True, exist_ok=True)

    if os.path.exists(DATABASE_PATH):
        print("Existing database found. Deleting...")
        os.remove(DATABASE_PATH)

    print("Initializing database...")
    Base.metadata.create_all(bind=engine)


def init_db():
    if os.path.exists(DATABASE_PATH):
        print("Existing database found. Deleting...")
        os.remove(DATABASE_PATH)

    if not os.path.exists(DATABASE_PATH):
        print("database does not exist. initializing...")
        Base.metadata.create_all(bind=engine)

    else:
        print("skipping initialization - database exists. ")
