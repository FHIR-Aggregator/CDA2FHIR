from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .cdamodels import Base
from pathlib import Path
import importlib.resources

DATABASE_PATH = str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'cda_data.db'))
DATABASE_URL = f'sqlite:///{DATABASE_PATH}'

engine = create_engine(DATABASE_URL, echo=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db():
    """initialize database"""
    Base.metadata.create_all(bind=engine)
