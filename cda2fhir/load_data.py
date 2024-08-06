import pandas as pd
import json
from pathlib import Path
import importlib.resources
from cda2fhir.database import engine
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy.engine.reflection import Inspector
from cda2fhir.database import init_db, SessionLocal
from cda2fhir.cdamodels import CDASubject, CDASubjectResearchSubject, CDAResearchSubject, CDADiagnosis, CDAResearchSubjectDiagnosis, \
    CDATreatment, CDAResearchSubjectTreatment, CDASubjectAlias, CDASubjectProject


def load_json_to_db(json_path, table_class, session, filter_species=None):
    """load json records and filter by CDA tags for human species - note: converted original data to list of dicts
    vs. dict of dicts (for json lint passing)"""

    with open(json_path, encoding='utf-8') as f:
        data = json.load(f)
        filter_species = {'Human', 'Homo sapiens'}
        for line in data:
            # allow for upstream filtering - only for subject with species in key
            if 'species' in line.keys() and filter_species and line.get("species") not in filter_species:
                continue
            try:
                session.add(table_class(**line))
            except IntegrityError:
                session.rollback()
                print(f"Skipping duplicate entry in {table_class.__tablename__}: {line}")
    session.commit()


def load_tsv_to_db(tsv_path, table_class, session):
    """load TSV files."""
    df = pd.read_csv(tsv_path, sep='\t')
    for row in df.to_dict(orient='records'):
        try:
            session.add(table_class(**row))
        except IntegrityError:
            session.rollback()
            print(f"Skipping duplicate entry in {table_class.__tablename__}: {row}")
    session.commit()


def clear_table(table_class, session: Session):
    """clear data from table."""
    session.query(table_class).delete()
    session.commit()


def table_exists(engine, table_name):
    """https://docs.sqlalchemy.org/en/20/core/reflection.html"""
    inspector = Inspector.from_engine(engine)
    return table_name in inspector.get_table_names()


def load_data():
    """load data into CDA models (call after initialization + change to DB load after CDA transition to DB)"""
    init_db()
    session = SessionLocal()

    # remove after final build
    clear_table(CDASubject, session)
    clear_table(CDAResearchSubject, session)
    clear_table(CDASubjectResearchSubject, session)
    clear_table(CDADiagnosis, session)
    clear_table(CDAResearchSubjectDiagnosis, session)
    clear_table(CDATreatment, session)
    clear_table(CDAResearchSubjectTreatment, session)
    clear_table(CDASubjectAlias, session)
    clear_table(CDASubjectProject, session)
    # clear_table(Specimen, session)
    # clear_table(ResearchSubjectSpecimen, session)

    try:
        # if not table_exists(engine, 'subject'): #TODO: add when relations and tables are defined
        load_json_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw' / 'subject.json')), CDASubject, session)
        # if not table_exists(engine, 'researchsubject'):
        load_json_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw' / 'researchsubject.json')), CDAResearchSubject, session)
        # if not table_exists(engine, 'subject_researchsubject'):
        load_tsv_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw' / 'association_tables' / 'subject_researchsubject.tsv')), CDASubjectResearchSubject, session)
        # if not table_exists(engine, 'diagnosis'):
        load_json_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw' / 'diagnosis.json')), CDADiagnosis, session)
        # if not table_exists(engine, 'treatment'):
        load_json_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw' / 'treatment.json')), CDATreatment, session)
        # if not table_exists(engine, 'researchsubject_diagnosis'):
        load_tsv_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw' / 'association_tables' / 'researchsubject_diagnosis.tsv')), CDAResearchSubjectDiagnosis, session)
        # if not table_exists(engine, 'researchsubject_treatment'):
        load_tsv_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw' / 'association_tables' / 'researchsubject_treatment.tsv')), CDAResearchSubjectTreatment, session)
        # if not table_exists(engine, 'subject_alias_table'):
        load_tsv_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw' / 'alias_files' / 'subject_integer_aliases.tsv')), CDASubjectAlias, session)
        # if not table_exists(engine, 'subject_project'):
        load_tsv_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw' / 'association_tables' / 'subject_associated_project.tsv')), CDASubjectProject, session)

        # load_json_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw' / 'specimen.json')), Specimen, session)
        # load_tsv_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw' /  'association_tables' / 'researchsubject_specimen.tsv')), ResearchSubjectSpecimen, session)
    finally:
        session.expire_all()
        session.close()
