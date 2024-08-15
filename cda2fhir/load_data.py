import pandas as pd
import json
import mimetypes
from pathlib import Path
import importlib.resources
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy.engine.reflection import Inspector
from cda2fhir.database import init_db, SessionLocal
from cda2fhir.cdamodels import (CDASubject, CDASubjectResearchSubject, CDAResearchSubject, CDADiagnosis, \
    CDAResearchSubjectDiagnosis, CDATreatment, CDAResearchSubjectTreatment, CDASubjectAlias, CDASubjectProject, \
    CDASpecimen, CDASubjectIdentifier, CDAResearchSubjectSpecimen, ProjectdbGap, GDCProgramdbGap)


def load_to_db(path, table_class, session):
    result = mimetypes.guess_type(path, strict=False)[0]

    if 'json' in result:
        """load json records and filter by CDA tags for human species - note: converted original data to list of dicts
            vs. dict of dicts (for json lint passing)"""
        with open(path, encoding='utf-8') as f:
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
    else:
        if 'spreadsheetml.sheet' in result:
            df = pd.read_excel(path)
        elif 'csv' in result:
            df = pd.read_csv(path)
        elif 'tab-separated-values' in result:
            df = pd.read_csv(path, sep='\t')

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
    clear_table(CDASpecimen, session)
    clear_table(CDAResearchSubjectSpecimen, session)
    clear_table(ProjectdbGap, session)
    clear_table(GDCProgramdbGap, session)
    clear_table(CDASubjectIdentifier, session)


    try:
        # if not table_exists(engine, 'subject'): #TODO: add when relations and tables are defined
        load_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw' / 'subject.json')),
                        CDASubject, session)
        # if not table_exists(engine, 'researchsubject'):
        load_to_db(
            str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw' / 'researchsubject.json')),
            CDAResearchSubject, session)
        # if not table_exists(engine, ''):
        load_to_db(str(Path(importlib.resources.files(
            'cda2fhir').parent / 'data' / 'raw' / 'subject_identifier.json')),
                   CDASubjectIdentifier, session)
        # if not table_exists(engine, 'subject_researchsubject'):
        load_to_db(str(Path(importlib.resources.files(
            'cda2fhir').parent / 'data' / 'raw' / 'association_tables' / 'subject_researchsubject.tsv')),
                       CDASubjectResearchSubject, session)
        # if not table_exists(engine, 'diagnosis'):
        load_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw' / 'diagnosis.json')),
                        CDADiagnosis, session)
        # if not table_exists(engine, 'treatment'):
        load_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw' / 'treatment.json')),
                        CDATreatment, session)
        # if not table_exists(engine, 'researchsubject_diagnosis'):
        load_to_db(str(Path(importlib.resources.files(
            'cda2fhir').parent / 'data' / 'raw' / 'association_tables' / 'researchsubject_diagnosis.tsv')),
                       CDAResearchSubjectDiagnosis, session)
        # if not table_exists(engine, 'researchsubject_treatment'):
        load_to_db(str(Path(importlib.resources.files(
            'cda2fhir').parent / 'data' / 'raw' / 'association_tables' / 'researchsubject_treatment.tsv')),
                       CDAResearchSubjectTreatment, session)
        # if not table_exists(engine, 'subject_alias_table'):
        load_to_db(str(Path(importlib.resources.files(
            'cda2fhir').parent / 'data' / 'raw' / 'alias_files' / 'subject_integer_aliases.tsv')), CDASubjectAlias,
                       session)
        # if not table_exists(engine, 'subject_project'):
        load_to_db(str(Path(importlib.resources.files(
            'cda2fhir').parent / 'data' / 'raw' / 'association_tables' / 'subject_associated_project.tsv')),
                       CDASubjectProject, session)
        # if not table_exists(engine, 'specimen'):
        load_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw' / 'specimen.json')),
                        CDASpecimen, session)
        # if not table_exists(engine, 'researchsubject_specimen'):
        load_to_db(str(Path(importlib.resources.files(
            'cda2fhir').parent / 'data' / 'raw' / 'association_tables' / 'researchsubject_specimen.tsv')),
                       CDAResearchSubjectSpecimen, session)
        # if not table_exists(engine, ''):
        load_to_db(str(Path(importlib.resources.files(
            'cda2fhir').parent / 'data' / 'raw' / 'dbgap_to_project' / 'zz61_all_GDC_projects_fully_case-covered_by_dbgap_studies.xlsx')),
                       ProjectdbGap, session)
        # if not table_exists(engine, ''):
        load_to_db(str(Path(importlib.resources.files(
            'cda2fhir').parent / 'data' / 'raw' / 'dbgap_to_project' / 'zz63_all_GDC_programs_fully_case-covered_by_dbgap_studies.xlsx')),
                       GDCProgramdbGap, session)

    finally:
        session.expire_all()
        session.close()
