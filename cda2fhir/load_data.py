import pandas as pd
import json
import os
import glob
import mimetypes
from pathlib import Path
import importlib.resources
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy.engine.reflection import Inspector
from cda2fhir.database import init_db, SessionLocal
from cda2fhir.cdamodels import (CDASubject, CDASubjectResearchSubject, CDAResearchSubject, CDADiagnosis,
                                CDAResearchSubjectDiagnosis, CDATreatment, CDAResearchSubjectTreatment, CDASubjectAlias,
                                CDASubjectProject,  CDASpecimen, CDASubjectIdentifier, CDAResearchSubjectSpecimen,
                                ProjectdbGap, GDCProgramdbGap, CDAProjectRelation, CDAFile, CDAFileSpecimen, CDAFileSubject,
                                CDAMutation, CDASubjectMutation)


def file_size(file_path):
    return os.path.getsize(file_path) / (1024 ** 3)  #size in GB


def load_to_db(paths, table_class, session):
    """load data from single file or list of files (JSON, CSV, Excel, TSV) paths into the database."""
    if isinstance(paths, str):
        paths = [paths]

    for path in paths:
        print("####### Processing PATH: ", path)
        result = mimetypes.guess_type(path, strict=False)[0]

        if 'json' in result:
            """load json records and filter by CDA tags for human species - note: converted original data to list of 
               dicts vs. dict of dicts (for json lint passing)"""
            with open(path, encoding='utf-8') as f:
                data = json.load(f)
                filter_species = {'Human', 'Homo sapiens'}
                for line in data:
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

        session.flush() # flush changes to the database
        session.commit()


def load_to_db_chunked(path, table_class, session, chunk_size=1000):
    """try chunk loading"""
    print(f"####### Processing PATH: {path}")
    filter_species = {'Human', 'Homo sapiens'}
    result = mimetypes.guess_type(path, strict=False)[0]

    try:
        if 'json' in result:
            with open(path, encoding='utf-8') as f:
                for i, line in enumerate(f, start=1):
                    if not line.strip():
                        continue

                    if i % chunk_size == 0:
                        session.commit()
                        session.expire_all()

                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError as e:
                        print(f"Skipping malformed JSON on line {i}: {e}")
                        continue

                    if 'species' in record and record.get("species") not in filter_species:
                        continue

                    try:
                        session.add(table_class(**record))
                    except IntegrityError:
                        session.rollback()
                        print(f"Skipping duplicate entry in {table_class.__tablename__}: {record}")

            session.flush()
            session.commit() # commit in chunck size
            session.expire_all() # should have been called by commit (just to make it more explicit)

        # small relational files
        elif 'spreadsheetml.sheet' in result:
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
        else:
            print(f"Unsupported file type for PATH: {path}")

    except Exception as e:
        print(f"Error processing PATH {path}: {e}")
        session.rollback()
    # commit should flush beforehand changes https://docs.sqlalchemy.org/en/20/orm/session_api.html#sqlalchemy.orm.Session.commit,
    # to be safe but
    session.flush() # flush changes to the database
    session.commit()  # final commit
    session.expire_all()  # final cleanup


def clear_table(table_class, session: Session):
    """clear data from table."""
    session.query(table_class).delete()
    session.commit()


def table_exists(engine, table_name):
    """https://docs.sqlalchemy.org/en/20/core/reflection.html"""
    inspector = Inspector.from_engine(engine)
    return table_name in inspector.get_table_names()


def load_data(transform_condition, transform_files, transform_treatment, transform_mutation):
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

        load_to_db(
            str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw' / 'researchsubject.json')),
            CDAResearchSubject, session)

        load_to_db(str(Path(importlib.resources.files(
            'cda2fhir').parent / 'data' / 'raw' / 'subject_identifier.json')),
                   CDASubjectIdentifier, session)

        load_to_db(str(Path(importlib.resources.files(
            'cda2fhir').parent / 'data' / 'raw' / 'association_tables' / 'subject_researchsubject.tsv')),
                   CDASubjectResearchSubject, session)
        if transform_condition:

            load_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw' / 'diagnosis.json')),
                       CDADiagnosis, session)
        if transform_treatment:

            load_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw' / 'treatment.json')),
                       CDATreatment, session)

        load_to_db(str(Path(importlib.resources.files(
            'cda2fhir').parent / 'data' / 'raw' / 'association_tables' / 'researchsubject_diagnosis.tsv')),
                   CDAResearchSubjectDiagnosis, session)

        load_to_db(str(Path(importlib.resources.files(
            'cda2fhir').parent / 'data' / 'raw' / 'association_tables' / 'researchsubject_treatment.tsv')),
                   CDAResearchSubjectTreatment, session)

        load_to_db(str(Path(importlib.resources.files(
            'cda2fhir').parent / 'data' / 'raw' / 'alias_files' / 'subject_integer_aliases.tsv')), CDASubjectAlias,
                   session)

        load_to_db(str(Path(importlib.resources.files(
            'cda2fhir').parent / 'data' / 'raw' / 'association_tables' / 'subject_associated_project.tsv')),
                   CDASubjectProject, session)
        if not transform_mutation and not transform_condition and not transform_files:

            load_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw' / 'specimen.json')),
                       CDASpecimen, session)

        load_to_db(str(Path(importlib.resources.files(
            'cda2fhir').parent / 'data' / 'raw' / 'association_tables' / 'researchsubject_specimen.tsv')),
                   CDAResearchSubjectSpecimen, session)

        load_to_db(str(Path(importlib.resources.files(
            'cda2fhir').parent / 'data' / 'raw' / 'dbgap_to_project' / 'zz61_all_GDC_projects_fully_case-covered_by_dbgap_studies.xlsx')),
                   ProjectdbGap, session)

        load_to_db(str(Path(importlib.resources.files(
            'cda2fhir').parent / 'data' / 'raw' / 'dbgap_to_project' / 'zz63_all_GDC_programs_fully_case-covered_by_dbgap_studies.xlsx')),
                   GDCProgramdbGap, session)

        load_to_db(str(Path(importlib.resources.files(
            'cda2fhir').parent / 'data' / 'raw' / 'Identifier_maps' / 'project_program_relation_summary_crdc.csv')),
                   CDAProjectRelation, session)

        #load_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw' / 'project_mutations'/'TARGET-WT_mutations.json')), CDAMutation, session)

        if transform_mutation:
            # load_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw' / 'project_mutations'/ 'CGCI-BLGSP_mutations.json')), CDAMutation, session)
            load_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw' / 'mutation.json')), CDAMutation, session)

            # mutation_folder_path = str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw' / 'project_mutations'))
            # mutation_file_paths = glob.glob(os.path.join(mutation_folder_path, '*'))
            # print("~~~ Globbed mutation files: ", mutation_file_paths)
            #
            # load_to_db(mutation_file_paths, CDAMutation, session)
            #
            load_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw' / 'subject_mutation.json')),
                        CDASubjectMutation, session)

        if transform_files:
            # TODO: can use chunck loading instead of file divvy-up
            load_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw' / 'file.0000.json')), CDAFile, session)

            # folder_path = str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw' / 'files_converted'))
            # file_paths = glob.glob(os.path.join(folder_path, '*'))
            # print("Globbed: ", file_paths)
            #
            # load_to_db(file_paths, CDAFile, session)

            # large file - can be useful to reduce the relations files by CDA project as well
            # file alias -> project  join by file id with file_subject

            # load_to_db(str(Path(importlib.resources.files(
            #     'cda2fhir').parent / 'data' / 'raw' / 'association_tables' / 'file_subject.tsv')),
            #            CDAFileSubject, session)

            load_to_db(str(Path(importlib.resources.files(
                'cda2fhir').parent / 'data' / 'raw' / 'association_tables' / 'file_specimen.tsv')),
                       CDAFileSpecimen, session)

    finally:
        session.expire_all()
        session.close()
