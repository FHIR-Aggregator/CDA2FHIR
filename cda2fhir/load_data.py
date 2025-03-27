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
                                CDAResearchSubjectDiagnosis, CDATreatment, CDAResearchSubjectTreatment,
                                CDASubjectProject,  CDASpecimen, CDASubjectIdentifier, CDAResearchSubjectSpecimen,
                                ProjectdbGap, GDCProgramdbGap, CDAProjectRelation, CDAFile, CDAFileSpecimen, CDAFileSubject,
                                CDAMutation, CDASubjectMutation)


def file_size(file_path):
    return os.path.getsize(file_path) / (1024 ** 3)  #size in GB


def load_to_db(paths, table_class, session, check_species=False):
    """
    Load data from a single file or list of files (JSON, CSV, Excel, TSV) into the database.

    Parameters:
        paths: A single file path or a list of file paths.
        table_class: The SQLAlchemy model class to load data into.
        session: The SQLAlchemy session.
        check_species: If True, check for a 'species' field in JSON records.
                         If the field exists, only load the record if its value is 'Human' or 'Homo sapiens'.
                         If the field does not exist, assume the record is human.
    """
    import json, mimetypes
    from sqlalchemy.exc import IntegrityError
    import pandas as pd

    human_species_nomenclature = {'Human', 'Homo sapiens'}

    if isinstance(paths, str):
        paths = [paths]

    for path in paths:
        print("####### Processing PATH: ", path)
        result = mimetypes.guess_type(path, strict=False)[0]

        if 'json' in result:
            with open(path, encoding='utf-8') as f:
                data = json.load(f)
                for line in data:
                    if check_species:
                        if 'species' in line:
                            if line.get("species") not in human_species_nomenclature:
                                continue
                        else:
                            line["species"] = "Human"
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
            else:
                print(f"Unsupported file type for PATH: {path}")
                continue

            for row in df.to_dict(orient='records'):
                try:
                    session.add(table_class(**row))
                except IntegrityError:
                    session.rollback()
                    print(f"Skipping duplicate entry in {table_class.__tablename__}: {row}")

        session.flush()  # flush changes to the database
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


def load_file_relations(session):
    """
    Loads file_subject and file_specimen relationships.
    """
    load_to_db(str(Path(importlib.resources.files(
        'cda2fhir').parent / 'data' / 'raw' / 'human_file_subject.tsv')),
               CDAFileSubject, session)
    print(f"Loaded CDAFileSubject relationships")

    load_to_db(str(Path(importlib.resources.files(
        'cda2fhir').parent / 'data' / 'raw' / 'association_tables' / 'file_specimen.tsv')),
               CDAFileSpecimen, session)
    print(f"Loaded CDAFileSpecimen relationships")


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
    clear_table(CDASubjectProject, session)
    clear_table(CDASpecimen, session)
    clear_table(CDAResearchSubjectSpecimen, session)
    clear_table(ProjectdbGap, session)
    clear_table(GDCProgramdbGap, session)
    clear_table(CDASubjectIdentifier, session)

    try:
        # if not table_exists(engine, 'subject'): #TODO: add when relations and tables are defined
        load_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw_022025' / 'subject.json')),
                   CDASubject, session)

        load_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw_022025' / 'subject_researchsubject.json')),
                   CDASubjectResearchSubject, session)

        load_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw_022025' / 'subject_identifier.json')),
                   CDASubjectIdentifier, session)

        load_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw_022025' / 'subject_associated_project.json')),
                   CDASubjectProject, session)

        load_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw_022025' / 'researchsubject.json')),
                   CDAResearchSubject, session)

        if transform_condition:
            load_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw_022025' / 'diagnosis.json')),
                       CDADiagnosis, session)

        if transform_treatment:
            load_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw_022025' / 'treatment.json')),
                       CDATreatment, session)

        load_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw_022025' / 'researchsubject_diagnosis.json')),
                   CDAResearchSubjectDiagnosis, session)


        load_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw_022025' / 'researchsubject_treatment.json')),
                   CDAResearchSubjectTreatment, session)


        if not transform_mutation and not transform_condition:

            load_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw_022025' / 'specimen.json')),
                       CDASpecimen, session)

        load_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw_022025' / 'researchsubject_specimen.json')),
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
            load_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw_022025' / 'cholangiocarcinoma_mutations.json')), CDAMutation, session)

            # mutation_folder_path = str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw' / 'project_mutations'))
            # mutation_file_paths = glob.glob(os.path.join(mutation_folder_path, '*'))
            # print("~~~ Globbed mutation files: ", mutation_file_paths)
            #
            # load_to_db(mutation_file_paths, CDAMutation, session)
            #
            load_to_db(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw_022025' / 'subject_mutation.json')),
                        CDASubjectMutation, session)

        if transform_files:
            # files_dir = Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw_022025' / 'files_converted')
            # file_paths = list(files_dir.glob('*'))  # glob all files in the directory
            #
            # if not file_paths:
            #     raise ValueError("Project-specific files were not found.")

            # load_file_relations(session)

            file_path = str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw_022025' / 'files_converted'/ 'cholangiocarcinoma_files.json'))
            load_to_db(file_path, CDAFile, session)

            file_size_mb = os.path.getsize(file_path) / (1024 ** 2)
            print(f"File: {file_path}, Size: {file_size_mb:.2f} MB")

            # folder_path = str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw' / 'files_converted'))
            # file_paths = glob.glob(os.path.join(folder_path, '*'))
            # print("Globbed: ", file_paths)
            # load_to_db(file_paths, CDAFile, session)

            # large file - can be useful to reduce the relations files by CDA project as well
            # file alias -> project  join by file id with file_subject

    finally:
        session.expire_all()
        session.close()
