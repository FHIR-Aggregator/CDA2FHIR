import json
import orjson
from pathlib import Path
import importlib.resources
from cda2fhir.load_data import load_data
from cda2fhir.database import SessionLocal
from cda2fhir.cdamodels import CDASubject, CDAResearchSubject, CDASubjectResearchSubject, CDADiagnosis, CDATreatment, CDASubjectAlias
from cda2fhir.transformer import PatientTransformer


def fhir_ndjson(entity, out_path):
    if isinstance(entity, list):
        with open(out_path, 'w', encoding='utf8') as file:
            file.write('\n'.join(map(lambda e: json.dumps(e, ensure_ascii=False), entity)))
    else:
        with open(out_path, 'w', encoding='utf8') as file:
            file.write(json.dumps(entity, ensure_ascii=False))


def cda2fhir():
    load_data()

    session = SessionLocal()
    transformer = PatientTransformer(session)

    try:
        subjects = session.query(CDASubject).all()

        for subject in subjects:
            print(f"id: {subject.id}, species: {subject.species}, sex: {subject.sex}")
             
        research_subjects = session.query(CDAResearchSubject).all()
        print("==== research subjects:")
        for research_subject in research_subjects:
            print(
                f"id: {research_subject.id}, project: {research_subject.member_of_research_project}, condition: {research_subject.primary_diagnosis_condition}")

        subject_researchsubjects = session.query(CDASubjectResearchSubject).all()
        print("==== RELATIONS:")
        for subject_researchsubject in subject_researchsubjects:
            print(
                f"researchsubject_id: {subject_researchsubject.researchsubject_id}, subject_id: {subject_researchsubject.subject_id}")

        diagnoses = session.query(CDADiagnosis).all()
        print("**** diagnosis:")
        for diagnosis in diagnoses:
            print(f"id: {diagnosis.id}, primary_diagnosis: {diagnosis.primary_diagnosis}")

        treatments = session.query(CDATreatment).all()
        print("**** treatment:")
        for treatment in treatments:
            print(f"id: {treatment.id}, therapeutic_agent: {treatment.therapeutic_agent}")

        """try subject to FHIR patient transformation"""
        # print("**** subjects type:", type(subjects[0]))
        patients = transformer.transform_human_subjects(subjects)
        save = True
        if save:
            patients = [orjson.loads(patient.json()) for patient in patients]
            fhir_ndjson(patients, str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'META' / "Patient.ndjson")))

        subject_aliases = session.query(CDASubjectAlias).all()
        print("==== Subject Alias RELATIONS:")
        for subject_alias in subject_aliases:
            print(
                f"subject_id: {subject_alias.subject_id}, subject_alias: {subject_alias.subject_alias}")

    finally:
        print("****** Closing Session ******")
        session.close()


if __name__ == '__main__':
    cda2fhir()
