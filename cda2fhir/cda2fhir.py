import json
import orjson
from pathlib import Path
import importlib.resources
from fhir.resources.identifier import Identifier
from cda2fhir.load_data import load_data
from cda2fhir.database import SessionLocal
from cda2fhir.cdamodels import CDASubject, CDAResearchSubject, CDASubjectResearchSubject, CDADiagnosis, CDATreatment, CDASubjectAlias, CDASubjectProject
from cda2fhir.transformer import PatientTransformer, ResearchStudyTransformer


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
    patient_transformer = PatientTransformer(session)
    research_study_transformer = ResearchStudyTransformer(session)

    verbose = False
    save = True

    try:
        subjects = session.query(CDASubject).all()
        for subject in subjects:
            print(f"id: {subject.id}, species: {subject.species}, sex: {subject.sex}")

            # projects = session.query(CDASubjectProject).filter_by(subject_id=subject.id).all()
            # for project in projects:
            #    print(f"@@@@@ Subject's associated  projects: {project.associated_project}")

        research_subjects = session.query(CDAResearchSubject).all()
        if verbose:
            print("==== research subjects:")
            for research_subject in research_subjects:
                print(
                    f"id: {research_subject.id}, project: {research_subject.member_of_research_project}, condition: {research_subject.primary_diagnosis_condition}")

        subject_researchsubjects = session.query(CDASubjectResearchSubject).all()
        if verbose:
            print("==== RELATIONS:")
            for subject_researchsubject in subject_researchsubjects:
                print(
                    f"researchsubject_id: {subject_researchsubject.researchsubject_id}, subject_id: {subject_researchsubject.subject_id}")

        diagnoses = session.query(CDADiagnosis).all()
        if verbose:
            print("**** diagnosis:")
            for diagnosis in diagnoses:
                print(f"id: {diagnosis.id}, primary_diagnosis: {diagnosis.primary_diagnosis}")

        treatments = session.query(CDATreatment).all()
        if verbose:
            print("**** treatment:")
            for treatment in treatments:
                print(f"id: {treatment.id}, therapeutic_agent: {treatment.therapeutic_agent}")

        # print("**** subjects type:", type(subjects[0]))
        patients = patient_transformer.transform_human_subjects(subjects)
        if save and patients:
            patients = [orjson.loads(patient.json()) for patient in patients]
            fhir_ndjson(patients, str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'META' / "Patient.ndjson")))

        observations = []
        for subject in subjects:
            if subject.cause_of_death:
                patient_identifiers = patient_transformer.patient_identifier(subject)
                patient_id = patient_transformer.patient_mintid(patient_identifiers[0])
                obs = patient_transformer.observation_cause_of_death(subject.cause_of_death)
                obs_identifier = Identifier(**{'system': "https://cda.readthedocs.io/", 'value': "".join([patient_id, subject.cause_of_death])})
                obs.id = patient_transformer.mint_id(identifier=obs_identifier, resource_type="Observation")
                obs.subject = {"reference": f"Patient/{patient_id}"}
                observations.append(obs)

        if save and observations:
            patient_observations = [orjson.loads(observation.json()) for observation in observations]
            fhir_ndjson(patient_observations, str(Path(
                importlib.resources.files('cda2fhir').parent / 'data' / 'META' / "Observation.ndjson")))

        subject_aliases = session.query(CDASubjectAlias).all()
        if verbose:
            print("==== Subject Alias RELATIONS:")
            for subject_alias in subject_aliases:
                print(
                    f"subject_id: {subject_alias.subject_id}, subject_alias: {subject_alias.subject_alias}")

        subject_projects = session.query(CDASubjectProject).all()
        # print(f"found {len(subject_projects)} subject projects")
        # subjects_with_projects = session.query(CDASubject).join(CDASubjectProject).all()

        research_studies = [research_study_transformer.research_study(project) for project in subject_projects if project.associated_project]
        research_studies = {rs.id: rs for rs in research_studies if rs}.values() # remove duplicates should be a better way

        if save and research_studies:
            rs = [orjson.loads(research_study.json()) for research_study in research_studies if research_study]
            fhir_ndjson(rs, str(Path(
                importlib.resources.files('cda2fhir').parent / 'data' / 'META' / "ResearchStudy.ndjson")))

    finally:
        print("****** Closing Session ******")
        session.close()


if __name__ == '__main__':
    cda2fhir()
