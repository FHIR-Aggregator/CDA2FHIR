import json
import orjson
from pathlib import Path
import importlib.resources
from fhir.resources.identifier import Identifier
from cda2fhir.load_data import load_data
from cda2fhir.database import SessionLocal
from cda2fhir.cdamodels import CDASubject, CDAResearchSubject, CDASubjectResearchSubject, CDADiagnosis, CDATreatment, \
    CDASubjectAlias, CDASubjectProject, CDAResearchSubjectDiagnosis, CDASpecimen
from cda2fhir.transformer import Transformer, PatientTransformer, ResearchStudyTransformer, ResearchSubjectTransformer, \
    ConditionTransformer
from sqlalchemy import select, func


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
    research_subject_transformer = ResearchSubjectTransformer(session)
    condition_transformer = ConditionTransformer(session)


    verbose = True
    save = True

    try:
        subjects = session.query(CDASubject).all()
        if verbose:
            for subject in subjects:
                print(f"id: {subject.id}, species: {subject.species}, sex: {subject.sex}")

            # projects = session.query(CDASubjectProject).filter_by(subject_id=subject.id).all()
            # for project in projects:
            #    print(f"@@@@@ Subject's associated  projects: {project.associated_project}")

        specimens = session.query(CDASpecimen).all()
        if verbose:
            print("****@@@@ SPECIMEN:")
            for specimen in specimens:
                print(f"id: {specimen.id}, source_material_type: {specimen.source_material_type}")

        cda_research_subjects = session.query(CDAResearchSubject).all()
        if verbose:
            print("==== research subjects:")
            for cda_research_subject in cda_research_subjects:
                print(
                    f"id: {cda_research_subject.id}, project: {cda_research_subject.member_of_research_project}, condition: {cda_research_subject.primary_diagnosis_condition}")

        subject_researchsubjects = session.query(CDASubjectResearchSubject).all()
        if verbose:
            print("==== RELATIONS:")
            for subject_researchsubject in subject_researchsubjects:
                print(
                    f"researchsubject_id: {subject_researchsubject.researchsubject_id}, subject_id: {subject_researchsubject.subject_id}")

        # diagnoses = session.query(CDADiagnosis).all()
        # if verbose:
        #    print("**** diagnosis:")
        #    for diagnosis in diagnoses:
        #        print(f"id: {diagnosis.id}, primary_diagnosis: {diagnosis.primary_diagnosis}")

        treatments = session.query(CDATreatment).all()
        if verbose:
            print("**** treatment:")
            for treatment in treatments:
                print(f"id: {treatment.id}, therapeutic_agent: {treatment.therapeutic_agent}")

        # print("**** subjects type:", type(subjects[0]))
        patients = patient_transformer.transform_human_subjects(subjects)
        if save and patients:
            patients = [orjson.loads(patient.json()) for patient in patients]
            fhir_ndjson(patients,
                        str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'META' / "Patient.ndjson")))
        """
        observations = []
        for subject in subjects:
            if subject.cause_of_death:
                patient_identifiers = patient_transformer.patient_identifier(subject)
                patient_id = patient_transformer.patient_mintid(patient_identifiers[0])
                obs = patient_transformer.observation_cause_of_death(subject.cause_of_death)
                obs_identifier = Identifier(
                    **{'system': "https://cda.readthedocs.io/", 'value': "".join([patient_id, subject.cause_of_death])})
                obs.id = patient_transformer.mint_id(identifier=obs_identifier, resource_type="Observation")
                obs.subject = {"reference": f"Patient/{patient_id}"}
                observations.append(obs)

        subject_aliases = session.query(CDASubjectAlias).all()
        if verbose:
            print("==== Subject Alias RELATIONS:")
            for subject_alias in subject_aliases:
                print(
                    f"subject_id: {subject_alias.subject_id}, subject_alias: {subject_alias.subject_alias}")

        subject_projects = session.query(CDASubjectProject).all()
        # print(f"found {len(subject_projects)} subject projects")
        # subjects_with_projects = session.query(CDASubject).join(CDASubjectProject).all()

        research_studies = []
        research_subjects = []
        for project in subject_projects:
            if project.associated_project:
                query_research_subjects = (
                    session.query(CDAResearchSubject)
                    .join(CDASubjectResearchSubject)
                    .filter(CDASubjectResearchSubject.subject_id == project.subject_id)
                    .all()
                )
                # fhir research subject
                _subject = (
                    session.query(CDASubject)
                    .filter(CDASubject.id == project.subject_id)
                    .all()
                )
                _patient = patient_transformer.transform_human_subjects(_subject)

                for cda_rs_subject in query_research_subjects:
                    research_study = research_study_transformer.research_study(project, cda_rs_subject)
                    if research_study:
                        research_studies.append(research_study)
                        if _patient and research_study:
                            _research_subject = research_subject_transformer.research_subject(cda_rs_subject, _patient[0], research_study)
                            research_subjects.append(_research_subject)

        if save and research_studies:
            research_studies = {rstudy.id: rstudy for rstudy in research_studies if
                                rstudy}.values()  # remove duplicates should be a better way
            rs = [orjson.loads(research_study.json()) for research_study in research_studies if research_study]
            fhir_ndjson(rs, str(Path(
                importlib.resources.files('cda2fhir').parent / 'data' / 'META' / "ResearchStudy.ndjson")))

        if save and research_subjects:
            research_subjects = {rsubject.id: rsubject for rsubject in research_subjects if
                                rsubject}.values()
            fhir_rsubjects = [orjson.loads(cdarsubject.json()) for cdarsubject in research_subjects if cdarsubject]
            fhir_ndjson(fhir_rsubjects, str(Path(
                importlib.resources.files('cda2fhir').parent / 'data' / 'META' / "ResearchSubject.ndjson")))

        # randomly choose N diagnosis to reduce runtime for development
        # takes ~ 2hrs for 1041360+ diagnosis records
        n = 100  # reduce size
        for _ in range(n):
            reduced_diagnoses = session.execute(
                select(CDADiagnosis)
                .order_by(func.random()) # randomly select
                .limit(n)
            ).scalars().all()

        conditions = []
        for diagnosis in reduced_diagnoses:
            _subject_diagnosis = (
                session.query(CDASubject)
                .join(CDASubjectResearchSubject, CDASubject.id == CDASubjectResearchSubject.subject_id)
                .join(CDAResearchSubject, CDASubjectResearchSubject.researchsubject_id == CDAResearchSubject.id)
                .join(CDAResearchSubjectDiagnosis, CDAResearchSubject.id == CDAResearchSubjectDiagnosis.researchsubject_id)
                .filter(CDAResearchSubjectDiagnosis.diagnosis_id == diagnosis.id)
                .all()
            )

            if _subject_diagnosis:
                _patient_diagnosis = patient_transformer.transform_human_subjects(_subject_diagnosis)
                if _patient_diagnosis and _patient_diagnosis[0].id:
                    print(f"------- patient id for diagnosis ID {diagnosis.id} is : {_patient_diagnosis[0].id}")
                    condition = condition_transformer.condition(diagnosis, _patient_diagnosis[0])
                    if condition:
                        conditions.append(condition)

                        # condition stage observation
                        if condition.stage:
                            stage = condition.stage[0]
                            if stage.assessment and stage.summary:
                                _display = stage.summary.coding[0].display
                                if _display:
                                    observation = condition_transformer.condition_observation(diagnosis, _display, _patient_diagnosis[0], condition.id)
                                    if observation:
                                        observations.append(observation)

        if save and conditions:
            _conditions = {_condition.id: _condition for _condition in conditions if _condition}.values()
            fhir_conditions = [orjson.loads(c.json()) for c in _conditions if c]
            fhir_ndjson(fhir_conditions, str(Path(
                importlib.resources.files('cda2fhir').parent / 'data' / 'META' / "Condition.ndjson")))

        if save and observations:
            observations = {_obs.id: _obs for _obs in observations if _obs}.values()
            patient_observations = [orjson.loads(observation.json()) for observation in observations]
            fhir_ndjson(patient_observations, str(Path(
                importlib.resources.files('cda2fhir').parent / 'data' / 'META' / "Observation.ndjson")))
        """
    finally:
        print("****** Closing Session ******")
        session.close()


if __name__ == '__main__':
    cda2fhir()
