import re
import copy
from typing import Optional
from fhir.resources.patient import Patient
from fhir.resources.identifier import Identifier
from fhir.resources.extension import Extension
from fhir.resources.observation import Observation
from fhir.resources.researchstudy import ResearchStudy
from fhir.resources.codeableconcept import CodeableConcept
from sqlalchemy.orm import Session
from cda2fhir.cdamodels import CDASubject, CDAResearchSubject, CDASubjectResearchSubject, CDASubjectProject
from uuid import uuid3, uuid5, NAMESPACE_DNS


class Transformer:
    def __init__(self, session: Session):
        self.session = session
        self.project_id = 'CDA'
        self.namespace = uuid3(NAMESPACE_DNS, 'cda.readthedocs.io')

    def mint_id(self, identifier: Identifier | str, resource_type: str = None) -> str:
        """create a UUID from an identifier. - mint id via Walsh's convention
        https://github.com/ACED-IDP/g3t_etl/blob/d095895b0cf594c2fd32b400e6f7b4f9384853e2/g3t_etl/__init__.py#L61"""
        # dispatch on type
        if isinstance(identifier, Identifier):
            assert resource_type, "resource_type is required for Identifier"
            identifier = f"{resource_type}/{identifier.system}|{identifier.value}"
        return self._mint_id(identifier)

    def _mint_id(self, identifier_string: str) -> str:
        """create a UUID from an identifier, insert project_id."""
        return str(uuid5(self.namespace, f"{self.project_id}/{identifier_string}"))

    def subject_id_to_research_subject(self, subject_id: str) -> CDAResearchSubject:
        research_subject = (
            self.session.query(CDAResearchSubject)
            .join(CDASubjectResearchSubject, CDAResearchSubject.id == CDASubjectResearchSubject.researchsubject_id)
            .filter(CDASubjectResearchSubject.subject_id == subject_id)
            .first()
        )

        if not research_subject:
            raise ValueError(f"research subject wasn't found for subject: {subject_id}")

        return research_subject


class PatientTransformer(Transformer):
    def __init__(self, session: Session):
        super().__init__(session)
        self.session = session
        self.project_id = 'CDA'
        self.namespace = uuid3(NAMESPACE_DNS, 'cda.readthedocs.io')

    def subject_to_patient(self, subject: CDASubject) -> Patient:
        """transform CDA Subject to FHIR Patient."""
        patient_identifiers = self.patient_identifier(subject)

        extensions = []
        birthSex = self.map_gender(subject.sex)
        if birthSex:
            extensions.append(birthSex)

        usCoreRace = self.map_race(subject.race)
        if usCoreRace:
            extensions.append(usCoreRace)

        usCoreEthnicity = self.map_ethnicity(subject.ethnicity)
        if usCoreEthnicity:
            extensions.append(usCoreEthnicity)

        patient = Patient(**{
            "id": self.patient_mintid(patient_identifiers[0]),
            "identifier": patient_identifiers,
            "deceasedBoolean": self.map_vital_status(subject.vital_status),
        })

        if extensions:
            patient.extension = extensions

        return patient

    def patient_identifier(self, subject: CDASubject) -> list[Identifier]:
        """FHIR patient Identifier from a CDA subject."""
        subject_id_system = "".join(["https://cda.readthedocs.io/", "subject_id"])
        subject_id_identifier = Identifier(**{'system': subject_id_system, 'value': str(subject.id)})

        subject_alias_system = "".join(["https://cda.readthedocs.io/", "subject_alias"])
        subject_alias_identifier = Identifier(**{'system': subject_alias_system, 'value': str(subject.alias_id)})
        return [subject_id_identifier, subject_alias_identifier]

    def patient_mintid(self, patient_identifier: Identifier) -> str:
        """FHIR patient ID from a CDA subject."""
        return self.mint_id(identifier=patient_identifier, resource_type="Patient")

    @staticmethod
    def map_gender(sex: str) -> Extension:
        """map CDA sex to FHIR gender."""
        sex = sex.strip() if sex else sex

        female = Extension(
            **{"url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex", "valueCode": "F"})
        male = Extension(
            **{"url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex", "valueCode": "M"})
        unknown = Extension(
            **{"url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex", "valueCode": "UNK"})

        if sex in ['male', 'Male', 'M']:
            return male
        elif sex in ['female', 'Female', 'F']:
            return female
        elif sex in ['Unspecified', 'Not specified in data', 'O', 'U', '0000']:  # clarify definitions
            return unknown

    @staticmethod
    def map_ethnicity(ethnicity: str) -> Extension:
        """map CDA ethnicity content to FHIR patient extension."""
        url = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity"
        cda_ethnicity = [None, 'not hispanic or latino', 'hispanic or latino',
                         'Not Hispanic or Latino', 'Hispanic or Latino', 'White', 'Asian',
                         'Black', '2', '1', 'C', '102', '01', '96', 'Non-Hispanic Non',
                         '98', 'anonymous', '7', 'W', 'REMOVED', 'WHITE', 'Caucasian',
                         '2131-1', 'Non-Hispanic [8]', 'UNK', 'B', 'Not Hispanic or',
                         'Non-Hispanic/Non', 'Hispanic/Latino', 'H', 'Non-Hispanic',
                         'Non-Hispanic [9]', 'Not Hispanic Lat', 'ETHNICGRP11356', 'A', 'N',
                         '[1]', '6', 'ETHNICGRP1683', 'Non-Hispanic [1]', 'ETHNICGRP1730',
                         '5', '02', 'white-ns', 'Black or African', 'Native Hawaiian',
                         'More than one', 'American Indian', 'Unknown [3]',
                         'Patient Refused', 'Hispanic or Lati', 'Hispanic/Spanish', 'WHT',
                         'WH', 'Mexican, Mexican', 'CAUCASI', 'ETHNICGRP871', '104',
                         'Pacific Islander', 'Hispanic Latino', 'Patient Declined',
                         'anonymized']

        ethnicity_extention = None
        if ethnicity in ['not hispanic or latino', 'Not Hispanic or Latino', 'Not Hispanic or', 'Non-Hispanic/Non',
                         'Non-Hispanic [1]', 'Non-Hispanic', 'Non-Hispanic [9]', 'Not Hispanic Lat']:
            ethnicity_extention = Extension(**{'url': url, 'valueString': 'not hispanic or latino'})
        elif ethnicity in ['hispanic or latino', 'Hispanic or Latino', 'Hispanic/Latino', 'Hispanic Latino']:
            ethnicity_extention = Extension(**{'url': url, 'valueString': 'hispanic or latino'})
        elif ethnicity in ['anonymous', 'REMOVED', 'Patient Refused', 'Patient Declined', 'anonymized']:
            ethnicity_extention = Extension(**{'url': url, 'valueString': 'not reported'})
        elif ethnicity:
            ethnicity_extention = Extension(
                **{'url': url, 'valueString': 'unknown'})  # Unknown to our team - Has room for content update
        return ethnicity_extention

    @staticmethod
    def map_race(race: str) -> Extension:
        """map CDA race content to FHIR patient extension."""
        url = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race"
        cda_race = [None, 'white', 'black or african american', 'asian',
                    'native hawaiian or other pacific islander',
                    'american indian or alaska native', 'White',
                    'Black or African American', 'Asian',
                    'Native Hawaiian or other Pacific Islander',
                    'American Indian or Alaska Native',
                    'Native Hawaiian or Other Pacific Islander',
                    'Black or African American;White']

        race_extention = None
        if race in ['white', 'White']:
            race_extention = Extension(**{'url': url, 'valueString': 'White'})
        elif race in ['black or african american', 'Black or African American']:
            race_extention = Extension(**{'url': url, 'valueString': 'Black or African American'})
        elif race in ['asian', 'Asian']:
            race_extention = Extension(**{'url': url, 'valueString': 'Asian'})
        elif race in ['native hawaiian or other pacific islander', 'Native Hawaiian or other Pacific Islander',
                      'Native Hawaiian or Other Pacific Islander']:
            race_extention = Extension(**{'url': url, 'valueString': 'Native Hawaiian or Other Pacific Islander'})
        elif race in ['american indian or alaska native', 'American Indian or Alaska Native']:
            race_extention = Extension(**{'url': url, 'valueString': 'American Indian or Alaska Native'})
        elif race:
            race_extention = Extension(**{'url': url, 'valueString': 'not reported'})
        return race_extention

    @staticmethod
    def map_vital_status(vital_status: str) -> bool:
        """map CDA vital status to FHIR deceased status."""
        return True if vital_status == 'Dead' else False if vital_status == 'Alive' else None

    @staticmethod
    def filter_related_records():
        """filter records based on CDA human subjects."""
        human_subject_ids = [subject.id for subject in
                             CDASubject.query.filter(CDASubject.species == 'Homo sapiens').all()]
        related_research_subjects = CDAResearchSubject.query.join(
            CDASubjectResearchSubject,
            CDAResearchSubject.id == CDASubjectResearchSubject.researchsubject_id
        ).filter(CDASubjectResearchSubject.subject_id.in_(human_subject_ids)).all()
        return related_research_subjects

    def transform_human_subjects(self, subjects: list[CDASubject]) -> list[Patient]:
        """transform human CDA Subjects to FHIR Patients."""
        human_subjects = [subject.query.filter(subject.species == 'Homo sapiens') for subject in subjects]
        print('**** human_subjects: ', human_subjects.__getitem__(0), len(human_subjects))
        patients = [self.subject_to_patient(subject) for subject in subjects]
        return patients

    def observation_cause_of_death(self, cause_of_death) -> Observation:
        """observation for official cause of death of CDA patient."""
        obs = Observation(**{
            "resourceType": "Observation",
            "id": "observation-cause-of-death",
            "status": "final",
            "category": [
                {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                            "code": "exam",
                            "display": "exam"
                        }
                    ]
                }
            ],
            "code": {
                "coding": [
                    {
                        "system": "http://loinc.org",
                        "code": "79378-6",
                        "display": "Cause of death"
                    }
                ]
            },
            "subject": {
                "reference": f"Patient/example"
            },
            "valueString": cause_of_death
        })
        return obs


class ResearchStudyTransformer(Transformer):
    def __init__(self, session: Session):
        super().__init__(session)
        self.session = session
        self.project_id = 'CDA'
        self.namespace = uuid3(NAMESPACE_DNS, 'cda.readthedocs.io')

    def research_study(self, project: CDASubjectProject, research_subject: CDAResearchSubject) -> ResearchStudy:
        """CDA Projects to FHIR ResearchStudy."""
        if project.associated_project:
            # print(f"associated project: {project.associated_project}")
            rs_identifier = self.research_study_identifier(project)
            rs_id = self.research_study_mintid(rs_identifier[0])

            condition = []

            if research_subject.primary_diagnosis_condition and re.match("^[^\s]+(\s[^\s]+)*$", research_subject.primary_diagnosis_condition):
                condition = [CodeableConcept(**{'coding': [{
                    'code': research_subject.primary_diagnosis_condition,
                    'display': research_subject.primary_diagnosis_condition,
                    'system': 'https://cda.readthedocs.io/'}]})]

            research_study = ResearchStudy(
                **{
                    'id': rs_id,
                    'identifier': rs_identifier,
                    'status': 'active',
                    'name': project.associated_project,
                    'condition': condition
                }
            )
            return research_study

    @staticmethod
    def research_study_identifier(project: CDASubjectProject) -> list[Identifier]:
        """CDA project FHIR Identifier."""
        project_id_system = "".join(["https://cda.readthedocs.io/", "associated_project"])
        project_id_identifier = Identifier(**{'system': project_id_system, 'value': str(project.associated_project)})

        return [project_id_identifier]

    def research_study_mintid(self, rs_identifier: Identifier) -> str:
        """CDA project FHIR Mint ID."""
        return self.mint_id(identifier=rs_identifier, resource_type="ResearchStudy")

