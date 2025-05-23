import re
import uuid
import logging
import sqlite3
from typing import Optional
from fhir.resources.patient import Patient
from fhir.resources.specimen import Specimen, SpecimenCollection
from fhir.resources.identifier import Identifier
from fhir.resources.extension import Extension
from fhir.resources.observation import Observation
from fhir.resources.researchstudy import ResearchStudy
from fhir.resources.documentreference import DocumentReference
from fhir.resources.group import Group, GroupMember
from fhir.resources.attachment import Attachment
from fhir.resources.bodystructure import BodyStructure, BodyStructureIncludedStructure
from fhir.resources.researchsubject import ResearchSubject
from fhir.resources.codeableconcept import CodeableConcept
from fhir.resources.reference import Reference
from fhir.resources.codeablereference import CodeableReference
from fhir.resources.condition import Condition, ConditionStage
from fhir.resources.substancedefinition import SubstanceDefinition, SubstanceDefinitionStructure, \
    SubstanceDefinitionStructureRepresentation, SubstanceDefinitionName
from fhir.resources.medication import Medication, MedicationIngredient
from fhir.resources.medicationadministration import MedicationAdministration, MedicationAdministrationDosage
from fhir.resources.resource import Resource
from fhir.resources.substance import Substance
from fhir.resources.quantity import Quantity
from fhir.resources.timing import Timing, TimingRepeat
from fhir.resources.range import Range
from sqlalchemy.orm import Session
from sqlalchemy import select
from cda2fhir.cdamodels import CDASubject, CDAResearchSubject, CDASubjectResearchSubject, CDASubjectProject, \
    CDADiagnosis, CDASpecimen, CDAFile, CDATreatment, CDAMutation
from uuid import uuid3, uuid5, NAMESPACE_DNS

CDA_SITE = 'cda.readthedocs.io'

MISSING_RELATIONS = ["MATCH-C1", "MATCH-P", "MATCH-Z1B",
                     "Proteogenomic Translational Research Centers (PTRC)", "DCCPS", "cmb_aml", "cmb_lca",
                     "CCDI", "cmb_mel", "cmb_mml", "cmb_crc", "cmb_gec", "ccdi_mci", "cmb_pca", "gtex",
                     "CPTAC3 Discovery and Confirmatory"]

logging.basicConfig(
    filename='info.log',
    level=logging.INFO,
    filemode='w',
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
stream_handler.setFormatter(stream_formatter)
logger.addHandler(stream_handler)

file_handler = logging.FileHandler("log_file.log", mode="w")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(stream_formatter)
logger.addHandler(file_handler)

error_handler = logging.FileHandler('error.log', mode="w")
error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(stream_formatter)
logger.addHandler(error_handler)

no_project_logger = logging.getLogger("no_project_associations")
no_project_logger.setLevel(logging.INFO)
no_project_handler = logging.FileHandler("no_project_associations.log", mode="w")
no_project_handler.setLevel(logging.INFO)
no_project_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
no_project_handler.setFormatter(no_project_formatter)
no_project_logger.addHandler(no_project_handler)

# logger.debug("Logging is now configured.")

class Transformer:
    def __init__(self, session: Session):
        self.session = session
        self.project_id = 'fhir_aggregator-cda'
        self.namespace = uuid3(NAMESPACE_DNS, CDA_SITE)
        self.SYSTEM_PDC = "https://proteomic.datacommons.cancer.gov/pdc/"
        self.SYSTEM_GDC = "https://gdc.cancer.gov/"
        self.SYSTEM_IDC = "https://portal.imaging.datacommons.cancer.gov/"
        self.SYSTEM_ICDC = "https://caninecommons.cancer.gov/"

    @staticmethod
    def get_component(key, value=None, component_type=None,
                      system=f"https://{CDA_SITE}"):
        if component_type == 'string':
            value = {"valueString": value}
        elif component_type == 'int':
            value = {"valueInteger": value}
        elif component_type == 'float':
            value = {"valueQuantity": {"value": value}}
        elif component_type == 'bool':
            value = {"valueBoolean": value}
        elif component_type == 'dateTime':
            value = {"valueDateTime": value}
        else:
            pass

        component = {
            "code": {
                "coding": [
                    {
                        "system": system,
                        "code": key,
                        "display": key
                    }
                ],
                "text": key
            }
        }

        if value:
            component.update(value)

        return component

    @staticmethod
    def is_valid_uuid(value: str) -> bool:
        if value is None:
            return False
        try:
            _obj = uuid.UUID(value, version=5)
        except ValueError:
            return False
        return True

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

    def program_research_study(self, name) -> ResearchStudy:
        """create top level program FHIR ResearchStudy"""
        _program_identifier = Identifier(**{"system": "".join([f"https://{CDA_SITE}/", "system"]), "value": name, "use": "official"})
        _id = self.mint_id(identifier=_program_identifier, resource_type="ResearchStudy")
        # All programs are part of - The NCI Cancer Research Data Commons (CRDC)
        _crdc_identifier = Identifier(
            **{"system": "".join([f"https://{CDA_SITE}/", "system"]), "value": 'CRDC', "use": "official"})
        _crdc_id = self.mint_id(identifier=_program_identifier, resource_type="ResearchStudy")
        research_study = ResearchStudy(
            **{
                'id': _id,
                'identifier': [_program_identifier],
                'status': 'active',
                'name': name,
                'title': name,
                'extension': [{
                            "url": "http://fhir-aggregator.org/fhir/StructureDefinition/part-of-study",
                            "valueReference": {"reference": f"ResearchStudy/{_crdc_id}"}
                        }]
            }
        )
        if research_study:
            return research_study


    def get_part_of_study_extension(self, subject: CDASubject, extensions: list):
        ext_url = "http://fhir-aggregator.org/fhir/StructureDefinition/part-of-study"

        def get_extension_url(ext):
            if isinstance(ext, dict):
                return ext.get("url")
            elif hasattr(ext, "url"):
                return ext.url
            return None

        # if any(get_extension_url(ext) == ext_url for ext in extensions):
        #     return

        project_value = None
        if subject.subject_project_relation and len(subject.subject_project_relation) > 0:
            for relation in subject.subject_project_relation:
                project_id_system = "".join([f"https://{CDA_SITE}/", "associated_project"])
                project_id_identifier = Identifier(
                    **{
                        'system': project_id_system,
                        'value': relation.associated_project,
                        "use": "official"
                    }
                )
                research_study_id = self.mint_id(identifier=project_id_identifier, resource_type="ResearchStudy")

                if research_study_id:
                    part_of_study = {
                        "url": "http://fhir-aggregator.org/fhir/StructureDefinition/part-of-study",
                        "valueReference": {"reference": f"ResearchStudy/{research_study_id}"}
                    }
                    extensions.append(part_of_study)
        elif '.' in subject.id:
            project_value = subject.id.split('.')[0]
            if project_value:
                project_id_system = f"https://{CDA_SITE}/associated_project"
                project_id_identifier = Identifier(system=project_id_system, value=project_value, use="official")
                research_study_id = self.mint_id(identifier=project_id_identifier, resource_type="ResearchStudy")
                if research_study_id:
                    extensions.append({
                        "url": ext_url,
                        "valueReference": {"reference": f"ResearchStudy/{research_study_id}"}
                    })
                else:
                    no_project_logger.info(f"Subject with id {subject.id} does not have any project associations.")


class PatientTransformer(Transformer):
    def __init__(self, session: Session):
        super().__init__(session)
        self.session = session
        self.project_id = 'CDA'
        self.namespace = uuid3(NAMESPACE_DNS, CDA_SITE)

    def subject_to_patient(self, subject: CDASubject) -> Patient:
        """transform CDA Subject to FHIR Patient."""
        patient_identifiers = self.patient_identifier(subject)

        extensions = []
        self.get_part_of_study_extension(subject, extensions=extensions)
        if (subject.subject_project_relation and len(subject.subject_project_relation) == 0 ) or len(extensions) == 0:
            no_project_logger.info(f"Subject with id {subject.id} does not have any project associations.")
            for missing_project in MISSING_RELATIONS:
                if missing_project in subject.id:
                    project_id_system = "".join([f"https://{CDA_SITE}/", "associated_project"])
                    project_id_identifier = Identifier(
                        **{
                            'system': project_id_system,
                            'value': missing_project,
                            "use": "official"
                        }
                    )
                    research_study_id = self.mint_id(identifier=project_id_identifier, resource_type="ResearchStudy")
                    if research_study_id:
                        part_of_study = {
                            "url": "http://fhir-aggregator.org/fhir/StructureDefinition/part-of-study",
                            "valueReference": {"reference": f"ResearchStudy/{research_study_id}"}
                        }
                        extensions.append(part_of_study)

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
        all_identifiers = []
        if subject.subject_identifier:
            for identifier in subject.subject_identifier:
                system = identifier.system
                field_name = identifier.field_name
                value = identifier.value
                if system == "GDC":
                    if "." in field_name:
                        field_name = field_name.split(".")[1].strip()
                    gdc_system = "".join([self.SYSTEM_GDC, field_name])
                    all_identifiers.append(Identifier(**{'system': gdc_system, 'value': str(value), "use": "secondary"}))
                if system == "PDC":
                    if "." in field_name:
                        field_name = field_name.split(".")[1].strip()
                    pdc_system = "".join([self.SYSTEM_PDC, field_name])
                    all_identifiers.append(
                        Identifier(**{'system': pdc_system, 'value': str(value), "use": "secondary"}))
                if system == "IDC":
                    if "." in field_name:
                        field_name = field_name.split(".")[1].strip()
                    idc_system = "".join([self.SYSTEM_IDC, field_name])
                    all_identifiers.append(
                        Identifier(**{'system': idc_system, 'value': str(value), "use": "secondary"}))
                if system == "ICDC":
                    if "." in field_name:
                        field_name = field_name.split(".")[1].strip()
                    icdc_system = "".join([self.SYSTEM_ICDC, field_name])
                    all_identifiers.append(
                        Identifier(**{'system': icdc_system, 'value': str(value), "use": "secondary"}))

        subject_id_system = "".join([f"https://{CDA_SITE}/", "subject_id"])
        subject_id_identifier = Identifier(**{'system': subject_id_system, 'value': str(subject.id), "use": "official"})
        all_identifiers.append(subject_id_identifier)

        subject_alias_system = "".join([f"https://{CDA_SITE}/", "subject_alias"])
        subject_alias_identifier = Identifier(**{'system': subject_alias_system, 'value': str(subject.integer_id_alias), "use": "secondary"})
        all_identifiers.append(subject_alias_identifier)

        return all_identifiers

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
        # human_subjects = [subject.query.filter(subject.species == 'Homo sapiens') for subject in subjects] # filtered in load
        # print('**** human_subjects: ', human_subjects.__getitem__(0), len(human_subjects))
        patients = [self.subject_to_patient(subject) for subject in subjects]
        return patients

    def observation_cause_of_death(self, cause_of_death) -> Observation:
        """
            observation for official cause of death of CDA patient. source:
            https://build.fhir.org/ig/HL7/vrdr/StructureDefinition-vrdr-cause-of-death-part1.html
        """
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
                        "code": "69453-9",
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

    def observation_days_to_death(self, days_to_death) -> Observation:
        """observation for official days to death of CDA patient."""
        obs = Observation(**{
            "resourceType": "Observation",
            "id": "observation-days-to-death",
            "status": "final",
            "category": [
                {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                            "code": "survey",
                            "display": "survey"
                        }
                    ]
                }
            ],
            "code": {
                "coding": [
                    {
                        "system": "https://ontobee.org/",
                        "code": "NCIT_C156419",
                        "display": "Days between Diagnosis and Death"
                    }
                ]
            },
            "valueQuantity": {
                "value": int(days_to_death),
                "unit": "days",
                "system": "http://unitsofmeasure.org",
                "code": "d"
            }
        })
        return obs

    def observation_days_to_birth(self, days_to_birth) -> Observation:
        """observation for official days to birth of CDA patient."""
        obs = Observation(**{
            "resourceType": "Observation",
            "id": "observation-days-to-birth",
            "status": "final",
            "category": [
                {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                            "code": "survey",
                            "display": "survey"
                        }
                    ]
                }
            ],
            "code": {
                "coding": [
                    {
                        "system": "https://ontobee.org/",
                        "code": "NCIT_C156418",
                        "display": "Days Between Birth and Diagnosis"
                    }
                ]
            },
            "valueQuantity": {
                "value": int(days_to_birth),
                "unit": "days",
                "system": "http://unitsofmeasure.org",
                "code": "d"
            }
        })
        return obs


class ResearchStudyTransformer(Transformer):
    def __init__(self, session: Session):
        super().__init__(session)
        self.session = session
        self.project_id = 'CDA'
        self.namespace = uuid3(NAMESPACE_DNS, CDA_SITE)

    def research_study(self, project: CDASubjectProject, research_subject: CDAResearchSubject) -> ResearchStudy:
        """CDA Projects to FHIR ResearchStudy."""
        if project.associated_project:
            rs_identifier = self.research_study_identifier(project, use="official")
            rs_id = self.research_study_mintid(rs_identifier[0])

            research_study = ResearchStudy(
                **{
                    'id': rs_id,
                    'identifier': rs_identifier,
                    'status': 'active',
                    'name': project.associated_project,
                    'title': project.associated_project,
                    'extension': []
                }
            )

            if research_subject and getattr(research_subject, 'primary_diagnosis_condition') and research_subject.primary_diagnosis_condition and re.match(r"^[^\s]+(\s[^\s]+)*$",
                                                                         research_subject.primary_diagnosis_condition):
                condition = [CodeableConcept(**{'coding': [{
                    'code': research_subject.primary_diagnosis_condition,
                    'display': research_subject.primary_diagnosis_condition,
                    'system': f'https://{CDA_SITE}/'}]})]

                research_study.condition = condition

            return research_study

    @staticmethod
    def research_study_identifier(project: CDASubjectProject, use: str) -> list[Identifier]:
        """CDA project FHIR Identifier."""
        project_id_system = "".join([f"https://{CDA_SITE}/", "associated_project"])
        project_id_identifier = Identifier(**{'system': project_id_system, 'value': str(project.associated_project), "use": use})

        return [project_id_identifier]

    def research_study_mintid(self, rs_identifier: Identifier) -> str:
        """CDA project FHIR Mint ID."""
        return self.mint_id(identifier=rs_identifier, resource_type="ResearchStudy")


class ResearchSubjectTransformer(Transformer):
    def __init__(self, session: Session):
        super().__init__(session)
        self.session = session
        self.project_id = 'CDA'
        self.namespace = uuid3(NAMESPACE_DNS, CDA_SITE)


    def research_subject(self, cda_research_subject: CDAResearchSubject, patient: Patient,
                         research_study: ResearchStudy) -> ResearchSubject:
        rs_identifier = self.research_subject_identifier(cda_research_subject, use="official")
        _id = self.research_subject_mintid(rs_identifier[0])

        part_of_extension = None
        if hasattr(patient, "extension") and patient.extension:
            for ext in patient.extension:
                if ext.url == "http://fhir-aggregator.org/fhir/StructureDefinition/part-of-study":
                    part_of_extension = ext
                    break

        research_subject = ResearchSubject(
            **{
                "id": _id,
                "identifier": rs_identifier,
                "status": "active",
                "subject": {
                    "reference": f"Patient/{patient.id}"
                },
                "study": {
                    "reference": f"ResearchStudy/{research_study.id}"
                },
                **({"extension": [part_of_extension]} if part_of_extension else {})
            }
        )
        return research_subject


    @staticmethod
    def research_subject_identifier(cda_research_subject: CDAResearchSubject, use: str) -> list[Identifier]:
        """CDA research subject FHIR Identifier."""
        assert cda_research_subject.id, "CDA Research Subject doesn't have an id"
        research_subject_id_system = "".join([f"https://{CDA_SITE}/", "researchsubject"])
        research_subject_id_identifier = Identifier(
            **{'system': research_subject_id_system, 'value': cda_research_subject.id, "use": use})

        return [research_subject_id_identifier]

    def research_subject_mintid(self, rs_identifier: Identifier) -> str:
        """CDA research subject FHIR Mint ID."""
        return self.mint_id(identifier=rs_identifier, resource_type="ResearchSubject")


class ConditionTransformer(Transformer):
    def __init__(self, session: Session):
        super().__init__(session)
        self.session = session
        self.project_id = 'CDA'
        self.namespace = uuid3(NAMESPACE_DNS, CDA_SITE)

    def condition(self, diagnosis: CDADiagnosis, patient: PatientTransformer) -> Condition:
        if diagnosis.primary_diagnosis is None or not re.match(r"^[^\s]+(\s[^\s]+)*$", diagnosis.primary_diagnosis):
            print(
                f"---- SKIPPING DIAGNOSIS {diagnosis.id} due to missing or invalid primary diagnosis string per fhir standards.")
            return None

        condition_identifier = self.condition_identifier(diagnosis)
        condition_id = self.condition_mintid(condition_identifier[0])

        _stage_code = None
        _stage_code , _stage_display = self.fetch_stage_info(diagnosis)

        stage_summary = None
        if _stage_code:
            stage_summary = CodeableConcept(**{
                "coding": [
                    {
                        "system": _stage_code['system'],
                        "code": _stage_code['code'],
                        "display": _stage_display
                    }
                ]
            })

        onset = None
        if diagnosis.age_at_diagnosis:
            onset = str(diagnosis.age_at_diagnosis)

        _condition_observation = None
        if _stage_code:
            _condition_observation = self.condition_observation(diagnosis, _stage_code, _stage_display, patient, condition_id)

        stage = []
        if stage_summary and _condition_observation:
            stage = [ConditionStage(
                **{
                    "summary": stage_summary,
                    "assessment": [
                        {
                            "reference": f"Observation/{_condition_observation.id}"
                        }
                    ]
                }
            )]

        code = None
        display = None
        if ":" in diagnosis.primary_diagnosis:
            code, display = diagnosis.primary_diagnosis.split(':', 1)
            code = code.strip()
            display = display.strip()
        else:
            code = diagnosis.primary_diagnosis
            display = diagnosis.primary_diagnosis

        if "cholangiocarcinoma" in diagnosis.primary_diagnosis.lower():
            code = "70179006"
            display = "Cholangiocarcinoma"

        if code == "70179006":
            system = "http://snomed.info/sct"
        else:
            system = f"https://{CDA_SITE}/"

        part_of_extension = None
        if hasattr(patient, "extension") and patient.extension:
            for ext in patient.extension:
                if ext.url == "http://fhir-aggregator.org/fhir/StructureDefinition/part-of-study":
                    part_of_extension = ext
                    break

        _condition = Condition(
            **{
                "id": condition_id,
                "identifier": condition_identifier,
                "subject": {
                    "reference": f"Patient/{patient.id}"
                },
                "clinicalStatus": CodeableConcept(
                    **{
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/condition-clinical",
                                "code": "active",
                                "display": "Active"
                            }
                        ]
                    }
                ),
                "category": [CodeableConcept(
                    **{
                        "coding": [{"system": "http://terminology.hl7.org/CodeSystem/condition-category",
                                    "code": "encounter-diagnosis",
                                    "display": "Encounter Diagnosis"}]}
                )],
                "code": CodeableConcept(
                    **{
                        "coding": [{"system": system,
                                    "code": code,
                                    "display": display}]}
                ),
                "onsetString": onset,
                "stage": stage,
                **({"extension": [part_of_extension]} if part_of_extension else {})
            }
        )
        return _condition


    def fetch_stage_info(self, diagnosis) -> tuple | None:
        _code = None
        _display = None
        if diagnosis.pathologic_stage:
            _display = diagnosis.pathologic_stage
            _code = {
                "system": "http://snomed.info/sct",
                "code": "1222593009",
                "display": "American Joint Committee on Cancer pathological stage group allowable value"
            }
        elif diagnosis.pathologic_stage_t:
            _display = diagnosis.pathologic_stage_t
            _code = {
                "system": "http://snomed.info/sct",
                "code": "1222589003",
                "display": "American Joint Committee on Cancer pathological T category allowable value"

            }
        elif diagnosis.pathologic_stage_n:
            _display = diagnosis.pathologic_stage_n
            _code = {
                "system": "http://snomed.info/sct",
                "code": "1222590007",
                "display": "American Joint Committee on Cancer pathological N category allowable value"

            }
        elif diagnosis.pathologic_stage_m:
            _display = diagnosis.pathologic_stage_m
            _code = {
                "system": "http://snomed.info/sct",
                "code": "1222591006",
                "display": "American Joint Committee on Cancer pathological M category allowable value"

            }
        elif diagnosis.grade:
            _display = diagnosis.grade
            _code = {
                "system": "http://snomed.info/sct",
                "code": "1222599008",
                "display": "American Joint Committee on Cancer pathological grade allowable value"
            }
        elif diagnosis.clinical_stage:
            _display = diagnosis.clinical_stage
            _code = {
                "system": "http://snomed.info/sct",
                "code": "1222592004",
                "display": "American Joint Committee on Cancer clinical stage group allowable value"

            }
        elif diagnosis.clinical_stage_t:
            _display = diagnosis.clinical_stage_t
            _code = {
                "system": "http://snomed.info/sct",
                "code": "1222585009",
                "display": "American Joint Committee on Cancer clinical T category allowable value"

            }
        elif diagnosis.clinical_stage_n:
            _display = diagnosis.clinical_stage_n
            _code = {
                "system": "http://snomed.info/sct",
                "code": "1222588006",
                "display": "American Joint Committee on Cancer clinical N category allowable value"

            }
        elif diagnosis.clinical_stage_m:
            _display = diagnosis.clinical_stage_m
            _code = {
                "system": "http://snomed.info/sct",
                "code": "1222587001",
                "display": "American Joint Committee on Cancer clinical M category allowable value"

            }

        return _code, _display

    def condition_observation(self, diagnosis, _stage_code, _stage_display, patient, _condition_id) -> Observation | None:
        condition_id_system = "".join([f"https://{CDA_SITE}/", "diagnosis"])
        observation_identifier = Identifier(**{'system': condition_id_system, 'value': diagnosis.id, "use": "official"})
        observation_id = self.mint_id(identifier=observation_identifier, resource_type="Observation")

        # observation_code = self.fetch_stage_info(diagnosis)
        # if observation_code is None:
        #     print(f"Skipping .... Observation for diagnosis condition {diagnosis.id} doesn't exist.")
        #     return None

        part_of_extension = None
        if hasattr(patient, "extension") and patient.extension:
            for ext in patient.extension:
                if ext.url == "http://fhir-aggregator.org/fhir/StructureDefinition/part-of-study":
                    part_of_extension = ext
                    break

        observation = Observation(
            **{
                "id": observation_id,
                "identifier": [observation_identifier],
                "status": "final",
                "category": [
                    {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                                "code": "laboratory",
                                "display": "Laboratory"
                            }
                        ]
                    }
                ],
                "code": {
                    "coding": [_stage_code]
                },
                "subject": {
                    "reference": f"Patient/{patient.id}"
                },
                "focus": [{
                    "reference": f"Condition/{_condition_id}"
                }],
                "valueCodeableConcept": {
                    "coding": [{
                        "system": _stage_code['system'],
                        "code": _stage_code['code'],
                        "display": _stage_display
                    }
                    ]
                },
                **({"extension": [part_of_extension]} if part_of_extension else {})
            }
        )
        return observation

    def observation_method_of_diagnosis(self, method_of_diagnosis, patient) -> Observation:
        """observation for official method_of_diagnosis of CDA patient."""

        part_of_extension = None
        if hasattr(patient, "extension") and patient.extension:
            for ext in patient.extension:
                if ext.url == "http://fhir-aggregator.org/fhir/StructureDefinition/part-of-study":
                    part_of_extension = ext
                    break

        obs = Observation(**{
            "resourceType": "Observation",
            "id": "observation-method-of-diagnosis",
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
                        "system": "https://ontobee.org/",
                        "code": "NCIT_C177576",
                        "display": "Diagnostic Method"
                    }
                ]
            },
            "subject": {
                "reference": f"Patient/example"
            },
            "valueString": method_of_diagnosis,
            **({"extension": [part_of_extension]} if part_of_extension else {})
        })
        return obs

    @staticmethod
    def condition_identifier(cda_diagnosis: CDADiagnosis) -> list[Identifier]:
        """CDA Diagnosis Condition FHIR Identifier."""
        condition_id_system = "".join([f"https://{CDA_SITE}/", "diagnosis"])
        condition_identifier = Identifier(**{'system': condition_id_system, 'value': cda_diagnosis.id, "use":"official"})
        return [condition_identifier]

    def condition_mintid(self, condition_identifier: Identifier) -> str:
        """CDA Diagnosis Condition FHIR Mint ID."""
        return self.mint_id(identifier=condition_identifier, resource_type="Condition")


class SpecimenTransformer(Transformer):
    def __init__(self, session: Session):
        super().__init__(session)
        self.session = session
        self.project_id = 'CDA'
        self.namespace = uuid3(NAMESPACE_DNS, CDA_SITE)

    def fhir_specimen(self, cda_specimen: CDASpecimen, patient: PatientTransformer) -> Specimen:

        fhir_specimen_identifier = self.specimen_identifier(cda_specimen)
        fhir_specimen_id = self.specimen_mintid(fhir_specimen_identifier[0])

        associated_project_value = str(cda_specimen.associated_project)
        if ';' in associated_project_value:
            projects = associated_project_value.split(';')
        else:
            projects = [associated_project_value]

        research_study_refs = []
        project_id_system = f"https://{CDA_SITE}/associated_project"
        for project in projects:
            project = project.strip()
            if project:
                project_id_identifier = Identifier(system=project_id_system, value=project, use="official")
                research_study_id = self.mint_id(identifier=project_id_identifier, resource_type="ResearchStudy")
                research_study_refs.append({"reference": f"ResearchStudy/{research_study_id}"})

        study_extensions = [
            {
                "url": "http://fhir-aggregator.org/fhir/StructureDefinition/part-of-study",
                "valueReference": ref
            }
            for ref in research_study_refs
        ]

        # there are one to many cases
        # project_id_system = "".join([f"https://{CDA_SITE}/", "associated_project"])
        # project_id_identifier = Identifier(**{'system': project_id_system, 'value': str(cda_specimen.associated_project), "use": "official"})
        # research_study_id = self.mint_id(identifier=project_id_identifier, resource_type="ResearchStudy")

        parent = None
        if cda_specimen.derived_from_specimen and cda_specimen.derived_from_specimen != "initial specimen":
            parent_specimen_id_system = "".join([f"https://{CDA_SITE}/", "specimen"])
            parent_specimen_identifier = Identifier(
                **{'system': parent_specimen_id_system, 'value': cda_specimen.derived_from_specimen, "use":"official"})
            parent_specimen_id = self.specimen_mintid(parent_specimen_identifier)
            parent = [{
                "reference": f"Specimen/{parent_specimen_id}"
            }]

        collection = None
        fhir_bodysite = self.specimen_body_structure(cda_specimen, patient, fhir_specimen=None, part_of_study_extensions=study_extensions)
        if fhir_bodysite:
            collection = SpecimenCollection(
                **{
                    "bodySite": CodeableReference(**{"reference": Reference(**{
                        "reference": f"BodyStructure/{fhir_bodysite.id}"
                    })})
                })
            print("of the collection: ", collection, "\n")

        specimen_type = None
        if cda_specimen.source_material_type:
            specimen_type = CodeableConcept(
                **{
                    "coding": [
                        {
                            "system": f"https://{CDA_SITE}/",
                            "code": cda_specimen.source_material_type,
                            "display": cda_specimen.source_material_type
                        }
                    ]
                }
            )

        specimen = Specimen(
            **{
                "id": fhir_specimen_id,
                "identifier": fhir_specimen_identifier,
                "type": specimen_type,
                "subject": {
                    "reference": f"Patient/{patient.id}"
                },
                "collection": collection,
                "extension": study_extensions # has cases where specimen is part of multiple studies
            }
        )

        if parent:
            specimen.parent = parent

        return specimen

    def specimen_observation(self, cda_specimen, patient, _specimen_id, fhir_specimen:Specimen) -> Observation:
        components = []
        if cda_specimen.days_to_collection:
            days_to_collection = self.get_component("days_to_collection", value=cda_specimen.days_to_collection,
                                                    component_type="int",
                                                    system=f"https://{CDA_SITE}")
            if days_to_collection:
                components.append(days_to_collection)

        if cda_specimen.specimen_type:
            specimen_type = self.get_component("specimen_type", value=cda_specimen.specimen_type,
                                               component_type="string",
                                               system=f"https://{CDA_SITE}")
            if specimen_type:
                components.append(specimen_type)

        if cda_specimen.primary_disease_type:
            primary_disease_type = self.get_component("primary_disease_type", value=cda_specimen.primary_disease_type,
                                                      component_type="string",
                                                      system=f"https://{CDA_SITE}")
            if primary_disease_type:
                components.append(primary_disease_type)

        if components:
            observation_identifier = Identifier(
                **{'system': f"https://{CDA_SITE}/specimen_observation", 'value': _specimen_id, "use":"official"})
            observation_id = self.mint_id(identifier=observation_identifier, resource_type="Observation")

            obs = Observation(
                **{
                    "id": observation_id,
                    "identifier": [observation_identifier],
                    "status": "final",
                    "category": [
                        {
                            "coding": [
                                {
                                    "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                                    "code": "laboratory",
                                    "display": "Laboratory"
                                }
                            ]
                        }
                    ],
                    "code": {
                        "coding": [
                            {
                                "system": "http://loinc.org",
                                "code": "81247-9",
                                "display": "Master HL7 genetic variant reporting panel"
                                # TODO: check specimen related codes
                            }
                        ]
                    },
                    "subject": {
                        "reference": f"Patient/{patient.id}"
                    },
                    "specimen": {
                        "reference": f"Specimen/{_specimen_id}"
                    },
                    "focus": [{
                        "reference": f"Specimen/{_specimen_id}"
                    }],
                    "component": components
                }
            )

            extensions = []
            if hasattr(fhir_specimen, "extension"):
                extensions = fhir_specimen.extension
            if extensions:
                obs.extension = extensions

            return obs

    def specimen_body_structure(self, cda_specimen, patient, fhir_specimen:Specimen| None, part_of_study_extensions: list | None) -> BodyStructure:

        if (cda_specimen.anatomical_site and 'Not specified in data' not in cda_specimen.anatomical_site
                and re.match(r"^[^\s]+(\s[^\s]+)*$", cda_specimen.anatomical_site)):

            body_structure_included_structure = []
            # requires content harmonization
            if ":" in cda_specimen.anatomical_site:
                code, display = cda_specimen.anatomical_site.split(":")
                body_structure_included_structure.append(BodyStructureIncludedStructure(**{"structure":
                    {"coding": [{
                        "system": f"https://{CDA_SITE}/",
                        "code": code.strip(),
                        "display": display.strip()
                    }]}
                }))
            elif "," in cda_specimen.anatomical_site and ":" not in cda_specimen.anatomical_site and "NOS" not in cda_specimen.anatomical_site and "Nos" not in cda_specimen.anatomical_site:
                body_sites = cda_specimen.anatomical_site.split(",")
                for body_site in body_sites:
                    body_structure_included_structure.append(BodyStructureIncludedStructure(**{"structure":
                        {"coding": [{
                            "system": f"https://{CDA_SITE}/",
                            "code": body_site.strip(),
                            "display": body_site.strip()
                        }]}
                    }))
            else:
                body_structure_included_structure = [BodyStructureIncludedStructure(**{"structure":
                    {"coding": [{
                        "system": f"https://{CDA_SITE}/",
                        "code": cda_specimen.anatomical_site,
                        "display": cda_specimen.anatomical_site
                    }]}
                })]

            cda_system = "".join([f"https://{CDA_SITE}", ])
            bd_identifier = Identifier(**{'system': cda_system, 'value': str(cda_specimen.anatomical_site), "use":"official"})


            body_structure = BodyStructure(
                **{"id": self.mint_id(identifier=bd_identifier, resource_type="BodyStructure"),
                   "identifier": [bd_identifier],
                   "includedStructure": body_structure_included_structure,
                   "patient": {
                       "reference": f"Patient/{patient.id}"

                   }})

            extensions = []
            if fhir_specimen:
                if hasattr(fhir_specimen, "extension"):
                    extensions = fhir_specimen.extension
                if extensions:
                    body_structure.extension = extensions
            elif part_of_study_extensions:
                body_structure.extension = part_of_study_extensions

                return body_structure

            return body_structure

    @staticmethod
    def specimen_identifier(cda_specimen: CDASpecimen) -> list[Identifier]:
        """CDA Specimen FHIR Identifier."""
        specimen_id_system = "".join([f"https://{CDA_SITE}/", "specimen"])
        specimen_identifier = Identifier(**{'system': specimen_id_system, 'value': cda_specimen.id, "use": "official"})
        return [specimen_identifier]

    def specimen_mintid(self, specimen_identifier: Identifier) -> str:
        """CDA Specimen FHIR Mint ID."""
        return self.mint_id(identifier=specimen_identifier, resource_type="Specimen")


class DocumentReferenceTransformer(Transformer):
    def __init__(self, session: Session, patient_transformer: PatientTransformer, specimen_transfomer: SpecimenTransformer):
        super().__init__(session)
        self.session = session
        self.project_id = 'CDA'
        self.namespace = uuid3(NAMESPACE_DNS, CDA_SITE)
        self.patient_transformer = patient_transformer
        self.specimen_transformer = specimen_transfomer

    def fhir_document_reference(self, cda_file: CDAFile, patients: list[CDASubject],
                                specimens: list[CDASpecimen]) -> dict:
        category = []
        group = None

        _doc_ref_identifier = Identifier(
            system=f"https://{CDA_SITE}/id",
            value=cda_file.id,
            use="official"
        )
        _doc_ref_alias_identifier = Identifier(
            system=f"https://{CDA_SITE}/alias",
            value=str(cda_file.integer_id_alias),
            use="secondary"
        )
        _doc_ref_dbgap_identifier = Identifier(
            system=f"https://{CDA_SITE}/dbgap_accession_number",
            value=cda_file.dbgap_accession_number,
            use="secondary"
        )

        _doc_ref_id = self.mint_id(identifier=_doc_ref_identifier, resource_type="DocumentReference")
        assert _doc_ref_id, "DocumentReference must have a mint id."
        assert _doc_ref_identifier, "DocumentReference must have an Identifier."

        specimen_fhir_ids = []
        specimen_cda_ids = []
        subject_reference = None

        if specimens:
            for specimen in specimens:
                specimen_identifiers = self.specimen_transformer.specimen_identifier(specimen)
                fhir_specimen_id = self.specimen_transformer.specimen_mintid(specimen_identifiers[0])
                if fhir_specimen_id and self.is_valid_uuid(fhir_specimen_id):
                    specimen_cda_ids.append(specimen.id)
                    specimen_fhir_ids.append(fhir_specimen_id)
            if specimen_fhir_ids:
                specimen_references = [Reference(reference=f"Specimen/{s}") for s in specimen_fhir_ids]
                if len(specimen_references) > 1:
                    group_identifier = Identifier(
                        system=f"https://{CDA_SITE}/specimen_group",
                        value="/".join([_doc_ref_identifier.value] + specimen_cda_ids),
                        use="secondary"
                    )
                    group = self.fhir_group(
                        member_ids=specimen_fhir_ids,
                        _type="specimen",
                        _identifier=group_identifier,
                        extensions=None
                    )
                    subject_reference = Reference(reference=f"Group/{group.id}")
                elif len(specimen_references) == 1:
                    subject_reference = specimen_references[0]
                else:
                    logging.error(
                        f"CDA file ID: {cda_file.id} - specimen FHIR ids {specimen_fhir_ids} are not valid mint_ids. Skipping transformation.")

        if not specimens and patients:
            patient_fhir_ids = []
            for subject in patients:
                patient_identifiers = self.patient_transformer.patient_identifier(subject)
                fhir_patient_id = self.patient_transformer.patient_mintid(patient_identifiers[0])
                if fhir_patient_id and self.is_valid_uuid(fhir_patient_id):
                    patient_fhir_ids.append(fhir_patient_id)
            if len(patient_fhir_ids) == 1:
                subject_reference = Reference(reference=f"Patient/{patient_fhir_ids[0]}")
            elif len(patient_fhir_ids) > 1:
                group = self.fhir_group(
                    member_ids=patient_fhir_ids,
                    _type="patient",
                    _identifier=_doc_ref_identifier,
                    extensions=None
                )
                subject_reference = Reference(reference=f"Group/{group.id}")
            else:
                logging.error(
                    f"CDA file ID: {cda_file.id} - patient FHIR ids {patient_fhir_ids} are not valid mint_ids. Skipping transformation.")
                return {"DocumentReference": None, "Group": None}

        _type = None
        if cda_file.file_format:
            _type = CodeableConcept(
                coding=[{
                    "system": f"https://{CDA_SITE}/file_format",
                    "code": cda_file.file_format,
                    "display": cda_file.file_format
                }]
            )
        if cda_file.data_category:
            category.append(CodeableConcept(
                coding=[{
                    "system": f"https://{CDA_SITE}/data_category",
                    "code": cda_file.data_category,
                    "display": cda_file.data_category
                }]
            ))
        if cda_file.data_type:
            category.append(CodeableConcept(
                coding=[{
                    "system": f"https://{CDA_SITE}/data_type",
                    "code": cda_file.data_type,
                    "display": cda_file.data_type
                }]
            ))
        if cda_file.data_modality:
            category.append(CodeableConcept(
                coding=[{
                    "system": f"https://{CDA_SITE}/data_modality",
                    "code": cda_file.data_modality,
                    "display": cda_file.data_modality
                }]
            ))
        if cda_file.imaging_modality:
            category.append(CodeableConcept(
                coding=[{
                    "system": f"https://{CDA_SITE}/imaging_modality",
                    "code": cda_file.imaging_modality,
                    "display": cda_file.imaging_modality
                }]
            ))
        if cda_file.imaging_series:
            category.append(CodeableConcept(
                coding=[{
                    "system": f"https://{CDA_SITE}/imaging_series",
                    "code": cda_file.imaging_series,
                    "display": cda_file.imaging_series
                }]
            ))

        part_of_extensions = []
        candidate_subject = None
        if patients:
            candidate_subject = patients[0]
        elif specimens:
            candidate_subject = self.session.execute(
                select(CDASubject).filter_by(id=specimens[0].derived_from_subject)
            ).scalar_one_or_none()
        if candidate_subject:
            self.get_part_of_study_extension(candidate_subject, extensions=part_of_extensions)

        if group and part_of_extensions:
            if specimens:
                group = self.fhir_group(
                    member_ids=specimen_fhir_ids,
                    _type="specimen",
                    _identifier=group_identifier,
                    extensions=part_of_extensions
                )
            else:
                group = self.fhir_group(
                    member_ids=patient_fhir_ids,
                    _type="patient",
                    _identifier=_doc_ref_identifier,
                    extensions=part_of_extensions
                )
            subject_reference = Reference(reference=f"Group/{group.id}")

        doc_ref = DocumentReference(
            **{
                "id": _doc_ref_id,
                "identifier": [_doc_ref_identifier, _doc_ref_dbgap_identifier, _doc_ref_alias_identifier],
                "status": "current",
                "version": "1",
                "subject": subject_reference,
                "type": _type,
                "category": category,
                "content": [
                    {
                        "attachment": self.fhir_attachment(cda_file),
                        "profile": [{"valueUri": cda_file.drs_uri}]
                    }
                ],
                **({"extension": part_of_extensions} if part_of_extensions else {})
            }
        )
        return {"DocumentReference": doc_ref, "Group": group}

    @staticmethod
    def fhir_attachment(cda_file: CDAFile) -> Attachment:

        attachment = Attachment(**{
            "contentType": cda_file.data_type,
            "title": cda_file.label,
            "size": cda_file.byte_size,
            "hash": cda_file.checksum.rstrip("\n")
        })
        return attachment

    def fhir_group(self, member_ids: list, _type: str, _identifier: Identifier, extensions: list = None) -> Group:
        ref_type = "Patient" if _type == "patient" else "Specimen" if _type == "specimen" else None
        assert ref_type, "Group member Reference FHIR type must be defined"

        _members = [GroupMember(entity=Reference(reference=f"{ref_type}/{m}")) for m in member_ids]
        _identifier.value = "_".join([_identifier.value, _type])
        group_id = self.mint_id(identifier=_identifier, resource_type="Group")
        group_data = {
            "id": group_id,
            "identifier": [_identifier],
            "membership": "definitional",
            "member": _members,
            "type": _type,
        }
        if extensions:
            group_data["extension"] = extensions
        return Group(**group_data)


class MedicationAdministrationTransformer(Transformer):
    def __init__(self, session: Session, patient_transformer: PatientTransformer):
        super().__init__(session)
        self.patient_transformer = patient_transformer
        self.session = session
        self.project_id = 'CDA'
        self.namespace = uuid3(NAMESPACE_DNS, CDA_SITE)
        self.SYSTEM_SNOME = 'http://snomed.info/sct'
        self.SYSTEM_LOINC = 'http://loinc.org'
        self.SYSTEM_chEMBL = 'https://www.ebi.ac.uk/chembl'

    @staticmethod
    def fetch_chembl_data(compounds: list, limit: int) -> tuple:
        db_file_path = "data/chembl_34.db"
        def get_chembl_compound_info(db_file_path, drug_names: list, _limit=limit) -> list:
            """Query Chembl COMPOUND_RECORDS by COMPOUND_NAME for FHIR Substance"""
            if len(drug_names) == 1:
                _drug_names = f"('{drug_names[0].upper()}')"
            else:
                _drug_names = tuple([x.upper() for x in drug_names])

            query = f"""
            SELECT DISTINCT 
                a.CHEMBL_ID,
                c.STANDARD_INCHI,
                c.CANONICAL_SMILES,
                cr.COMPOUND_NAME
            FROM 
                MOLECULE_DICTIONARY as a
            LEFT JOIN 
                COMPOUND_STRUCTURES as c ON a.MOLREGNO = c.MOLREGNO
            LEFT JOIN 
                ACTIVITIES as p ON a.MOLREGNO = p.MOLREGNO
            LEFT JOIN 
                compound_records as cr ON a.MOLREGNO = cr.MOLREGNO
            LEFT JOIN
                source as sr ON cr.SRC_ID = sr.SRC_ID
            WHERE cr.COMPOUND_NAME IN {_drug_names}
            LIMIT {str(_limit)};
            """

            conn = sqlite3.connect(db_file_path)
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            conn.close()

            return rows

        chembl_data = get_chembl_compound_info(db_file_path, compounds, limit)
        exists = bool(chembl_data)  # true if data is found

        return exists, chembl_data

    @staticmethod
    def create_substance_definition_representations(drug_data: list) -> list:
        representations = []
        for row in drug_data:
            if 'STANDARD_INCHI' in row and row['STANDARD_INCHI'] is not None:
                representations.append(SubstanceDefinitionStructureRepresentation(
                    **{"representation": row['STANDARD_INCHI'],
                       "format": CodeableConcept(**{"coding": [{"code": "InChI",
                                                                "system": 'http://hl7.org/fhir/substance-representation-format',
                                                                "display": "InChI"}]})}))

            if 'CANONICAL_SMILES' in row and row['CANONICAL_SMILES'] is not None:
                representations.append(SubstanceDefinitionStructureRepresentation(
                    **{"representation": row['CANONICAL_SMILES'],
                       "format": CodeableConcept(**{"coding": [{"code": "SMILES",
                                                                "system": 'http://hl7.org/fhir/substance-representation-format',
                                                                "display": "SMILES"}]})}))
        return representations

    def create_substance_definition(self, compound_name: str, representations: list) -> SubstanceDefinition:
        sub_def_identifier = Identifier(**{"system": self.SYSTEM_chEMBL, "value": compound_name, "use": "official"})
        sub_def_id = self.mint_id(identifier=sub_def_identifier, resource_type="SubstanceDefinition")

        return SubstanceDefinition(**{"id": sub_def_id,
                                      "identifier": [sub_def_identifier],
                                      "structure": SubstanceDefinitionStructure(**{"representation": representations}),
                                      "name": [SubstanceDefinitionName(**{"name": compound_name})]
                                      })

    def create_substance(self, compound_name: str, substance_definition: SubstanceDefinition) -> Substance:
        code = None
        if substance_definition:
            code = CodeableReference(
                **{"concept": CodeableConcept(**{"coding": [
                    {"code": compound_name, "system": "/".join([self.SYSTEM_chEMBL, "compound_name"]),
                     "display": compound_name}]}),
                   "reference": Reference(**{"reference": f"SubstanceDefinition/{substance_definition.id}"})})

        sub_identifier = Identifier(
            **{"system": self.SYSTEM_chEMBL, "value": compound_name, "use": "official"})
        sub_id = self.mint_id(identifier=sub_identifier, resource_type="Substance")

        return Substance(**{"id": sub_id,
                            "identifier": [sub_identifier],
                            "instance": True,  # place-holder
                            "category": [CodeableConcept(**{"coding": [{"code": "drug",
                                                                        "system": "http://terminology.hl7.org/CodeSystem/substance-category",
                                                                        "display": "Drug or Medicament"}]})],
                            "code": code})

    def create_medication(self, compound_name: Optional[str], treatment_type: Optional[str],
                          _substance: Optional[Substance]) -> Medication:

        if compound_name:
            if ":" in compound_name:
                compound_name.replace(":", "_")
            code = CodeableConcept(**{"coding": [
                {"code": compound_name, "system": "/".join([self.SYSTEM_chEMBL, "compound_name"]),
                 "display": compound_name}]})

            med_identifier = Identifier(
                **{"system": self.SYSTEM_chEMBL, "value": compound_name, "use": "official"})
        else:
            if ":" in treatment_type:
                treatment_type.replace(":", "_")

            code = CodeableConcept(**{
                "coding": [{"code": treatment_type,
                            "system": "/".join([f"https://{CDA_SITE}", "treatment_type"]),  # TODO: change
                            "display": treatment_type}]})

            med_identifier = Identifier(
                **{"system": f"https://{CDA_SITE}/", "value": treatment_type, "use": "official"})

        med_id = self.mint_id(identifier=med_identifier, resource_type="Medication")

        ingredients = []
        if _substance:
            ingredients.append(MedicationIngredient(**{
                "item": CodeableReference(
                    **{"reference": Reference(**{"reference": f"Substance/{_substance.id}"})})}))

        return Medication(**{"id": med_id,
                             "identifier": [med_identifier],
                             "code": code,
                             "ingredient": ingredients})

    def create_medication_administration(self, treatment: CDATreatment, subject: CDASubject, medication: Optional[Medication]) -> Optional[MedicationAdministration]:
        """
        Creates a MedicationAdministration resource. Defaults to SNOMED "Unknown" if no Medication is provided.
        """
        assert subject, "Medication Administration requires patient information"

        patient_identifiers = self.patient_transformer.patient_identifier(subject)
        fhir_patient_id = self.patient_transformer.patient_mintid(patient_identifiers[0])

        medication_name = treatment.therapeutic_agent.upper() if treatment.therapeutic_agent else "Unknown"

        medication_admin_id = self.mint_id(identifier=Identifier(**{"system": "/".join([f"https://{CDA_SITE}", "treatment"]),
                                                                    "use": "official",
                                                                    "value": f"{fhir_patient_id}-{medication_name}"}),
                                           resource_type="MedicationAdministration")

        # default value filler - required by FHIR
        timing = {"repeat": {"boundsRange": {
                    "low": {"value": int(treatment.days_to_treatment_start)} if treatment.days_to_treatment_start else {"value": 0},
                    "high": {"value": int(treatment.days_to_treatment_end)} if treatment.days_to_treatment_end else {"value": 1}}}}

        medication_code = "261665006" if medication is None else medication_name
        medication_display = "Unknown" if medication is None else medication_name
        medication_reference = {"reference": f"Medication/{medication.id}"} if medication else None

        extensions = []
        _extensions = []
        self.get_part_of_study_extension(subject, extensions= extensions)
        if isinstance(extensions[0], dict):
            for e in extensions:
                _extensions.append(Extension(**e))
        else:
            _extensions = extensions

        med_admin = {
            "id": medication_admin_id,
            "identifier": [{
                "system": "/".join([f"https://{CDA_SITE}", "treatment"]),
                "use": "official",
                "value": f"{fhir_patient_id}-{medication_name}"
            }],
            "status": "completed" if treatment.days_to_treatment_end else "in-progress",
            "medication": {"concept": {
                    "coding": [{
                        "code": medication_code,
                        "system": f"https://{CDA_SITE}/medication",
                        "display": medication_display
                    }]
                },
                "reference": medication_reference
            },
            "subject": {"reference": f"Patient/{fhir_patient_id}"},
            "occurenceTiming": timing # there isn't an equivalent in R4
        }
        md = MedicationAdministration(**med_admin)
        if _extensions and md:
            md.extension = _extensions
        return md


class MutationTransformer(Transformer):
    def __init__(self, session: Session, patient_transformer: PatientTransformer):
        super().__init__(session)
        self.patient_transformer = patient_transformer
        self.session = session
        self.project_id = 'CDA'
        self.namespace = uuid3(NAMESPACE_DNS, CDA_SITE)
        self.SYSTEM_SNOME = 'http://snomed.info/sct'
        self.SYSTEM_LOINC = 'http://loinc.org'
        self.SYSTEM_chEMBL = 'https://www.ebi.ac.uk/chembl'

    def create_mutation_observation(self, mutation: CDAMutation, subject: CDASubject) -> Observation:
        """creates an Observation resource for a given mutation and subject."""
        assert mutation.id, f"mutations must have an id"
        assert subject, "Mutation results requires subject information"

        patient_identifiers = self.patient_transformer.patient_identifier(subject)
        fhir_patient_id = self.patient_transformer.patient_mintid(patient_identifiers[0])

        mutation_identifier = Identifier(**{"system": self.SYSTEM_chEMBL, "value": mutation.id, "use": "official"})
        mutation_id = self.mint_id(identifier=mutation_identifier, resource_type="Observation")
        components = []

        for field_name, field_value in vars(mutation).items():
            if field_name.startswith("_") or field_name in ["id", "integer_id_alias"] or field_value is None:
                continue

            if isinstance(field_value, int):
                component_type = "int"
            elif isinstance(field_value, bool):
                component_type = "bool"
            else:
                component_type = "string"

            component = self.get_component(
                key=field_name,
                value=field_value,
                component_type=component_type,
                system=f"https://{CDA_SITE}/{field_name}"
            )
            if component:
                components.append(component)

        extensions = []
        self.get_part_of_study_extension(subject, extensions=extensions)

        _extensions = []
        self.get_part_of_study_extension(subject, extensions=extensions)
        if isinstance(extensions[0], dict):
            for e in extensions:
                _extensions.append(Extension(**e))
        else:
            _extensions = extensions

        obs = Observation(
            **{
                "id": mutation_id,
                "identifier": [mutation_identifier],
                "status": "final",
                "category": [
                    {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                                "code": "laboratory",
                                "display": "Laboratory"
                            }
                        ]
                    }
                ],
                "code": {
                    "coding": [
                        {
                            "system": "http://purl.obolibrary.org/obo/",
                            "code": "NCIT_C164934",
                            "display": "Performed Genetic Observation Result Mutation Type Code"
                        }
                    ]
                },
                "subject": {
                    "reference": f"Patient/{fhir_patient_id}"
                },
                "focus": [{
                    "reference": f"Patient/{fhir_patient_id}" #TODO: add specimen
                }],
                "component": components
            }
        )
        obs.extension = _extensions
        return obs
