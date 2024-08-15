import re
from fhir.resources.patient import Patient
from fhir.resources.specimen import Specimen, SpecimenCollection
from fhir.resources.identifier import Identifier
from fhir.resources.extension import Extension
from fhir.resources.observation import Observation
from fhir.resources.researchstudy import ResearchStudy
from fhir.resources.bodystructure import BodyStructure, BodyStructureIncludedStructure
from fhir.resources.researchsubject import ResearchSubject
from fhir.resources.codeableconcept import CodeableConcept
from fhir.resources.reference import Reference
from fhir.resources.codeablereference import CodeableReference
from fhir.resources.condition import Condition, ConditionStage
from sqlalchemy.orm import Session
from cda2fhir.cdamodels import CDASubject, CDAResearchSubject, CDASubjectResearchSubject, CDASubjectProject, \
    CDADiagnosis, CDASpecimen, CDAResearchSubjectSpecimen
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

    def program_research_study(self, name) -> ResearchStudy:
        """create top level program FHIR ResearchStudy"""
        _program_identifier = Identifier(**{"system": "".join(["https://cda.readthedocs.io/", "system"]), "value": name})
        _id = self.mint_id(identifier=_program_identifier, resource_type="ResearchStudy")
        research_study = ResearchStudy(
            **{
                'id': _id,
                'identifier': [_program_identifier],
                'status': 'active',
                'name': name
            }
        )
        if research_study:
            return research_study


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
        # human_subjects = [subject.query.filter(subject.species == 'Homo sapiens') for subject in subjects] # filtered in load
        # print('**** human_subjects: ', human_subjects.__getitem__(0), len(human_subjects))
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

            if research_subject.primary_diagnosis_condition and re.match(r"^[^\s]+(\s[^\s]+)*$",
                                                                         research_subject.primary_diagnosis_condition):
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


class ResearchSubjectTransformer(Transformer):
    def __init__(self, session: Session):
        super().__init__(session)
        self.session = session
        self.project_id = 'CDA'
        self.namespace = uuid3(NAMESPACE_DNS, 'cda.readthedocs.io')

    def research_subject(self, cda_research_subject: CDAResearchSubject, patient: PatientTransformer,
                         research_study: ResearchStudyTransformer) -> ResearchSubject:
        rs_identifier = self.research_subject_identifier(cda_research_subject)
        _id = self.research_subject_mintid(rs_identifier[0])

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
                }
            })

        return research_subject

    @staticmethod
    def research_subject_identifier(cda_research_subject: CDAResearchSubject) -> list[Identifier]:
        """CDA research subject FHIR Identifier."""
        research_subject_id_system = "".join(["https://cda.readthedocs.io/", "researchsubject"])
        research_subject_id_identifier = Identifier(
            **{'system': research_subject_id_system, 'value': cda_research_subject.id})

        return [research_subject_id_identifier]

    def research_subject_mintid(self, rs_identifier: Identifier) -> str:
        """CDA research subject FHIR Mint ID."""
        return self.mint_id(identifier=rs_identifier, resource_type="ResearchSubject")


class ConditionTransformer(Transformer):
    def __init__(self, session: Session):
        super().__init__(session)
        self.session = session
        self.project_id = 'CDA'
        self.namespace = uuid3(NAMESPACE_DNS, 'cda.readthedocs.io')

    def condition(self, diagnosis: CDADiagnosis, patient: PatientTransformer) -> Condition:
        if diagnosis.primary_diagnosis is None or not re.match(r"^[^\s]+(\s[^\s]+)*$", diagnosis.primary_diagnosis):
            print(
                f"---- SKIPPING DIAGNOSIS {diagnosis.id} due to missing or invalid primary diagnosis string per fhir standards.")
            return None

        condition_identifier = self.condition_identifier(diagnosis)
        condition_id = self.condition_mintid(condition_identifier[0])

        _stage_display = None
        if diagnosis.stage:
            _stage_display = diagnosis.stage
        elif diagnosis.grade:
            _stage_display = diagnosis.grade

        stage_summary = None
        if _stage_display:
            stage_summary = CodeableConcept(**{
                "coding": [
                    {
                        "system": "https://cda.readthedocs.io/",
                        "code": _stage_display,
                        "display": _stage_display
                    }
                ]
            })

        onset = None
        if diagnosis.age_at_diagnosis:
            onset = str(diagnosis.age_at_diagnosis)

        if _stage_display:
            _condition_observation = self.condition_observation(diagnosis, _stage_display, patient, condition_id)

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
                "code": CodeableConcept(
                    **{
                        "coding": [{"system": "https://cda.readthedocs.io/",
                                    "code": code,
                                    "display": display}]}
                ),
                "onsetString": onset,
                "stage": stage
            }
        )
        return _condition

    def condition_observation(self, diagnosis, display, patient, _condition_id) -> Observation:
        condition_id_system = "".join(["https://cda.readthedocs.io/", "diagnosis"])
        observation_identifier = Identifier(**{'system': condition_id_system, 'value': diagnosis.id})
        observation_id = self.mint_id(identifier=observation_identifier, resource_type="Observation")

        observation_code = None
        if diagnosis.stage:
            observation_code = {
                "system": "https://thesaurus.cancer.gov",
                "code": "C177556",
                "display": "AJCC v8 Pathologic Stage"
            }
        elif diagnosis.grade:
            observation_code = {
                "system": "http://loinc.org",
                "code": "33732-9",
                "display": "Histology grade [Identifier] in Cancer specimen"
            }
        if observation_code is None:
            print(f"Skipping Observation for diagnosis condition {diagnosis.id}")
            return None

        observation = Observation(
            **{
                "id": observation_id,
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
                    "coding": [observation_code]
                },
                "subject": {
                    "reference": f"Patient/{patient.id}"
                },
                "focus": [{
                    "reference": f"Condition/{_condition_id}"
                }],
                "valueCodeableConcept": {
                    "coding": [{
                        "system": "https://cda.readthedocs.io/",
                        "code": display,
                        "display": display
                    }
                    ]
                }
            }
        )
        return observation

    @staticmethod
    def condition_identifier(cda_diagnosis: CDADiagnosis) -> list[Identifier]:
        """CDA Diagnosis Condition FHIR Identifier."""
        condition_id_system = "".join(["https://cda.readthedocs.io/", "diagnosis"])
        condition_identifier = Identifier(**{'system': condition_id_system, 'value': cda_diagnosis.id})
        return [condition_identifier]

    def condition_mintid(self, condition_identifier: Identifier) -> str:
        """CDA Diagnosis Condition FHIR Mint ID."""
        return self.mint_id(identifier=condition_identifier, resource_type="Condition")


class SpecimenTransformer(Transformer):
    def __init__(self, session: Session):
        super().__init__(session)
        self.session = session
        self.project_id = 'CDA'
        self.namespace = uuid3(NAMESPACE_DNS, 'cda.readthedocs.io')

    def fhir_specimen(self, cda_specimen: CDASpecimen, patient: PatientTransformer) -> Specimen:

        fhir_specimen_identifier = self.specimen_identifier(cda_specimen)
        fhir_specimen_id = self.specimen_mintid(fhir_specimen_identifier[0])

        parent = []
        if cda_specimen.derived_from_specimen:
            parent_specimen_id_system = "".join(["https://cda.readthedocs.io/", "specimen"])
            parent_specimen_identifier = Identifier(
                **{'system': parent_specimen_id_system, 'value': cda_specimen.derived_from_specimen})
            parent_specimen_id = self.specimen_mintid(parent_specimen_identifier)
            parent = [{
                "reference": f"Specimen/{parent_specimen_id}"
            }]

        collection = None
        fhir_bodysite = self.specimen_body_structure(cda_specimen, patient)
        if fhir_bodysite:
            collection = SpecimenCollection(
                **{
                    "bodySite": CodeableReference(** {"reference": Reference(**{
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
                            "system": "https://cda.readthedocs.io/",
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
                "parent": parent,
                "collection": collection,
            }
        )

        return specimen

    @staticmethod
    def get_component(key, value=None, component_type=None,
                      system="https://cda.readthedocs.io"):
        print("SPECIMEN Component :", key, value, component_type)
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

    def specimen_observation(self, cda_specimen, patient, _specimen_id) -> Observation:
        components = []
        if cda_specimen.days_to_collection:
            days_to_collection = self.get_component("days_to_collection", value=cda_specimen.days_to_collection, component_type="int",
                          system="https://cda.readthedocs.io")
            if days_to_collection:
                components.append(days_to_collection)

        if cda_specimen.specimen_type:
            specimen_type = self.get_component("specimen_type", value=cda_specimen.specimen_type, component_type="string",
                          system="https://cda.readthedocs.io")
            if specimen_type:
                components.append(specimen_type)

        if cda_specimen.primary_disease_type:
            primary_disease_type = self.get_component("primary_disease_type", value=cda_specimen.primary_disease_type, component_type="string",
                          system="https://cda.readthedocs.io")
            if primary_disease_type:
                components.append(primary_disease_type)

        if components:
            observation_identifier = Identifier(**{'system': "https://cda.readthedocs.io/specimen_observation", 'value': _specimen_id})
            observation_id = self.mint_id(identifier=observation_identifier, resource_type="Observation")

            obs = Observation(
                **{
                    "id": observation_id,
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
                                "display": "Master HL7 genetic variant reporting panel" # TODO: check specimen related codes
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
            return obs

    def specimen_body_structure(self, cda_specimen, patient) -> BodyStructure:

        if (cda_specimen.anatomical_site and 'Not specified in data' not in cda_specimen.anatomical_site
                and re.match(r"^[^\s]+(\s[^\s]+)*$", cda_specimen.anatomical_site)):

            body_structure_included_structure = []
            # requires content harmonization
            if ":" in cda_specimen.anatomical_site:
                code, display = cda_specimen.anatomical_site.split(":")
                body_structure_included_structure.append(BodyStructureIncludedStructure(**{"structure":
                    {"coding": [{
                        "system": "https://cda.readthedocs.io/",
                        "code": code.strip(),
                        "display": display.strip()
                    }]}
                }))
            elif "," in cda_specimen.anatomical_site and ":" not in cda_specimen.anatomical_site and "NOS" not in cda_specimen.anatomical_site and "Nos" not in cda_specimen.anatomical_site:
                body_sites = cda_specimen.anatomical_site.split(",")
                for body_site in body_sites:
                    body_structure_included_structure.append(BodyStructureIncludedStructure(**{"structure":
                        {"coding": [{
                            "system": "https://cda.readthedocs.io/",
                            "code": body_site.strip(),
                            "display": body_site.strip()
                        }]}
                    }))
            else:
                body_structure_included_structure = [BodyStructureIncludedStructure(**{"structure":
                    {"coding": [{
                        "system": "https://cda.readthedocs.io/",
                        "code": cda_specimen.anatomical_site,
                        "display": cda_specimen.anatomical_site
                    }]}
                })]

            cda_system = "".join(["https://cda.readthedocs.io", ])
            bd_identifier = Identifier(**{'system': cda_system, 'value': str(cda_specimen.anatomical_site)})

            body_structure = BodyStructure(
                **{"id": self.mint_id(identifier=bd_identifier, resource_type="BodyStructure"),
                   "includedStructure": body_structure_included_structure,
                   "patient": {
                       "reference": f"Patient/{patient.id}"
                   }})

            return body_structure

    @staticmethod
    def specimen_identifier(cda_specimen: CDASpecimen) -> list[Identifier]:
        """CDA Specimen FHIR Identifier."""
        specimen_id_system = "".join(["https://cda.readthedocs.io/", "specimen"])
        specimen_identifier = Identifier(**{'system': specimen_id_system, 'value': cda_specimen.id})
        return [specimen_identifier]

    def specimen_mintid(self, specimen_identifier: Identifier) -> str:
        """CDA Specimen FHIR Mint ID."""
        return self.mint_id(identifier=specimen_identifier, resource_type="Specimen")
