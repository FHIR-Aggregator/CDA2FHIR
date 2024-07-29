from typing import Optional, List
from sqlalchemy import ForeignKey, Integer, String, create_engine
from sqlalchemy.orm import Mapped, mapped_column, relationship, DeclarativeBase, QueryPropertyDescriptor
from sqlalchemy.orm import sessionmaker, scoped_session
from pathlib import Path
import importlib.resources

DATABASE_PATH = str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'cda_data.db'))
DATABASE_URL = f'sqlite:////{DATABASE_PATH}'

e = create_engine(DATABASE_URL, echo=True)
Session = scoped_session(sessionmaker(e))


class Base(DeclarativeBase):
    pass


class CDASubject(Base):
    __tablename__ = 'subject'
    query: QueryPropertyDescriptor = Session.query_property()
    id: Mapped[str] = mapped_column(String, primary_key=True)
    species: Mapped[Optional[str]] = mapped_column(String)
    sex: Mapped[Optional[str]] = mapped_column(String)
    race: Mapped[Optional[str]] = mapped_column(String)
    ethnicity: Mapped[Optional[str]] = mapped_column(String)
    days_to_birth: Mapped[Optional[int]] = mapped_column(Integer)
    vital_status: Mapped[Optional[str]] = mapped_column(String)
    days_to_death: Mapped[Optional[int]] = mapped_column(Integer)
    cause_of_death: Mapped[Optional[str]] = mapped_column(String)
    integer_id_alias: Mapped[Optional[int]] = mapped_column(Integer)
    researchsubject_subjects: Mapped[List["CDASubjectResearchSubject"]] = relationship(
        back_populates="subject"
    )
    subject_alias_relation: Mapped[List["CDASubjectAlias"]] = relationship(
        back_populates="subject_alias_relations"
    )


class CDAResearchSubject(Base):
    __tablename__ = 'researchsubject'
    query: QueryPropertyDescriptor = Session.query_property()
    id: Mapped[str] = mapped_column(String, primary_key=True)
    member_of_research_project: Mapped[Optional[str]] = mapped_column(String)
    primary_diagnosis_condition: Mapped[Optional[str]] = mapped_column(String)
    primary_diagnosis_site: Mapped[Optional[str]] = mapped_column(String)
    integer_id_alias: Mapped[Optional[int]] = mapped_column(Integer)
    subject_researchsubjects: Mapped[List["CDASubjectResearchSubject"]] = relationship(
        back_populates="researchsubject"
    )
    diagnosis_researchsubjects: Mapped[List["CDAResearchSubjectDiagnosis"]] = relationship(
        back_populates="researchsubject"
    )
    treatment_researchsubjects: Mapped[List["CDAResearchSubjectTreatment"]] = relationship(
        back_populates="researchsubject"
    )
    # specimen_researchsubjects: Mapped[List["CDAResearchSubjectSpecimen"]] = relationship(
    #    back_populates="researchsubject"
    # )


class CDASubjectResearchSubject(Base):
    __tablename__ = 'subject_researchsubject'
    query: QueryPropertyDescriptor = Session.query_property()
    subject_id: Mapped[str] = mapped_column(ForeignKey("subject.id"), primary_key=True)
    researchsubject_id: Mapped[str] = mapped_column(ForeignKey("researchsubject.id"), primary_key=True)
    subject: Mapped["CDASubject"] = relationship(
        back_populates="researchsubject_subjects"
    )
    researchsubject: Mapped["CDAResearchSubject"] = relationship(
        back_populates="subject_researchsubjects"
    )


class CDASubjectAlias(Base):
    __tablename__ = 'subject_alias_table'
    query: QueryPropertyDescriptor = Session.query_property()
    subject_id: Mapped[str] = mapped_column(ForeignKey("subject.id"), primary_key=True)
    subject_alias: Mapped[int] = mapped_column(Integer, primary_key=True)
    subject_alias_relations: Mapped["CDASubject"] = relationship(
        back_populates="subject_alias_relation"
    )


class CDADiagnosis(Base):
    __tablename__ = 'diagnosis'
    query: QueryPropertyDescriptor = Session.query_property()
    id: Mapped[str] = mapped_column(String, primary_key=True)
    primary_diagnosis: Mapped[Optional[str]] = mapped_column(String)
    age_at_diagnosis: Mapped[Optional[int]] = mapped_column(Integer)
    morphology: Mapped[Optional[str]] = mapped_column(String)
    stage: Mapped[Optional[str]] = mapped_column(String)
    grade: Mapped[Optional[str]] = mapped_column(String)
    method_of_diagnosis: Mapped[Optional[str]] = mapped_column(String)
    integer_id_alias: Mapped[Optional[int]] = mapped_column(Integer)
    researchsubject_diagnoses: Mapped[List["CDAResearchSubjectDiagnosis"]] = relationship(
        back_populates="diagnosis"
    )


class CDAResearchSubjectDiagnosis(Base):
    __tablename__ = 'researchsubject_diagnosis'
    query: QueryPropertyDescriptor = Session.query_property()
    researchsubject_id: Mapped[str] = mapped_column(ForeignKey('researchsubject.id'), primary_key=True)
    diagnosis_id: Mapped[str] = mapped_column(ForeignKey('diagnosis.id'), primary_key=True)
    diagnosis: Mapped["CDADiagnosis"] = relationship(
        back_populates="researchsubject_diagnoses"
    )
    researchsubject: Mapped["CDAResearchSubject"] = relationship(
        back_populates="diagnosis_researchsubjects"
    )


class CDATreatment(Base):
    __tablename__ = 'treatment'
    query: QueryPropertyDescriptor = Session.query_property()
    id: Mapped[str] = mapped_column(String, primary_key=True)
    treatment_type: Mapped[Optional[str]] = mapped_column(String)
    treatment_outcome: Mapped[Optional[str]] = mapped_column(String)
    days_to_treatment_start: Mapped[Optional[int]] = mapped_column(Integer)
    days_to_treatment_end: Mapped[Optional[int]] = mapped_column(Integer)
    therapeutic_agent: Mapped[Optional[str]] = mapped_column(String)
    treatment_anatomic_site: Mapped[Optional[str]] = mapped_column(String)
    treatment_effect: Mapped[Optional[str]] = mapped_column(String)
    treatment_end_reason: Mapped[Optional[str]] = mapped_column(String)
    number_of_cycles: Mapped[Optional[str]] = mapped_column(String)
    integer_id_alias: Mapped[Optional[int]] = mapped_column(Integer)
    researchsubject_treatments: Mapped[List["CDAResearchSubjectTreatment"]] = relationship(
        back_populates="treatment"
    )


class CDAResearchSubjectTreatment(Base):
    __tablename__ = 'researchsubject_treatment'
    query: QueryPropertyDescriptor = Session.query_property()
    researchsubject_id: Mapped[str] = mapped_column(ForeignKey('researchsubject.id'), primary_key=True)
    treatment_id: Mapped[str] = mapped_column(ForeignKey('treatment.id'), primary_key=True)
    researchsubject: Mapped["CDAResearchSubject"] = relationship(
        back_populates="treatment_researchsubjects"
    )
    treatment: Mapped["CDATreatment"] = relationship(
        back_populates="researchsubject_treatments"
    )


# TODO: issues loading specimen - look into associated_project field & data.
"""
class CDASpecimen(Base):
    __tablename__ = 'specimen'
    id: Mapped[str] = mapped_column(String, primary_key=True)
    associated_project: Mapped[Optional[str]] = mapped_column(String)
    days_to_collection: Mapped[Optional[int]] = mapped_column(Integer)
    primary_disease_type: Mapped[Optional[str]] = mapped_column(String)
    anatomical_site: Mapped[Optional[str]] = mapped_column(String)
    source_material_type: Mapped[Optional[str]] = mapped_column(String)
    specimen_type: Mapped[Optional[str]] = mapped_column(String)
    derived_from_specimen: Mapped[Optional[str]] = mapped_column(String)
    derived_from_subject: Mapped[Optional[str]] = mapped_column(String)
    integer_id_alias: Mapped[Optional[int]] = mapped_column(Integer)
    researchsubject_specimens: Mapped[List["CDAResearchSubjectSpecimen"]] = relationship(
        back_populates="specimen"
    )


class CDAResearchSubjectSpecimen(Base):
    __tablename__ = 'researchsubject_specimen'
    researchsubject_id: Mapped[str] = mapped_column(ForeignKey('researchsubject.id'), primary_key=True)
    specimen_id: Mapped[str] = mapped_column(ForeignKey('specimen.id'), primary_key=True)
    researchsubject: Mapped["CDAResearchSubject"] = relationship(
        back_populates="specimen_researchsubjects"
    )
    specimen: Mapped["CDASpecimen"] = relationship(
        back_populates="researchsubject_specimens"
    )
"""