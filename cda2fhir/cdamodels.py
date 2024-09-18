from typing import Optional, List
from sqlalchemy import ForeignKey, Integer, String, create_engine, PrimaryKeyConstraint
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
    subject_project_relation: Mapped[List["CDASubjectProject"]] = relationship(
        back_populates="subject"
    )

    @property
    def alias_id(self):
        """Fetch CDA subject's alias id from subject_alias_relations table - (one to one)"""
        if self.subject_alias_relation: # TODO:need relations for new data
            return self.subject_alias_relation.__getitem__(0).subject_alias
        else:
            return None


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
    specimen_researchsubjects: Mapped[List["CDAResearchSubjectSpecimen"]] = relationship(
        back_populates="researchsubject"
    )
    treatment_researchsubjects: Mapped[List["CDAResearchSubjectTreatment"]] = relationship(
        back_populates="researchsubject"
    )


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


class CDASubjectProject(Base):
    __tablename__ = 'subject_project'
    query: QueryPropertyDescriptor = Session.query_property()
    subject_id: Mapped[str] = mapped_column(ForeignKey("subject.id"), primary_key=True)
    associated_project: Mapped[str] = mapped_column(String, primary_key=True)
    subject: Mapped["CDASubject"] = relationship(
        back_populates="subject_project_relation"
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
    query: QueryPropertyDescriptor = Session.query_property()
    researchsubject_id: Mapped[str] = mapped_column(ForeignKey('researchsubject.id'), primary_key=True)
    specimen_id: Mapped[str] = mapped_column(ForeignKey('specimen.id'), primary_key=True)
    specimen: Mapped["CDASpecimen"] = relationship(
        back_populates="researchsubject_specimens"
    )
    researchsubject: Mapped["CDAResearchSubject"] = relationship(
        back_populates="specimen_researchsubjects"
    )


class ProjectdbGap(Base):
    __tablename__ = 'project_dbGap'
    query: QueryPropertyDescriptor = Session.query_property()
    GDC_project_id: Mapped[str] = mapped_column(String, primary_key=True)
    dbgap_study_accession: Mapped[str] = mapped_column(String, primary_key=True)


class GDCProgramdbGap(Base):
    __tablename__ = 'gdc_program_dbGap'
    query: QueryPropertyDescriptor = Session.query_property()
    GDC_program_name: Mapped[str] = mapped_column(String, primary_key=True)
    dbgap_study_accession: Mapped[str] = mapped_column(String, primary_key=True)


# class CDAdbGap(Base):
#    __tablename__ = 'cda_dbGap'
#    query: QueryPropertyDescriptor = Session.query_property()
#    # TODO: make one table via all xlsx sheets

class CDASubjectIdentifier(Base):
    __tablename__ = 'cda_subject_identifier' # CDA provenance info relation table.
    query: QueryPropertyDescriptor = Session.query_property()
    subject_alias:  Mapped[Optional[int]] = mapped_column(Integer, primary_key=True)
    value: Mapped[Optional[str]] = mapped_column(String, primary_key=True)
    system: Mapped[Optional[str]] = mapped_column(String, primary_key=True)
    field_name: Mapped[Optional[str]] = mapped_column(String, primary_key=True)


class CDAProjectRelation(Base):
    __tablename__ = 'project_program_relation'
    query: QueryPropertyDescriptor = Session.query_property()
    # id = mapped_column(Integer, primary_key=True)
    program: Mapped[Optional[str]] = mapped_column(String)
    sub_program: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    project_gdc: Mapped[str] = mapped_column(String, nullable=True)
    project_pdc: Mapped[str] = mapped_column(String, nullable=True)
    project_idc: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    project_cds: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    project_icdc: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    __table_args__ = (
        PrimaryKeyConstraint('project_gdc', 'project_pdc', 'project_idc',
                             'project_cds', 'project_icdc', 'sub_program', 'program'),
    )
