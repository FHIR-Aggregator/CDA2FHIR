from typing import Optional, List
from sqlalchemy import ForeignKey, Integer, String, Boolean, create_engine, PrimaryKeyConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship, DeclarativeBase, QueryPropertyDescriptor
from sqlalchemy.orm import sessionmaker, scoped_session
from pathlib import Path
import importlib.resources
from sqlalchemy.engine import Engine
from sqlalchemy import event


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=OFF")
    cursor.execute("PRAGMA journal_mode=MEMORY")
    cursor.close()


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
    integer_id_alias: Mapped[Optional[int]] = mapped_column(Integer, unique=True)
    researchsubject_subjects: Mapped[List["CDASubjectResearchSubject"]] = relationship(
        back_populates="subject"
    )
    subject_project_relation: Mapped[List["CDASubjectProject"]] = relationship(
        back_populates="subject"
    )
    subject_identifier: Mapped[List["CDASubjectIdentifier"]] = relationship(
        "CDASubjectIdentifier",
        back_populates="subject"
    )
    subject_file_relation: Mapped[List["CDAFileSubject"]] = relationship(
        back_populates="subject"
    )
    subject_mutation_relation: Mapped[List["CDASubjectMutation"]] = relationship(
        "CDASubjectMutation",
        back_populates="subject"
    )

    @property
    def alias_id(self):
        return self.integer_id_alias



class CDASubjectIdentifier(Base):
    __tablename__ = 'subject_identifier'
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    subject_alias: Mapped[int] = mapped_column(Integer, ForeignKey('subject.integer_id_alias'), nullable=False)
    system: Mapped[str] = mapped_column(String, nullable=False)
    field_name: Mapped[str] = mapped_column(String, nullable=False)
    value: Mapped[str] = mapped_column(String, nullable=False)
    subject: Mapped["CDASubject"] = relationship("CDASubject", back_populates="subject_identifier")



class CDAResearchSubject(Base):
    __tablename__ = 'researchsubject'
    query: QueryPropertyDescriptor = Session.query_property()
    id: Mapped[str] = mapped_column(String, primary_key=True)
    member_of_research_project: Mapped[Optional[str]] = mapped_column(String)
    primary_diagnosis_condition: Mapped[Optional[str]] = mapped_column(String)
    primary_diagnosis_site: Mapped[Optional[str]] = mapped_column(String)
    integer_id_alias: Mapped[Optional[int]] = mapped_column(Integer, unique=True)
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
    subject_alias: Mapped[int] = mapped_column(ForeignKey("subject.integer_id_alias"), primary_key=True)
    researchsubject_alias: Mapped[int] = mapped_column(ForeignKey("researchsubject.integer_id_alias"), primary_key=True)
    subject: Mapped["CDASubject"] = relationship(
        back_populates="researchsubject_subjects"
    )
    researchsubject: Mapped["CDAResearchSubject"] = relationship(
        back_populates="subject_researchsubjects"
    )


class CDASubjectProject(Base):
    __tablename__ = 'subject_project'
    query: QueryPropertyDescriptor = Session.query_property()
    subject_alias: Mapped[int] = mapped_column(ForeignKey("subject.integer_id_alias"), primary_key=True)
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
    pathologic_stage: Mapped[Optional[str]] = mapped_column(String)
    pathologic_stage_m: Mapped[Optional[str]] = mapped_column(String)
    pathologic_stage_n: Mapped[Optional[str]] = mapped_column(String)
    pathologic_stage_t: Mapped[Optional[str]] = mapped_column(String)
    clinical_stage: Mapped[Optional[str]] = mapped_column(String)
    clinical_stage_m: Mapped[Optional[str]] = mapped_column(String)
    clinical_stage_n: Mapped[Optional[str]] = mapped_column(String)
    clinical_stage_t: Mapped[Optional[str]] = mapped_column(String)
    grade: Mapped[Optional[str]] = mapped_column(String)
    method_of_diagnosis: Mapped[Optional[str]] = mapped_column(String)
    integer_id_alias: Mapped[Optional[int]] = mapped_column(Integer, unique=True)
    researchsubject_diagnoses: Mapped[List["CDAResearchSubjectDiagnosis"]] = relationship(
        back_populates="diagnosis"
    )


class CDAResearchSubjectDiagnosis(Base):
    __tablename__ = 'researchsubject_diagnosis'
    query: QueryPropertyDescriptor = Session.query_property()
    researchsubject_alias: Mapped[str] = mapped_column(ForeignKey('researchsubject.integer_id_alias'), primary_key=True)
    diagnosis_alias: Mapped[str] = mapped_column(ForeignKey('diagnosis.integer_id_alias'), primary_key=True)
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
    number_of_cycles: Mapped[Optional[int]] = mapped_column(Integer)
    integer_id_alias: Mapped[Optional[int]] = mapped_column(Integer, unique=True)
    researchsubject_treatments: Mapped[List["CDAResearchSubjectTreatment"]] = relationship(
        back_populates="treatment"
    )


class CDAResearchSubjectTreatment(Base):
    __tablename__ = 'researchsubject_treatment'
    query: QueryPropertyDescriptor = Session.query_property()
    researchsubject_alias: Mapped[str] = mapped_column(ForeignKey('researchsubject.integer_id_alias'), primary_key=True)
    treatment_alias: Mapped[str] = mapped_column(ForeignKey('treatment.integer_id_alias'), primary_key=True)
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
    file_specimen_relation: Mapped[List["CDAFileSpecimen"]] = relationship(
        back_populates="specimen"
    )


class CDAResearchSubjectSpecimen(Base):
    __tablename__ = 'researchsubject_specimen'
    query: QueryPropertyDescriptor = Session.query_property()
    researchsubject_alias: Mapped[str] = mapped_column(ForeignKey('researchsubject.integer_id_alias'), primary_key=True)
    specimen_alias: Mapped[str] = mapped_column(ForeignKey('specimen.integer_id_alias'), primary_key=True)
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


class CDAFile(Base):
    __tablename__ = 'cda_file'
    query: QueryPropertyDescriptor = Session.query_property()
    id: Mapped[Optional[str]] = mapped_column(String, primary_key=True)
    label: Mapped[Optional[str]] = mapped_column(String)
    data_category: Mapped[Optional[str]] = mapped_column(String)
    data_type: Mapped[Optional[str]] = mapped_column(String)
    file_format: Mapped[Optional[str]] = mapped_column(String)
    drs_uri: Mapped[Optional[str]] = mapped_column(String)
    byte_size: Mapped[Optional[int]] = mapped_column(Integer)
    checksum: Mapped[Optional[str]] = mapped_column(String)
    data_modality: Mapped[Optional[str]] = mapped_column(String)
    imaging_modality: Mapped[Optional[str]] = mapped_column(String)
    dbgap_accession_number: Mapped[Optional[str]] = mapped_column(String)
    imaging_series: Mapped[Optional[str]] = mapped_column(String)
    integer_id_alias: Mapped[Optional[int]] = mapped_column(Integer)
    specimen_file_relation: Mapped[List["CDAFileSpecimen"]] = relationship(
        back_populates="file"
    )
    file_subject_relation: Mapped[List["CDAFileSubject"]] = relationship(
        back_populates="file"
    )



class CDAFileSubject(Base):
    __tablename__ = 'file_subject'
    query: QueryPropertyDescriptor = Session.query_property()
    file_alias: Mapped[str] = mapped_column(ForeignKey("cda_file.integer_id_alias"), primary_key=True)
    subject_alias: Mapped[str] = mapped_column(ForeignKey("subject.integer_id_alias"), primary_key=True)
    subject: Mapped["CDASubject"] = relationship(
        back_populates="subject_file_relation"
    )
    file: Mapped["CDAFile"] = relationship(
        back_populates="file_subject_relation"
    )



class CDAFileSpecimen(Base):
    __tablename__ = 'file_specimen'
    query: QueryPropertyDescriptor = Session.query_property()
    file_alias: Mapped[str] = mapped_column(ForeignKey("cda_file.integer_id_alias"), primary_key=True)
    specimen_alias: Mapped[str] = mapped_column(ForeignKey("specimen.integer_id_alias"), primary_key=True)
    specimen: Mapped["CDASpecimen"] = relationship(
        back_populates="file_specimen_relation"
    )
    file: Mapped["CDAFile"] = relationship(
        back_populates="specimen_file_relation"
    )


class CDAMutation(Base):
    __tablename__ = 'mutation'
    query: QueryPropertyDescriptor = Session.query_property()

    id: Mapped[str] = mapped_column(String, primary_key=True)
    integer_id_alias: Mapped[Optional[int]] = mapped_column(Integer)
    project_short_name: Mapped[Optional[str]] = mapped_column(String)
    hugo_symbol: Mapped[Optional[str]] = mapped_column(String)
    entrez_gene_id: Mapped[Optional[str]] = mapped_column(String)
    hotspot: Mapped[Optional[bool]] = mapped_column(Boolean)
    ncbi_build: Mapped[Optional[str]] = mapped_column(String)
    chromosome: Mapped[Optional[str]] = mapped_column(String)
    variant_type: Mapped[Optional[str]] = mapped_column(String)
    variant_class: Mapped[Optional[str]] = mapped_column(String)
    reference_allele: Mapped[Optional[str]] = mapped_column(String)
    match_norm_seq_allele1: Mapped[Optional[str]] = mapped_column(String)
    match_norm_seq_allele2: Mapped[Optional[str]] = mapped_column(String)
    tumor_seq_allele1: Mapped[Optional[str]] = mapped_column(String)
    tumor_seq_allele2: Mapped[Optional[str]] = mapped_column(String)
    dbsnp_rs: Mapped[Optional[str]] = mapped_column(String)
    mutation_status: Mapped[Optional[str]] = mapped_column(String)
    transcript_id: Mapped[Optional[str]] = mapped_column(String)
    gene: Mapped[Optional[str]] = mapped_column(String)
    one_consequence: Mapped[Optional[str]] = mapped_column(String)
    hgnc_id: Mapped[Optional[str]] = mapped_column(String)
    primary_site: Mapped[Optional[str]] = mapped_column(String)
    case_barcode: Mapped[Optional[str]] = mapped_column(String)
    case_id: Mapped[Optional[str]] = mapped_column(String)
    sample_barcode_tumor: Mapped[Optional[str]] = mapped_column(String)
    tumor_submitter_uuid: Mapped[Optional[str]] = mapped_column(String)
    sample_barcode_normal: Mapped[Optional[str]] = mapped_column(String)
    normal_submitter_uuid: Mapped[Optional[str]] = mapped_column(String)
    aliquot_barcode_tumor: Mapped[Optional[str]] = mapped_column(String)
    tumor_aliquot_uuid: Mapped[Optional[str]] = mapped_column(String)
    aliquot_barcode_normal: Mapped[Optional[str]] = mapped_column(String)
    matched_norm_aliquot_uuid: Mapped[Optional[str]] = mapped_column(String)

    subject_mutations: Mapped[List["CDASubjectMutation"]] = relationship(
        back_populates="mutation"
    )

class CDASubjectMutation(Base):
    __tablename__ = 'subject_mutation'
    query: QueryPropertyDescriptor = Session.query_property()
    subject_alias: Mapped[int] = mapped_column(ForeignKey("subject.integer_id_alias"), primary_key=True)
    mutation_alias: Mapped[int] = mapped_column(ForeignKey("mutation.integer_id_alias"), primary_key=True)

    subject: Mapped["CDASubject"] = relationship(
        "CDASubject",
        back_populates="subject_mutation_relation"
    )
    mutation: Mapped["CDAMutation"] = relationship(
        "CDAMutation",
        back_populates="subject_mutations"
    )