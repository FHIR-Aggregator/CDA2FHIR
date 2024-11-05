from cda2fhir import utils
import json
import orjson
from pathlib import Path
import importlib.resources
from fhir.resources.identifier import Identifier
from fhir.resources.reference import Reference
from fhir.resources.documentreference import DocumentReference
from fhir.resources.group import Group
from cda2fhir.load_data import load_data
from cda2fhir.database import SessionLocal
from cda2fhir.cdamodels import CDASubject, CDAResearchSubject, CDASubjectResearchSubject, CDADiagnosis, CDATreatment, \
    CDASubjectAlias, CDASubjectProject, CDAResearchSubjectDiagnosis, CDASpecimen, ProjectdbGap, GDCProgramdbGap, \
    CDASubjectIdentifier, CDAProjectRelation, CDAFile, CDAFileSubject, CDAFileSpecimen
from cda2fhir.transformer import PatientTransformer, ResearchStudyTransformer, ResearchSubjectTransformer, \
    ConditionTransformer, SpecimenTransformer, DocumentReferenceTransformer
from sqlalchemy import select, func, or_
from sqlalchemy.orm import selectinload

gdc_dbgap_names = ['APOLLO', 'CDDP_EAGLE', 'CGCI', 'CTSP', 'EXCEPTIONAL_RESPONDERS', 'FM', 'HCMI', 'MMRF', 'NCICCR',
                   'OHSU', 'ORGANOID', 'REBC', 'TARGET', 'TCGA', 'TRIO', 'VAREPOP', 'WCDT']


def fhir_ndjson(entity, out_path):
    if isinstance(entity, list):
        with open(out_path, 'w', encoding='utf8') as file:
            file.write('\n'.join(map(lambda e: json.dumps(e, ensure_ascii=False), entity)))
    else:
        with open(out_path, 'w', encoding='utf8') as file:
            file.write(json.dumps(entity, ensure_ascii=False))


def cda2fhir(path, n_samples, n_diagnosis, transform_files, n_files, save=True, verbose=False):
    """CDA2FHIR attempts to transform the baseclass definitions of CDA data defined in cdamodels to query relevant
    information to create FHIR entities: Specimen, ResearchSubject,
    ResearchStudy, Condition, BodyStructure, Observation utilizing transfomer classes."""
    load_data(transform_files)

    session = SessionLocal()
    patient_transformer = PatientTransformer(session)
    research_study_transformer = ResearchStudyTransformer(session)
    research_subject_transformer = ResearchSubjectTransformer(session)
    condition_transformer = ConditionTransformer(session)
    specimen_transformer = SpecimenTransformer(session)
    file_transformer = DocumentReferenceTransformer(session, patient_transformer, specimen_transformer)

    if path:
        meta_path = Path(path)
    else:
        meta_path = Path(importlib.resources.files('cda2fhir').parent / 'data' / 'META')
        Path(meta_path).mkdir(parents=True, exist_ok=True)

    observations = []

    try:
        # Specimen and Observation and BodyStructure -----------------------------------
        if n_samples:
            n_samples = int(n_samples)

            for _ in range(n_samples):
                specimens = session.execute(
                    select(CDASpecimen)
                    .order_by(func.random())
                    .limit(n_samples)
                ).scalars().all()
        else:
            specimens = session.query(CDASpecimen).all()

        assert specimens, "Specimens is not defined."

        if verbose:
            print("===== SPECIMEN: ")
            for specimen in specimens:
                print(f"id: {specimen.id}, source_material_type: {specimen.source_material_type}")

        fhir_specimens = []
        specimen_bds = []
        for specimen in specimens:
            _cda_subject = session.execute(
                select(CDASubject)
                .filter_by(id=specimen.derived_from_subject)
            ).scalar_one_or_none()

            _cda_parent_specimen = session.execute(
                select(CDASpecimen)
                .filter_by(id=specimen.derived_from_specimen)
            ).scalar_one_or_none()

            if _cda_subject:
                # if subject and specimen derived from id exists in relative tables, then create the specimen
                _specimen_patient = patient_transformer.transform_human_subjects([_cda_subject])
                fhir_specimen = specimen_transformer.fhir_specimen(specimen, _specimen_patient[0])

                if fhir_specimen:
                    fhir_specimens.append(fhir_specimen)

                    specimen_bd = specimen_transformer.specimen_body_structure(specimen, _specimen_patient[0])
                    if specimen_bd:
                        specimen_bds.append(specimen_bd)

                    _specimen_obs = specimen_transformer.specimen_observation(specimen, _specimen_patient[0],
                                                                              fhir_specimen.id)
                    if _specimen_obs:
                        observations.append(_specimen_obs)

        if save and fhir_specimens:
            fhir_specimens = {fs.id: fs for fs in fhir_specimens if
                              fs}.values()  # remove duplicates should be a better way
            _fhir_specimens = [orjson.loads(s.json()) for s in fhir_specimens]
            fhir_ndjson(_fhir_specimens, str(meta_path / "Specimen.ndjson"))

            fhir_specimen_dbs = {sbd.id: sbd for sbd in specimen_bds if
                                 sbd}.values()
            fhir_specimen_dbs = [orjson.loads(s.json()) for s in fhir_specimen_dbs]
            fhir_ndjson(fhir_specimen_dbs, str(meta_path / "BodyStructure.ndjson"))

        # Patient and Observation -----------------------------------
        subjects = session.query(CDASubject).all()
        if verbose:
            for subject in subjects:
                print(f"id: {subject.id}, species: {subject.species}, sex: {subject.sex}")

            # projects = session.query(CDASubjectProject).filter_by(subject_id=subject.id).all()
            # for project in projects:
            #    print(f"@@@@@ Subject's associated  projects: {project.associated_project}")

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

        patients = patient_transformer.transform_human_subjects(subjects)
        if save and patients:
            patients = [orjson.loads(patient.json()) for patient in patients]
            fhir_ndjson(patients, str(meta_path / "Patient.ndjson"))

        for subject in subjects:
            if subject.cause_of_death:
                patient_identifiers = patient_transformer.patient_identifier(subject)
                patient_id = patient_transformer.patient_mintid(patient_identifiers[0])
                obs = patient_transformer.observation_cause_of_death(subject.cause_of_death)
                obs_identifier = Identifier(
                    **{'system': "https://cda.readthedocs.io/cause_of_death", 'value': "".join([patient_id, subject.cause_of_death])})
                obs.id = patient_transformer.mint_id(identifier=obs_identifier, resource_type="Observation")
                obs.subject = {"reference": f"Patient/{patient_id}"}
                obs.focus = [{"reference": f"Patient/{patient_id}"}]
                observations.append(obs)

            if subject.days_to_death:
                obs_days_to_death = patient_transformer.observation_days_to_death(subject.days_to_death)
                obs_days_to_death_identifier = Identifier(
                    **{'system': "https://cda.readthedocs.io/days_to_death", 'value': "".join([patient_id, subject.days_to_death])})
                obs_days_to_death.id = patient_transformer.mint_id(identifier=obs_days_to_death_identifier, resource_type="Observation")
                obs_days_to_death.subject = {"reference": f"Patient/{patient_id}"}
                obs_days_to_death.focus = [{"reference": f"Patient/{patient_id}"}]
                observations.append(obs_days_to_death)

            if subject.days_to_birth:
                obs_days_to_birth = patient_transformer.observation_days_to_birth(subject.days_to_birth)
                obs_days_to_birth_identifier = Identifier(
                    **{'system': "https://cda.readthedocs.io/days_to_birth", 'value': "".join([patient_id, subject.days_to_birth])})
                obs_days_to_birth.id = patient_transformer.mint_id(identifier=obs_days_to_birth_identifier, resource_type="Observation")
                obs_days_to_birth.subject = {"reference": f"Patient/{patient_id}"}
                obs_days_to_birth.focus = [{"reference": f"Patient/{patient_id}"}]
                observations.append(obs_days_to_birth)

        # ResearchStudy and ResearchSubject -----------------------------------
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
                    .all()  # there is only one
                )
                _patient = patient_transformer.transform_human_subjects(_subject)

                for cda_rs_subject in query_research_subjects:
                    research_study = research_study_transformer.research_study(project, cda_rs_subject)

                    # gdc_dbgap = session.execute(session.query(GDCProgramdbGap).filter(GDCProgramdbGap.GDC_program_name.in_(gdc_dbgap_names))).all()
                    gdc_dbgap = session.execute(session.query(GDCProgramdbGap).where(
                        GDCProgramdbGap.GDC_program_name.contains(research_study.name))).one_or_none()

                    if gdc_dbgap:
                        # parent dbGap ID for GDC projects ex. TCGA dgGap id for all projetcs including TCGA substring (ex. TCGA-BRCA)
                        research_study.identifier.append(
                            Identifier(**{"system": "https://www.ncbi.nlm.nih.gov/dbgap_accession_number", "value": gdc_dbgap[0], "use": "secondary"}))

                    # query and fetch projet's dbgap id
                    dbGap_study_accession = session.execute(
                        session.query(ProjectdbGap)
                        .filter_by(GDC_project_id=research_study.name)
                    ).first()

                    if dbGap_study_accession:
                        dbGap_identifier = Identifier(**{'system': "https://www.ncbi.nlm.nih.gov/dbgap_accession_number",
                                                         'value': dbGap_study_accession[0].dbgap_study_accession,
                                                         "use": "secondary"})
                        research_study.identifier.append(dbGap_identifier)

                    if research_study:
                        if _patient and research_study:
                            _research_subject = research_subject_transformer.research_subject(cda_rs_subject,
                                                                                              _patient[0],
                                                                                              research_study)
                            research_subjects.append(_research_subject)

                            # check and fetch program for project relation
                            query_subject_alias = (
                                session.query(CDASubjectAlias)
                                .join(CDASubject)
                                .filter(CDASubject.id == _subject[0].id)
                                .all()  # there is only one
                            )

                            if "BEATAML" in _subject[0].id:
                                subject_id_value = _subject[0].id.replace("BEATAML1.0.", "")
                            else:
                                subject_id_value = _subject[0].id.split(".")[1]

                            _cda_subject_identifiers = (session.execute(
                                select(CDASubjectIdentifier)
                                .filter_by(
                                    subject_alias=query_subject_alias[0].subject_alias,
                                    value=subject_id_value))
                                                        .all())
                            part_of_study = []
                            for _cda_subject_identifier in _cda_subject_identifiers:
                                _program_research_study = research_study_transformer.program_research_study(
                                    name=_cda_subject_identifier[0].system)
                                if _program_research_study:
                                    research_studies.append(_program_research_study)
                                    # research_study.partOf =
                                    part_of_study.append(
                                        Reference(**{"reference": f"ResearchStudy/{_program_research_study.id}"}))

                            # ResearchStudy relations
                            # CRDC <- GDC, IDC, PDC, ICDC, CDS and HTAN & CMPC
                            project_name = project.associated_project
                            associated_project_programs = session.query(CDAProjectRelation).filter(
                                or_(
                                    CDAProjectRelation.project_gdc == project_name,
                                    CDAProjectRelation.project_pdc == project_name,
                                    CDAProjectRelation.project_idc == project_name,
                                    CDAProjectRelation.project_cds == project_name,
                                    CDAProjectRelation.project_icdc == project_name
                                )
                            ).all()

                            for _p in associated_project_programs:
                                print("===========: ", _p, "\n")
                                _p_name = None
                                if _p.project_gdc == project_name:
                                    _p_name = 'GDC'
                                elif _p.project_pdc == project_name:
                                    _p_name = 'PDC'
                                elif _p.project_idc == project_name:
                                    _p_name = 'IDC'
                                elif _p.project_cds == project_name:
                                    _p_name = 'CDS'
                                elif _p.project_icdc == project_name:
                                    _p_name = 'ICDC'

                                print("Program:", _p.program, "Sub-Program:", _p.sub_program, "GDC:", _p.project_gdc,
                                      "PDC:", _p.project_pdc,
                                      "IDC:", _p.project_idc, "CDS:", _p.project_cds, "ICDC:", _p.project_icdc,
                                      "program_project_match:", _p_name)
                                #
                                if _p.program:
                                    parent_program = research_study_transformer.program_research_study(
                                        name=_p.program)
                                    part_of_study = [p for p in part_of_study if
                                                     p.reference not in f"ResearchStudy/{parent_program.id}"]
                                    part_of_study.append(
                                        Reference(**{"reference": f"ResearchStudy/{parent_program.id}"}))
                                    research_studies.append(parent_program)

                                if _p_name:
                                    main_program = research_study_transformer.program_research_study(
                                        name=_p_name)
                                    part_of_study = [p for p in part_of_study if
                                                     p.reference not in f"ResearchStudy/{main_program.id}"]
                                    part_of_study.append(Reference(**{"reference": f"ResearchStudy/{main_program.id}"}))
                                    research_studies.append(main_program)

                                if _p.sub_program:
                                    parent_sub_program = research_study_transformer.program_research_study(
                                        name=_p.sub_program)
                                    part_of_study = [p for p in part_of_study if
                                                     p.reference not in f"ResearchStudy/{parent_sub_program.id}"]
                                    part_of_study.append(
                                        Reference(**{"reference": f"ResearchStudy/{parent_sub_program.id}"}))
                                    research_studies.append(parent_sub_program)

                            if part_of_study:
                                research_study.partOf = part_of_study

                        research_studies.append(research_study)

        if save and research_studies:
            research_studies = {rstudy.id: rstudy for rstudy in research_studies if
                                rstudy}.values()  # remove duplicates should be a better way
            rs = [orjson.loads(research_study.json()) for research_study in research_studies if research_study]
            fhir_ndjson(rs, str(meta_path / "ResearchStudy.ndjson"))

        if save and research_subjects:
            research_subjects = {rsubject.id: rsubject for rsubject in research_subjects if
                                 rsubject}.values()
            fhir_rsubjects = [orjson.loads(cdarsubject.json()) for cdarsubject in research_subjects if cdarsubject]
            fhir_ndjson(fhir_rsubjects, str(meta_path / "ResearchSubject.ndjson"))

        # Condition and Observation -----------------------------------
        # randomly choose N diagnosis to reduce runtime for development
        # takes ~ 2hrs for 1041360+ diagnosis records
        if n_diagnosis:
            n_diagnosis = int(n_diagnosis)

            for _ in range(n_diagnosis):
                diagnoses = session.execute(
                    select(CDADiagnosis)
                    .order_by(func.random())  # randomly select
                    .limit(n_diagnosis)
                ).scalars().all()
        else:
            diagnoses = session.query(CDADiagnosis).all()
            if verbose:
                print("**** diagnosis:")
                for diagnosis in diagnoses:
                    print(f"id: {diagnosis.id}, primary_diagnosis: {diagnosis.primary_diagnosis}")

        assert diagnoses, "Diagnosis is not defined."

        conditions = []
        for diagnosis in diagnoses:
            _subject_diagnosis = (
                session.query(CDASubject)
                .join(CDASubjectResearchSubject, CDASubject.id == CDASubjectResearchSubject.subject_id)
                .join(CDAResearchSubject, CDASubjectResearchSubject.researchsubject_id == CDAResearchSubject.id)
                .join(CDAResearchSubjectDiagnosis,
                      CDAResearchSubject.id == CDAResearchSubjectDiagnosis.researchsubject_id)
                .filter(CDAResearchSubjectDiagnosis.diagnosis_id == diagnosis.id)
                .all()
            )

            if _subject_diagnosis:
                _patient_diagnosis = patient_transformer.transform_human_subjects(_subject_diagnosis)
                if _patient_diagnosis and _patient_diagnosis[0].id:
                    if verbose:
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
                                    observation = condition_transformer.condition_observation(diagnosis, _display,
                                                                                              _patient_diagnosis[0],
                                                                                              condition.id)
                                    if observation:
                                        observations.append(observation)

        if save and conditions:
            _conditions = {_condition.id: _condition for _condition in conditions if _condition}.values()
            fhir_conditions = [orjson.loads(c.json()) for c in _conditions if c]
            fhir_ndjson(fhir_conditions, str(meta_path / "Condition.ndjson"))

        if save and observations:
            observations = {_obs.id: _obs for _obs in observations if _obs}.values()
            patient_observations = [orjson.loads(observation.json()) for observation in observations]
            fhir_ndjson(patient_observations, str(meta_path / "Observation.ndjson"))

        # MedicationAdministration and Medication  -----------------------------------
        treatments = session.query(CDATreatment).all()
        if verbose:
            print("**** treatment:")
            for treatment in treatments:
                print(f"id: {treatment.id}, therapeutic_agent: {treatment.therapeutic_agent}")

        # File  -----------------------------------
        # requires pre-processing and validation
        # large record set -> 30+ GB takes time
        if transform_files:
            if n_files:
                n_files = int(n_files)
                files = session.execute(
                    select(CDAFile)
                    .order_by(func.random())
                    .limit(n_files)
                    .options(
                        selectinload(CDAFile.file_subject_relation).selectinload(CDAFileSubject.subject),
                        selectinload(CDAFile.specimen_file_relation).selectinload(CDAFileSpecimen.specimen)
                    )
                ).scalars().all()
            else:
                files = session.query(CDAFile).options(
                    selectinload(CDAFile.file_subject_relation).selectinload(CDAFileSubject.subject),
                    selectinload(CDAFile.specimen_file_relation).selectinload(CDAFileSpecimen.specimen)
                ).all()

            assert files, "Files are not defined."

            all_files = []
            all_groups = []
            for file in files:
                print(f"File ID: {file.id}, File DRS URI: {file.drs_uri}")

                _file_subjects = [
                    session.query(CDASubject).filter(CDASubject.id == file_subject.subject_id).first()
                    for file_subject in file.file_subject_relation
                ]
                _file_subjects = [subject for subject in _file_subjects if subject]  # remove none
                print(f"++++++++++++++ FILE's SUBJECTS: {[_subject.id for _subject in _file_subjects]}")

                _file_specimens = [
                    session.query(CDASpecimen).filter(CDASpecimen.id == file_specimen.specimen_id).first()
                    for file_specimen in file.specimen_file_relation
                ]
                _file_specimens = [specimen for specimen in _file_specimens if specimen]  # remove none
                print(f"+++++++++++++ FILE's SPECIMENS: {[_specimen.id for _specimen in _file_specimens]}")

                # DocumentReference passing associated CDASubject and CDASpecimen
                fhir_file = file_transformer.fhir_document_reference(file, _file_subjects, _file_specimens)
                if fhir_file["DocumentReference"] and isinstance(fhir_file["DocumentReference"], DocumentReference):
                    all_files.append(fhir_file["DocumentReference"])

                this_files_group = fhir_file.get("Group")
                if this_files_group and isinstance(this_files_group, Group):
                    all_groups.append(this_files_group)

            if save and all_files:
                document_references = {_doc_ref.id: _doc_ref for _doc_ref in all_files if _doc_ref}.values()
                fhir_document_references = [orjson.loads(document_reference.json()) for document_reference in document_references]

                utils.create_or_extend(new_items=fhir_document_references, folder_path='data/META', resource_type='DocumentReference', update_existing=False)
                # fhir_ndjson(fhir_document_references, str(meta_path / "DocumentReference.ndjson"))

            if save and all_groups:
                groups = {group.id: group for group in all_groups if group.id}.values()
                fhir_groups = [orjson.loads(group.json()) for group in groups]

                utils.create_or_extend(new_items=fhir_groups, folder_path='data/META', resource_type='Group', update_existing=False)
                # fhir_ndjson(fhir_groups, str(meta_path / "Group.ndjson"))

    finally:
        print("****** Closing Session ******")
        session.close()
