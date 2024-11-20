from cda2fhir import utils
import orjson
from pathlib import Path
import importlib.resources
from functools import lru_cache
from fhir.resources.identifier import Identifier
from fhir.resources.reference import Reference
from fhir.resources.documentreference import DocumentReference
from fhir.resources.group import Group
from cda2fhir.load_data import load_data
from cda2fhir.database import SessionLocal
from cda2fhir.cdamodels import CDASubject, CDAResearchSubject, CDASubjectResearchSubject, CDADiagnosis, CDATreatment, \
    CDASubjectAlias, CDASubjectProject, CDAResearchSubjectDiagnosis, CDASpecimen, ProjectdbGap, GDCProgramdbGap, \
    CDASubjectIdentifier, CDAProjectRelation, CDAFile, CDAFileSubject, CDAFileSpecimen, CDAResearchSubjectTreatment, \
    CDAMutation, CDASubjectMutation
from cda2fhir.transformer import PatientTransformer, ResearchStudyTransformer, ResearchSubjectTransformer, \
    ConditionTransformer, SpecimenTransformer, DocumentReferenceTransformer, MedicationAdministrationTransformer, \
    MutationTransformer
from sqlalchemy import select, func, or_
from sqlalchemy.orm import selectinload

gdc_dbgap_names = ['APOLLO', 'CDDP_EAGLE', 'CGCI', 'CTSP', 'EXCEPTIONAL_RESPONDERS', 'FM', 'HCMI', 'MMRF', 'NCICCR',
                   'OHSU', 'ORGANOID', 'REBC', 'TARGET', 'TCGA', 'TRIO', 'VAREPOP', 'WCDT']


def cda2fhir(path, n_samples, n_diagnosis, transform_condition, transform_files, transform_treatment, transform_mutation, n_files, save=True, verbose=False):
    """CDA2FHIR attempts to transform the baseclass definitions of CDA data defined in cdamodels to query relevant
    information to create FHIR entities: Specimen, ResearchSubject, ResearchStudy, Condition, BodyStructure,
    Observation, MedicationAdministration, Medication, Substance, SubstanceDefinition utilizing transformer classes."""
    load_data(transform_condition, transform_files, transform_treatment, transform_mutation)

    session = SessionLocal()
    patient_transformer = PatientTransformer(session)
    research_study_transformer = ResearchStudyTransformer(session)
    research_subject_transformer = ResearchSubjectTransformer(session)
    condition_transformer = ConditionTransformer(session)
    specimen_transformer = SpecimenTransformer(session)
    file_transformer = DocumentReferenceTransformer(session, patient_transformer, specimen_transformer)
    treatment_transformer = MedicationAdministrationTransformer(session, patient_transformer)
    mutation_transformer = MutationTransformer(session, patient_transformer)

    if path:
        meta_path = Path(path)
    else:
        meta_path = Path(importlib.resources.files('cda2fhir').parent / 'data' / 'META')
        Path(meta_path).mkdir(parents=True, exist_ok=True)

    try:
        # MedicationAdministration and Medication  -----------------------------------
        if transform_treatment:
            treatments = session.query(CDATreatment).all()
            therapeutic_agent_compounds = []
            for treatment in treatments:
                # list of all possible therapeutic_agent capitalized and queried in CHembl - Query once
                if treatment.therapeutic_agent:
                    therapeutic_agent_compounds.append(treatment.therapeutic_agent.upper())
            therapeutic_agent_compounds = list(set(therapeutic_agent_compounds))
            chembl_data_exists, chembl_data = treatment_transformer.fetch_chembl_data(therapeutic_agent_compounds, limit=10000)
            compound_results = {compound.upper(): [] for compound in therapeutic_agent_compounds}

            if chembl_data_exists:
                for record in chembl_data:
                    compound_name = record[3].upper()
                    if compound_name in compound_results:
                        compound_results[compound_name].append(record)

            key_info = ["CHEMBL_ID", "STANDARD_INCHI", "CANONICAL_SMILES", "COMPOUND_NAME"]
            substance_definations = []
            substances = []
            medications = []
            medication_administrations = []
            for compound_name, compound_data in compound_results.items():
                if compound_data:
                    compound_data = [dict(zip(key_info, row)) for row in compound_data]
                    sdr = treatment_transformer.create_substance_definition_representations(compound_data)
                    if sdr:
                        sd = treatment_transformer.create_substance_definition(compound_name=compound_name, representations=sdr)
                        if sd:
                            substance_definations.append(sd)
                            substance = treatment_transformer.create_substance(compound_name=compound_name, substance_definition=sd)
                            if substance:
                                substances.append(substance)

                                medication = treatment_transformer.create_medication(compound_name=compound_name,
                                                                                     treatment_type=None,
                                                                                     _substance=substance)
                                if medication:
                                    medications.append(medication)

            if save and substance_definations:
                utils.deduplicate_and_save(substance_definations, "SubstanceDefinition.ndjson", meta_path, save)

            if save and substances:
                utils.deduplicate_and_save(substances, "Substance.ndjson", meta_path, save)

            if save and medications:
                utils.deduplicate_and_save(medications, "Medication.ndjson", meta_path, save)

            for treatment in treatments:
                _subject_treatment = (
                    session.query(CDASubject)
                    .join(CDASubjectResearchSubject, CDASubject.id == CDASubjectResearchSubject.subject_id)
                    .join(CDAResearchSubject, CDASubjectResearchSubject.researchsubject_id == CDAResearchSubject.id)
                    .join(CDAResearchSubjectTreatment,
                          CDAResearchSubject.id == CDAResearchSubjectTreatment.researchsubject_id)
                    .filter(CDAResearchSubjectTreatment.treatment_id == treatment.id)
                    .all()
                )

                if _subject_treatment:
                    for subject in _subject_treatment:
                        compound_name = treatment.therapeutic_agent.upper() if treatment.therapeutic_agent else None
                        medication = next((med for med in medications if med.code.coding[0].code == compound_name), None)
                        med_admin = treatment_transformer.create_medication_administration(
                            treatment=treatment,
                            subject=subject,
                            medication=medication
                        )

                        if med_admin:
                            medication_administrations.append(med_admin)

            if save and medication_administrations:
                utils.deduplicate_and_save(medication_administrations, "MedicationAdministration.ndjson", meta_path, save)

            if verbose:
                print("**** treatment:")
                for treatment in treatments:
                    print(f"id: {treatment.id}, therapeutic_agent: {treatment.therapeutic_agent}")

            # expire session after the treatment transformation block to release memory
            session.expire_all()

        # Mutation 5GB file to Observation  ---------------------------------------------------
        if transform_mutation:

            mutations_query = session.query(CDAMutation)
            batch_size = 1000 # transform in batches. TODO: could use a smaller batch size

            # lookup the subjects for a mutation, cache the results
            @lru_cache(maxsize=None)
            def lookup_mutation_subjects(_session, integer_id_alias):
                _mutation_subjects = (
                    _session.query(CDASubject)
                    .join(CDASubjectMutation, CDASubject.integer_id_alias == CDASubjectMutation.subject_alias)
                    .filter(CDASubjectMutation.mutation_alias == integer_id_alias)
                    .all()
                )
                return _mutation_subjects

            for offset in range(0, mutations_query.count(), batch_size):
                mutations = mutations_query.offset(offset).limit(batch_size).all() # using offset and limit
                mutation_observations = []

                for mutation in mutations:
                    mutation_subjects = lookup_mutation_subjects(session, mutation.integer_id_alias)
                    mutation_observations.append(
                        mutation_transformer.create_mutation_observation(mutation, mutation_subjects[0]))

                # for mutation in mutations:
                #     mutation_subjects = (
                #         session.query(CDASubject)
                #         .join(CDASubjectMutation, CDASubject.integer_id_alias == CDASubjectMutation.subject_alias)
                #         .filter(CDASubjectMutation.mutation_alias == mutation.integer_id_alias)
                #         .all()
                #     )
                #
                #     mutation_observations.append(mutation_transformer.create_mutation_observation(mutation, mutation_subjects[0]))

                if mutation_observations:
                    fhir_mutation_obs = [orjson.loads(mo.json()) for mo in mutation_observations if mo]
                    utils.create_or_extend(new_items=fhir_mutation_obs, folder_path='data/META',
                                           resource_type='Observation', update_existing=False)

                # release memory after each batch
                session.expire_all()

            # if save and mutation_observations:
            #     mutation_obs = {mut_obs.id: mut_obs for mut_obs in mutation_observations if mut_obs}.values()
            #     fhir_mutation_obs = [orjson.loads(mo.json()) for mo in mutation_obs]
            #     utils.create_or_extend(new_items=fhir_mutation_obs, folder_path='data/META', resource_type='Observation', update_existing=False)

        # Specimen and Observation and BodyStructure -----------------------------------
        if not transform_treatment and not transform_condition and not transform_files and not transform_mutation:
            observations = []
            specimens = None
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
                utils.deduplicate_and_save(fhir_specimens, "Specimen.ndjson", meta_path, save)
                if specimen_bds:
                    utils.deduplicate_and_save(specimen_bds, "BodyStructure.ndjson", meta_path, save)

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
                utils.deduplicate_and_save(patients, "Patient.ndjson", meta_path, save)

            for subject in subjects:
                patient_identifiers = patient_transformer.patient_identifier(subject)
                patient_id = patient_transformer.patient_mintid(patient_identifiers[0])
                if subject.cause_of_death and patient_id:
                    obs = patient_transformer.observation_cause_of_death(subject.cause_of_death)
                    obs_identifier = Identifier(
                        **{'system': "https://cda.readthedocs.io/cause_of_death", 'value': "".join([str(patient_id), subject.cause_of_death])})
                    obs.id = patient_transformer.mint_id(identifier=obs_identifier, resource_type="Observation")
                    obs.subject = {"reference": f"Patient/{patient_id}"}
                    obs.focus = [{"reference": f"Patient/{patient_id}"}]
                    observations.append(obs)

                if subject.days_to_death and patient_id:
                    obs_days_to_death = patient_transformer.observation_days_to_death(subject.days_to_death)
                    obs_days_to_death_identifier = Identifier(
                        **{'system': "https://cda.readthedocs.io/days_to_death", 'value': "".join([str(patient_id), str(subject.days_to_death)])})
                    obs_days_to_death.id = patient_transformer.mint_id(identifier=obs_days_to_death_identifier, resource_type="Observation")
                    obs_days_to_death.subject = {"reference": f"Patient/{patient_id}"}
                    obs_days_to_death.focus = [{"reference": f"Patient/{patient_id}"}]
                    observations.append(obs_days_to_death)

                if subject.days_to_birth and patient_id:
                    obs_days_to_birth = patient_transformer.observation_days_to_birth(subject.days_to_birth)
                    obs_days_to_birth_identifier = Identifier(
                        **{'system': "https://cda.readthedocs.io/days_to_birth", 'value': "".join([str(patient_id), str(subject.days_to_birth)])})
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
                                # CRDC <- GDC, IDC, PDC, ICDC, CDS, HTAN, CMPC
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

                                    # print("Program:", _p.program, "Sub-Program:", _p.sub_program, "GDC:", _p.project_gdc,
                                    #       "PDC:", _p.project_pdc,
                                    #       "IDC:", _p.project_idc, "CDS:", _p.project_cds, "ICDC:", _p.project_icdc,
                                    #       "program_project_match:", _p_name)

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
                utils.deduplicate_and_save(research_studies, "ResearchStudy.ndjson", meta_path, save)

            if save and research_subjects:
                utils.deduplicate_and_save(research_subjects, "ResearchSubject.ndjson", meta_path, save)

            if save and observations:
                obs_dedup = {_obs.id: _obs for _obs in observations if _obs}.values()
                fhir_observation = [orjson.loads(_observation.json()) for _observation in obs_dedup]
                utils.create_or_extend(new_items=fhir_observation, folder_path='data/META', resource_type='Observation', update_existing=False)

            # expire session to release memory
            session.expire_all()

        # Condition and Observation -----------------------------------
        # randomly choose N diagnosis to reduce runtime for development
        # takes ~ 2hrs for 1041360+ diagnosis records
        if transform_condition:
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
            condition_observations = []
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
                                            condition_observations.append(observation)

                            if diagnosis.method_of_diagnosis:
                                obs_method_of_diagnosis = condition_transformer.observation_method_of_diagnosis(diagnosis.method_of_diagnosis)
                                obs_method_of_diagnosis_identifier = Identifier(
                                    **{'system': "https://cda.readthedocs.io/method_of_diagnosis",
                                       'value': "".join([_patient_diagnosis[0].id, diagnosis.method_of_diagnosis])})
                                obs_method_of_diagnosis.id = patient_transformer.mint_id(identifier=obs_method_of_diagnosis_identifier,
                                                                                         resource_type="Observation")
                                obs_method_of_diagnosis.subject = {"reference": f"Patient/{_patient_diagnosis[0].id}"}
                                obs_method_of_diagnosis.focus = [{"reference": f"Condition/{condition.id}"}]
                                condition_observations.append(obs_method_of_diagnosis)

            if save and conditions:
                utils.deduplicate_and_save(conditions, "Condition.ndjson", meta_path, save)

            if save and condition_observations:
                condition_observations_dedup = {_obs.id: _obs for _obs in condition_observations if _obs}.values()
                fhir_condition_observation = [orjson.loads(_observation.json()) for _observation in condition_observations_dedup]
                utils.create_or_extend(new_items=fhir_condition_observation, folder_path='data/META',
                                       resource_type='Observation', update_existing=False)

            # expire session to release memory
            session.expire_all()

        # File  -----------------------------------
        # requires pre-processing and validation
        # large record set -> 30+ GB takes time
        # TODO: test run
        if transform_files:
            batch_size = 1000
            total_files = session.query(func.count(CDAFile.id)).scalar()

            for offset in range(0, total_files, batch_size):
                files = session.query(CDAFile).options(
                    selectinload(CDAFile.file_subject_relation).selectinload(CDAFileSubject.subject),
                    selectinload(CDAFile.specimen_file_relation).selectinload(CDAFileSpecimen.specimen)
                ).offset(offset).limit(batch_size).all()

                assert files, "No files found in this batch."

                all_files = []
                all_groups = []
                for file in files:
                    print(f"File ID: {file.id}, File DRS URI: {file.drs_uri}")
                    _file_subjects = [
                        session.query(CDASubject).filter(CDASubject.id == file_subject.subject_id).first()
                        for file_subject in file.file_subject_relation
                    ]
                    _file_subjects = [subject for subject in _file_subjects if subject]
                    # print(f"++++++++++++++ FILE's SUBJECTS: {[_subject.id for _subject in _file_subjects]}")

                    _file_specimens = [
                        session.query(CDASpecimen).filter(CDASpecimen.id == file_specimen.specimen_id).first()
                        for file_specimen in file.specimen_file_relation
                    ]
                    _file_specimens = [specimen for specimen in _file_specimens if specimen]
                    # print(f"+++++++++++++ FILE's SPECIMENS: {[_specimen.id for _specimen in _file_specimens]}")

                    fhir_file = file_transformer.fhir_document_reference(file, _file_subjects, _file_specimens)
                    if fhir_file["DocumentReference"] and isinstance(fhir_file["DocumentReference"], DocumentReference):
                        all_files.append(fhir_file["DocumentReference"])

                    if "Group" in fhir_file and isinstance(fhir_file["Group"], Group):
                        all_groups.append(fhir_file["Group"])

                if save and all_files:
                    document_references = {_doc_ref.id: _doc_ref for _doc_ref in all_files if _doc_ref}.values()
                    fhir_document_references = [orjson.loads(doc_ref.json()) for doc_ref in document_references]
                    utils.create_or_extend(new_items=fhir_document_references, folder_path='data/META',
                                           resource_type='DocumentReference', update_existing=False)

                if save and all_groups:
                    groups = {group.id: group for group in all_groups if group.id}.values()
                    fhir_groups = [orjson.loads(group.json()) for group in groups]
                    utils.create_or_extend(new_items=fhir_groups, folder_path='data/META', resource_type='Group',
                                           update_existing=False)
                # expire session for this batch to release memory
                session.expire_all()

    finally:
        print("****** Closing Session ******")
        session.close()
