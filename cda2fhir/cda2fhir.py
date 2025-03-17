import logging
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
    CDASubjectProject, CDAResearchSubjectDiagnosis, CDASpecimen, ProjectdbGap, GDCProgramdbGap, \
    CDASubjectIdentifier, CDAProjectRelation, CDAFile, CDAFileSubject, CDAFileSpecimen, CDAResearchSubjectTreatment, \
    CDAMutation, CDASubjectMutation
from cda2fhir.transformer import PatientTransformer, ResearchStudyTransformer, ResearchSubjectTransformer, \
    ConditionTransformer, SpecimenTransformer, DocumentReferenceTransformer, MedicationAdministrationTransformer, \
    MutationTransformer
from sqlalchemy import select, func, or_,  and_
from sqlalchemy.orm import aliased
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import contains_eager

gdc_dbgap_names = ['APOLLO', 'CDDP_EAGLE', 'CGCI', 'CTSP', 'EXCEPTIONAL_RESPONDERS', 'FM', 'HCMI', 'MMRF', 'NCICCR',
                   'OHSU', 'ORGANOID', 'REBC', 'TARGET', 'TCGA', 'TRIO', 'VAREPOP', 'WCDT']


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler("cda2fhir.log", mode="w")
file_handler.setLevel(logging.DEBUG)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

logger.debug("logging is configured....")


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
            substance_definitions = []
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
                            substance_definitions.append(sd)
                            substance = treatment_transformer.create_substance(compound_name=compound_name, substance_definition=sd)
                            if substance:
                                substances.append(substance)

                                medication = treatment_transformer.create_medication(compound_name=compound_name,
                                                                                     treatment_type=None,
                                                                                     _substance=substance)
                                if medication:
                                    medications.append(medication)

            if save and substance_definitions:
                substance_definitions = utils.load_list_entities(substance_definitions)
                cleaned_substance_definitions = utils.clean_resources(substance_definitions)
                utils.deduplicate_and_save(cleaned_substance_definitions, "SubstanceDefinition.ndjson", meta_path, save)

            if save and substances:
                substances = utils.load_list_entities(substances)
                cleaned_substances = utils.clean_resources(substances)
                utils.deduplicate_and_save(cleaned_substances, "Substance.ndjson", meta_path, save)

            if save and medications:
                medications = utils.load_list_entities(medications)
                cleaned_medications = utils.clean_resources(medications)
                utils.deduplicate_and_save(cleaned_medications, "Medication.ndjson", meta_path, save)

            for treatment in treatments:
                _subject_treatment = (
                    session.query(CDASubject)
                    .join(CDASubjectResearchSubject, CDASubject.integer_id_alias == CDASubjectResearchSubject.subject_alias)
                    .join(CDAResearchSubject, CDASubjectResearchSubject.researchsubject_alias == CDAResearchSubject.integer_id_alias)
                    .join(CDAResearchSubjectTreatment, CDAResearchSubject.integer_id_alias == CDAResearchSubjectTreatment.researchsubject_alias)
                    .filter(CDAResearchSubjectTreatment.treatment_alias == treatment.integer_id_alias)
                    .all()
                )
                #     _subject_treatment = (
                #         session.query(CDASubject)
                #         .join(CDASubjectResearchSubject, CDASubject.id == CDASubjectResearchSubject.subject_id)
                #         .join(CDAResearchSubject, CDASubjectResearchSubject.researchsubject_id == CDAResearchSubject.id)
                #         .join(CDAResearchSubjectTreatment,
                #               CDAResearchSubject.id == CDAResearchSubjectTreatment.researchsubject_id)
                #         .filter(CDAResearchSubjectTreatment.treatment_id == treatment.id)
                #         .all()
                #     )

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
                medication_administrations = utils.load_list_entities(medication_administrations)
                cleaned_medication_administrations = utils.clean_resources(medication_administrations)
                utils.deduplicate_and_save(cleaned_medication_administrations, "MedicationAdministration.ndjson", meta_path, save)

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
                # _mutation_subjects = (
                #     _session.query(CDASubject)
                #     .join(CDASubjectMutation, CDASubject.integer_id_alias == CDASubjectMutation.subject_alias)
                #     .filter(CDASubjectMutation.mutation_alias == integer_id_alias)
                #     .all()
                # )
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
                    cleaned_fhir_mutation_obs = utils.clean_resources(fhir_mutation_obs)
                    utils.create_or_extend(new_items=cleaned_fhir_mutation_obs, folder_path='data/META',
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
                    specimen_project_name =  specimen.associated_project
                    _specimen_patient = patient_transformer.transform_human_subjects([_cda_subject])
                    fhir_specimen = specimen_transformer.fhir_specimen(specimen, _specimen_patient[0])

                    if fhir_specimen:
                        fhir_specimens.append(fhir_specimen)

                        specimen_bd = specimen_transformer.specimen_body_structure(specimen, _specimen_patient[0], fhir_specimen=fhir_specimen, part_of_study_extensions=None)
                        if specimen_bd:
                            specimen_bds.append(specimen_bd)

                        _specimen_obs = specimen_transformer.specimen_observation(specimen, _specimen_patient[0],
                                                                                  fhir_specimen.id, fhir_specimen)
                        if _specimen_obs:
                            observations.append(_specimen_obs)

            if save and fhir_specimens:
                fhir_specimens = utils.load_list_entities(fhir_specimens)
                cleaned_fhir_specimens = utils.clean_resources(fhir_specimens)
                utils.deduplicate_and_save(cleaned_fhir_specimens, "Specimen.ndjson", meta_path, save)
                if specimen_bds:
                    specimen_bds = utils.load_list_entities(specimen_bds)
                    cleaned_specimen_bds = utils.clean_resources(specimen_bds)
                    utils.deduplicate_and_save(cleaned_specimen_bds, "BodyStructure.ndjson", meta_path, save)

            # Patient and Observation -----------------------------------
            subjects = session.query(CDASubject).all()
            if verbose:
                for subject in subjects:
                    print(f"id: {subject.id}, species: {subject.species}, sex: {subject.sex}")

            cda_research_subjects = session.query(CDAResearchSubject).all()
            if verbose:
                print("==== research subjects:")
                for cda_research_subject in cda_research_subjects:
                    print(
                        f"id: {cda_research_subject.id}, project: {cda_research_subject.member_of_research_project}, condition: {cda_research_subject.primary_diagnosis_condition}")

            patients = patient_transformer.transform_human_subjects(subjects)
            logger.debug(f"For subjects:{subjects} \nFound {len(patients)} patient records.")

            if save and patients:
                patients = utils.load_list_entities(patients)
                cleaned_patients = utils.clean_resources(patients)
                utils.deduplicate_and_save(cleaned_patients, "Patient.ndjson", meta_path, save)

            for subject in subjects:
                patient_identifiers = patient_transformer.patient_identifier(subject)
                patient_id = patient_transformer.patient_mintid(patient_identifiers[0])

                part_ext = []
                patient_transformer.get_part_of_study_extension(subject, extensions=part_ext)


                if subject.cause_of_death and patient_id:
                    obs = patient_transformer.observation_cause_of_death(subject.cause_of_death)
                    obs_identifier = Identifier(
                        **{'system': "https://cda.readthedocs.io/cause_of_death", 'value': "".join([str(patient_id), subject.cause_of_death])})
                    obs.id = patient_transformer.mint_id(identifier=obs_identifier, resource_type="Observation")
                    obs.subject = {"reference": f"Patient/{patient_id}"}
                    obs.focus = [{"reference": f"Patient/{patient_id}"}]
                    if part_ext and (not hasattr(obs, "extension") or not obs.extension):
                        obs.extension = part_ext
                    observations.append(obs)

                if subject.days_to_death and patient_id:
                    obs_days_to_death = patient_transformer.observation_days_to_death(subject.days_to_death)
                    obs_days_to_death_identifier = Identifier(
                        **{'system': "https://cda.readthedocs.io/days_to_death", 'value': "".join([str(patient_id), str(subject.days_to_death)])})
                    obs_days_to_death.id = patient_transformer.mint_id(identifier=obs_days_to_death_identifier, resource_type="Observation")
                    obs_days_to_death.subject = {"reference": f"Patient/{patient_id}"}
                    obs_days_to_death.focus = [{"reference": f"Patient/{patient_id}"}]
                    if part_ext and (not hasattr(obs_days_to_death, "extension") or not obs_days_to_death.extension):
                        obs_days_to_death.extension = part_ext
                    observations.append(obs_days_to_death)

                if subject.days_to_birth and patient_id:
                    obs_days_to_birth = patient_transformer.observation_days_to_birth(subject.days_to_birth)
                    obs_days_to_birth_identifier = Identifier(
                        **{'system': "https://cda.readthedocs.io/days_to_birth", 'value': "".join([str(patient_id), str(subject.days_to_birth)])})
                    obs_days_to_birth.id = patient_transformer.mint_id(identifier=obs_days_to_birth_identifier, resource_type="Observation")
                    obs_days_to_birth.subject = {"reference": f"Patient/{patient_id}"}
                    obs_days_to_birth.focus = [{"reference": f"Patient/{patient_id}"}]
                    if part_ext and (not hasattr(obs_days_to_birth, "extension") or not obs_days_to_birth.extension):
                        obs_days_to_birth.extension = part_ext
                    observations.append(obs_days_to_birth)

            # ResearchStudy and ResearchSubject -----------------------------------
            def get_research_subjects_for_subject(subject, fallback_mapping):
                """
                Return the list of research subjects for a subject.
                Use the eager-loaded relationship if present; otherwise fall back
                to the cached result from a batch query keyed by project code.
                """
                rs_list = [assoc.researchsubject for assoc in subject.researchsubject_subjects]
                if not rs_list and '.' in subject.id:
                    project_code = subject.id.split('.')[0]
                    rs_list = fallback_mapping.get(project_code, [])
                return rs_list

            def append_identifiers_to_study(session, study):
                """
                Append identifiers from GDCProgramdbGap and ProjectdbGap to the study.
                """
                try:
                    gdc_dbgap = session.query(GDCProgramdbGap).filter(
                        GDCProgramdbGap.GDC_program_name.contains(study.name)
                    ).one_or_none()
                    if gdc_dbgap:
                        study.identifier.append(
                            Identifier(
                                system="https://www.ncbi.nlm.nih.gov/dbgap_accession_number",
                                value=gdc_dbgap[0],
                                use="secondary"
                            )
                        )
                    dbGap = session.query(ProjectdbGap).filter_by(
                        GDC_project_id=study.name
                    ).first()
                    if dbGap:
                        study.identifier.append(
                            Identifier(
                                system="https://www.ncbi.nlm.nih.gov/dbgap_accession_number",
                                value=dbGap.dbgap_study_accession,
                                use="secondary"
                            )
                        )
                    return study
                except Exception:
                    logger.exception("Error retrieving identifiers for study %s", study.name)
                    raise

            def compute_part_of_references(session, subject, proj_relation_map, research_studies):
                """
                Compute the partOf references for a subject using a preloaded proj_relation_map.
                Returns a deduplicated list of FHIR Reference objects based on their 'reference' string.
                """
                part_refs = []
                try:
                    parts = subject.id.split(".")
                    if len(parts) < 2:
                        raise ValueError(f"Unexpected subject id format: {subject.id}")
                    subject_id_value = parts[1]

                    identifier_results = session.execute(
                        select(CDASubjectIdentifier).filter_by(
                            subject_alias=subject.integer_id_alias,
                            value=subject_id_value
                        )
                    ).all()
                    for (identifier_obj,) in identifier_results:
                        prog_study = research_study_transformer.program_research_study(name=identifier_obj.system)
                        if prog_study:
                            research_studies.append(prog_study)
                            ref = Reference(reference=f"ResearchStudy/{prog_study.id}")
                            part_refs.append(ref)

                    proj_assoc = session.query(CDASubjectProject).filter(
                        CDASubjectProject.subject_alias == subject.integer_id_alias
                    ).first()
                    if proj_assoc:
                        project_name = proj_assoc.associated_project
                        rels = proj_relation_map.get(project_name, [])
                        for rel in rels:
                            for attr, default in [("project_gdc", "GDC"), ("project_pdc", "PDC"),
                                                  ("project_idc", "IDC"), ("project_cds", "CDS"),
                                                  ("project_icdc", "ICDC")]:
                                if getattr(rel, attr) == project_name:
                                    prog = research_study_transformer.program_research_study(name=default)
                                    if prog:
                                        research_studies.append(prog)
                                        ref = Reference(reference=f"ResearchStudy/{prog.id}")
                                        part_refs.append(ref)
                            for field in ("program", "sub_program"):
                                val = getattr(rel, field)
                                if val:
                                    prog = research_study_transformer.program_research_study(name=val)
                                    if prog:
                                        research_studies.append(prog)
                                        ref = Reference(reference=f"ResearchStudy/{prog.id}")
                                        part_refs.append(ref)

                    unique_refs = {}
                    for ref in part_refs:
                        if ref.reference not in unique_refs:
                            unique_refs[ref.reference] = ref
                    return list(unique_refs.values())

                except Exception:
                    logger.exception("Error retrieving partOf references for subject %s", subject.id)
                    raise

            def process_projects(session):
                """
                Process projects to create one ResearchStudy per project and a set of ResearchSubject
                resources for each subject linked to that project.
                Uses eager loading and batch queries to reduce database round trips.
                Returns:
                    research_studies, research_subjects: lists of FHIR ResearchStudy and ResearchSubject objects.
                """
                research_studies = []
                research_subjects = []

                unique_project_objs = (
                    session.query(CDASubjectProject)
                    .filter(CDASubjectProject.associated_project.isnot(None))
                    .group_by(CDASubjectProject.associated_project)
                    .all()
                )
                logger.info("Unique project objects: %s", unique_project_objs)

                project_names = [proj.associated_project for proj in unique_project_objs]

                project_relations = session.query(CDAProjectRelation).filter(
                    or_(
                        CDAProjectRelation.project_gdc.in_(project_names),
                        CDAProjectRelation.project_pdc.in_(project_names),
                        CDAProjectRelation.project_idc.in_(project_names),
                        CDAProjectRelation.project_cds.in_(project_names),
                        CDAProjectRelation.project_icdc.in_(project_names)
                    )
                ).all()
                proj_relation_map = {}
                for rel in project_relations:
                    for attr in ("project_gdc", "project_pdc", "project_idc", "project_cds", "project_icdc"):
                        proj = getattr(rel, attr)
                        if proj in project_names:
                            proj_relation_map.setdefault(proj, []).append(rel)

                fallback_mapping = {}

                def get_fallback_research_subjects(project_code):
                    if project_code not in fallback_mapping:
                        fallback_mapping[project_code] = session.query(CDAResearchSubject).filter(
                            CDAResearchSubject.member_of_research_project == project_code
                        ).all()
                    return fallback_mapping[project_code]

                for project_obj in unique_project_objs:
                    project_name = project_obj.associated_project
                    subjects = (
                        session.query(CDASubject)
                        .options(joinedload(CDASubject.researchsubject_subjects))
                        .join(CDASubjectProject, CDASubject.integer_id_alias == CDASubjectProject.subject_alias)
                        .filter(CDASubjectProject.associated_project == project_name)
                        .all()
                    )
                    logger.info("Project '%s': %d subjects found", project_name, len(subjects))

                    project_rs = []
                    for subject in subjects:
                        rs_list = [assoc.researchsubject for assoc in subject.researchsubject_subjects]
                        if not rs_list and '.' in subject.id:
                            project_code = subject.id.split('.')[0]
                            rs_list = get_fallback_research_subjects(project_code)
                        project_rs.extend(rs_list)

                    if not project_rs:
                        logger.warning("No research subjects found for any subject in project '%s'", project_name)
                        continue

                    rep_rs = project_rs[0]
                    try:
                        study = research_study_transformer.research_study(project_obj, rep_rs)
                        study = append_identifiers_to_study(session, study)
                        logger.info("Created research study for project '%s': %s", project_name, study)
                    except Exception:
                        logger.exception("Error creating research study for project '%s'", project_name)
                        continue

                    for subject in subjects:
                        try:
                            patient_data = patient_transformer.subject_to_patient(subject)
                            logger.info("Patient data for subject %s obtained", subject.id)
                        except Exception:
                            logger.exception("Error transforming patient data for subject %s", subject.id)
                            continue

                        rs_list = get_research_subjects_for_subject(subject, fallback_mapping)
                        if not rs_list:
                            logger.warning("No research subjects found for subject %s in project %s", subject.id,
                                           project_name)
                            continue

                        for rs in rs_list:
                            if rs:
                                try:
                                    rsub = research_subject_transformer.research_subject(rs, patient_data, study)
                                    research_subjects.append(rsub)
                                    logger.info("Created research subject: %s", rsub)
                                except Exception:
                                    logger.exception("Error creating research subject for subject %s", subject.id)
                                    continue

                        try:
                            part_refs = compute_part_of_references(session, subject, proj_relation_map, research_studies)
                            if part_refs:
                                existing_refs = {ref.reference: ref for ref in study.partOf} if study.partOf else {}
                                for ref in part_refs:
                                    existing_refs.setdefault(ref.reference, ref)
                                study.partOf = list(existing_refs.values())
                        except Exception:
                            logger.exception("Error computing partOf references for subject %s and study %s",
                                             subject.id, study.id)

                    research_studies.append(study)

                return research_studies, research_subjects

            research_studies, research_subjects = process_projects(session)

            if save and research_studies:
                research_studies = utils.load_list_entities(research_studies)
                cleaned_research_studies = utils.clean_resources(research_studies)
                utils.deduplicate_and_save(cleaned_research_studies, "ResearchStudy.ndjson", meta_path, save)

            if save and research_subjects:
                research_subjects = utils.load_list_entities(research_subjects)
                cleaned_research_subjects = utils.clean_resources(research_subjects)
                utils.deduplicate_and_save(cleaned_research_subjects, "ResearchSubject.ndjson", meta_path, save)

            if save and observations:
                obs_dedup = {_obs.id: _obs for _obs in observations if _obs}.values()
                fhir_observation = [orjson.loads(_observation.json()) for _observation in obs_dedup]
                cleaned_fhir_observation = utils.clean_resources(fhir_observation)
                utils.create_or_extend(new_items=cleaned_fhir_observation, folder_path='data/META', resource_type='Observation', update_existing=False)

            # Release session memory
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
                # _subject_diagnosis = (
                #     session.query(CDASubject)
                #     .join(CDASubjectResearchSubject, CDASubject.id == CDASubjectResearchSubject.subject_id)
                #     .join(CDAResearchSubject, CDASubjectResearchSubject.researchsubject_id == CDAResearchSubject.id)
                #     .join(CDAResearchSubjectDiagnosis,
                #           CDAResearchSubject.id == CDAResearchSubjectDiagnosis.researchsubject_id)
                #     .filter(CDAResearchSubjectDiagnosis.diagnosis_id == diagnosis.id)
                #     .all()
                # )
                _subject_diagnosis = (
                    session.query(CDASubject)
                    .join(CDASubjectResearchSubject, CDASubject.integer_id_alias == CDASubjectResearchSubject.subject_alias)
                    .join(CDAResearchSubject, CDASubjectResearchSubject.researchsubject_alias == CDAResearchSubject.integer_id_alias)
                    .join(CDAResearchSubjectDiagnosis, CDAResearchSubject.integer_id_alias == CDAResearchSubjectDiagnosis.researchsubject_alias)
                    .filter(CDAResearchSubjectDiagnosis.diagnosis_alias == diagnosis.integer_id_alias)
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
                conditions = utils.load_list_entities(conditions)
                cleaned_conditions = utils.clean_resources(conditions)
                utils.deduplicate_and_save(cleaned_conditions, "Condition.ndjson", meta_path, save)

            if save and condition_observations:
                condition_observations_dedup = {_obs.id: _obs for _obs in condition_observations if _obs}.values()
                fhir_condition_observation = [orjson.loads(_observation.json()) for _observation in condition_observations_dedup]
                cleaned_condition_observations = utils.clean_resources(fhir_condition_observation)
                utils.create_or_extend(new_items=cleaned_condition_observations, folder_path='data/META',
                                       resource_type='Observation', update_existing=False)

            # expire session to release memory
            session.expire_all()

        # File  -----------------------------------
        # requires pre-processing and validation
        # large record set -> 30+ GB takes time
        # TODO: test run
        if transform_files:
            batch_size = 1000

            @lru_cache(maxsize=100)
            def lookup_file_specimens(_file):
                return [
                    file_specimen.specimen
                    for file_specimen in _file.specimen_file_relation
                    if file_specimen.specimen
                ]

            @lru_cache(maxsize=100)
            def lookup_file_subjects(_file):
                return [
                    file_subject.subject
                    for file_subject in _file.file_subject_relation
                    if file_subject.subject
                ]

            # files = (
            #     session.query(CDAFile)
            #     .join(CDAFileSpecimen, CDAFile.id == CDAFileSpecimen.file_id)
            #     .join(CDASpecimen, CDAFileSpecimen.specimen_id == CDASpecimen.id)
            #     # .filter(CDASpecimen.id.isnot(None))
            #     .options(
            #         selectinload(CDAFile.file_subject_relation).selectinload(CDAFileSubject.subject),
            #         selectinload(CDAFile.specimen_file_relation).selectinload(CDAFileSpecimen.specimen),
            #     )
            #     .all()
            # )

            # stmt = (
            #     select(CDAFile)
            #     .outerjoin(CDAFileSpecimen, CDAFile.id == CDAFileSpecimen.file_id)
            #     .outerjoin(CDASpecimen, CDAFileSpecimen.specimen_id == CDASpecimen.id)
            #     .outerjoin(specimen_subject_alias, CDASpecimen.derived_from_subject == specimen_subject_alias.id)
            #     .filter(
            #         and_(
            #             CDASpecimen.derived_from_subject.isnot(None),
            #             specimen_subject_alias.species.in_({'Human', 'Homo sapiens'}),
            #         )
            #     )
            #     .options(
            #         contains_eager(CDAFile.specimen_file_relation),
            #     )
            # )
            #
            # files = session.execute(stmt).unique().scalars().all() # eager loading causes memory usage with duplicate results

            file_subject_alias = aliased(CDASubject)
            specimen_subject_alias = aliased(CDASubject)

            # stmt = (
            #     select(CDAFile)
            #     .outerjoin(CDAFileSpecimen, CDAFile.id == CDAFileSpecimen.file_id)
            #     .outerjoin(CDASpecimen, CDAFileSpecimen.specimen_id == CDASpecimen.id)
            #     .outerjoin(CDAFileSubject, CDAFile.id == CDAFileSubject.file_id)
            #     .outerjoin(file_subject_alias, CDAFileSubject.subject_id == file_subject_alias.id)
            #     .outerjoin(specimen_subject_alias, CDASpecimen.derived_from_subject == specimen_subject_alias.id)
            #     .filter(
            #         or_(
            #             file_subject_alias.species.in_({'Human', 'Homo sapiens'}),
            #             CDASpecimen.derived_from_subject.isnot(None),
            #             and_(
            #                 CDASpecimen.derived_from_subject.isnot(None),
            #                 specimen_subject_alias.species.in_({'Human', 'Homo sapiens'}),
            #             ),
            #         )
            #     )
            # )
            # files = session.execute(stmt).scalars().all()
            stmt = (
                select(CDAFile)
                .outerjoin(CDAFileSpecimen, CDAFile.integer_id_alias == CDAFileSpecimen.file_alias)
                .outerjoin(CDASpecimen, CDASpecimen.integer_id_alias == CDAFileSpecimen.specimen_alias)
                .outerjoin(CDAFileSubject, CDAFile.integer_id_alias == CDAFileSubject.file_alias)
                .outerjoin(file_subject_alias, CDAFileSubject.subject_alias == file_subject_alias.integer_id_alias)
                .outerjoin(specimen_subject_alias, CDASpecimen.derived_from_subject == specimen_subject_alias.id)
            )

            files = session.execute(stmt).scalars().all()

            if not files:
                print("No valid files found. Skipping file transformation.")
                session.expire_all()
            else:
                for offset in range(0, len(files), batch_size):
                    all_files = []
                    all_groups = []
                    session.expire_all()
                    for file in files:
                        print(f"File ID: {file.id}, File DRS URI: {file.drs_uri}")
                        _file_specimens = lookup_file_specimens(file)
                        print(f"Specimen relation: {file.specimen_file_relation}")

                        # if not _file_specimens:
                        #     print(f"------------- No specimens found for File ID: {file.id}. Skipping...")
                        #     continue

                        _file_subjects = lookup_file_subjects(file)
                        print(f"Subject relation: {file.file_subject_relation}")

                        fhir_file = file_transformer.fhir_document_reference(file, _file_subjects, _file_specimens)
                        if fhir_file["DocumentReference"] and isinstance(fhir_file["DocumentReference"], DocumentReference):
                            all_files.append(fhir_file["DocumentReference"])

                        if "Group" in fhir_file and isinstance(fhir_file["Group"], Group):
                            all_groups.append(fhir_file["Group"])

                    if save and all_files:
                        document_references = {_doc_ref.id: _doc_ref for _doc_ref in all_files if _doc_ref}.values()
                        fhir_document_references = [orjson.loads(doc_ref.json()) for doc_ref in document_references]
                        cleaned_fhir_document_references = utils.clean_resources(fhir_document_references)
                        utils.create_or_extend(new_items=cleaned_fhir_document_references, folder_path='data/META',
                                               resource_type='DocumentReference', update_existing=False)

                    if save and all_groups:
                        groups = {group.id: group for group in all_groups if group.id}.values()
                        fhir_groups = [orjson.loads(group.json()) for group in groups]
                        cleaned_fhir_groups = utils.clean_resources(fhir_groups)
                        utils.create_or_extend(new_items=cleaned_fhir_groups, folder_path='data/META', resource_type='Group',
                                               update_existing=False)
                    # expire session for this batch to release memory
                    session.expire_all()

    finally:
        print("****** Closing Session ******")
        session.close()
