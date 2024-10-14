import json
import gzip
import os
import orjson
import pandas as pd
from fhir.resources import get_fhir_model_class


def is_gzipped(file_path):
    """Checks if a file is gzipped by reading its first two bytes."""
    with open(file_path, 'rb') as file:
        magic_number = file.read(2)
    return magic_number == b'\x1f\x8b'


def is_valid_json(file_path):
    """Checks a json file's format validity"""
    try:
        if is_gzipped(file_path):
            with gzip.open(file_path, 'rt', encoding='utf-8') as file:
                json.load(file)
        else:
            with open(file_path, 'r', encoding='utf-8') as file:
                json.load(file)

        print("Valid JSON")
        return True
    except json.JSONDecodeError as e:
        print(f"Invalid JSON: {e}")
        return False
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return False
    except UnicodeDecodeError as e:
        print(f"Decoding error: {e}")
        return False


def fix_json_format(file_path):
    """
    Fixes the JSON format of a file from {{}, {}, {}} to correct json format [{},{},{}].
    """
    if is_gzipped(file_path):
        open_func = lambda x, mode: gzip.open(x, mode, encoding='utf-8')
    else:
        open_func = lambda x, mode: open(x, mode, encoding='utf-8')

    with open_func(file_path, 'rt') as file:
        content = file.read()

    stripped_content = content.strip()
    if stripped_content.startswith('{') and stripped_content.endswith('}'):
        fixed_content = '[' + stripped_content[1:-1] + ']'
    else:
        print("file doesn't have the expected format.")
        return

    fixed_file_path = 'fixed_' + os.path.basename(file_path)
    fixed_dir = os.path.dirname(file_path)
    fixed_full_path = os.path.join(fixed_dir, fixed_file_path)

    with open_func(fixed_full_path, 'wt') as fixed_file:
        fixed_file.write(fixed_content)


def count_study_research_subjects(researchStudy_file_path, researchSubject_file_path, field, substring):
    study_ids = set()
    research_subject_count = 0

    # find all ResearchStudies with substring in identifier or partOf field
    with open(researchStudy_file_path, 'r') as file:
        for line in file:
            resource = json.loads(line)
            if resource['resourceType'] == 'ResearchStudy':
                if field == 'identifier':
                    if 'identifier' in resource:
                        identifiers = resource['identifier']
                        if any(substring in iden.get('value', '') for iden in identifiers):
                            study_ids.add(resource['id'])
                if field == 'partOf':
                    if 'partOf' in resource:
                        partOf = resource['partOf']
                        if any(substring in ref.get('reference', '') for ref in partOf):
                            study_ids.add(resource['id'])

    # count ResearchSubjects
    if study_ids:
        with open(researchSubject_file_path, 'r') as file:
            for line in file:
                resource = json.loads(line)
                if resource['resourceType'] == 'ResearchSubject':
                    if resource['study']['reference'].split('/')[-1] in study_ids:
                        research_subject_count += 1

    print(f'number of ResearchSubjects for studies with substring "{substring} is": {research_subject_count}')
    return research_subject_count


def count_patient_demographics(patient_path):
    white_count = 0
    black_count = 0
    native_count = 0
    hispanic_count = 0
    not_hispanic_count = 0
    male_count = 0
    female_count = 0
    deseased_count = 0
    alive_count = 0

    with open(patient_path, 'r') as file:
        for line in file:
            resource = json.loads(line)
            if resource['resourceType'] == 'Patient':
                if 'extension' in resource:
                    for ext in resource['extension']:
                        if 'url' in ext.keys():
                            if ext['url'] == "http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex":
                                if ext['valueCode'] == "M":
                                    male_count += 1
                                elif ext['valueCode'] == "F":
                                    female_count += 1
                            if ext['url'] == "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race":
                                if ext['valueString'] == 'White':
                                    white_count += 1
                                elif ext['valueString'] == 'Black or African American':
                                    black_count += 1
                                elif ext['valueString'] == 'Native Hawaiian or Other Pacific Islander':
                                    native_count += 1
                            if ext['url'] == "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity":
                                if ext['valueString'] == "not hispanic or latino":
                                    not_hispanic_count += 1
                                elif ext['valueString'] == 'hispanic or latino':
                                    hispanic_count += 1

                if 'deseasedBoolean' in resource:
                    if resource['deseasedBoolean']:
                        deseased_count += 1
                    else:
                        alive_count += 1

    print(f'number of males: {male_count} and females: {female_count} found in CDA2FHIR data.')
    print(f'number of deseased: {male_count} and alive: {female_count} patients found in CDA2FHIR data.')
    print(f'number of white: {white_count}, black: {black_count}, and native or pacific islander: {native_count} patients race found in CDA2FHIR data.')
    print(f'number of hispanic: {hispanic_count} and non hispanic: {not_hispanic_count} patients ethnicity found in CDA2FHIR data.')


def create_project_program_relations(path="data/raw/Identifier_maps"):
    """combines all CDA's project's relations into a single associative table with it's associated program membership"""
    # GDC_project_id, CDS_study_id
    cds_gdc = pd.read_excel("data/raw/Identifier_maps/naive_CDS-GDC_project_id_map.hand_edited_to_remove_false_positives.xlsx")
    # CDS_study_id IDC_collection_id
    cds_idc = pd.read_excel("data/raw/Identifier_maps/naive_CDS-IDC_project_id_map.hand_edited_to_remove_false_positives.xlsx")
    # CDS_study_id PDC_pdc_study_id
    cds_pdc = pd.read_excel("data/raw/Identifier_maps/naive_CDS-PDC_project_id_map.hand_edited_to_remove_false_positives.xlsx")
    # GDC_project_id IDC_collection_id
    gdc_idc = pd.read_excel("data/raw/Identifier_maps/naive_GDC-IDC_project_id_map.hand_edited_to_remove_false_positives.xlsx")
    # GDC_project_id PDC_pdc_study_id
    gdc_pdc = pd.read_excel("data/raw/Identifier_maps/naive_GDC-PDC_project_id_map.hand_edited_to_remove_false_positives.xlsx")
    # ICDC_study_id IDC_collection_id
    icdc_idc = pd.read_excel("data/raw/Identifier_maps/naive_ICDC-IDC_project_id_map.hand_edited_to_remove_false_positives.xlsx")
    # IDC_collection_id PDC_pdc_study_id
    idc_pdc = pd.read_excel("data/raw/Identifier_maps/naive_IDC-PDC_project_id_map.hand_edited_to_remove_false_positives.xlsx")

    # subject_project = pd.read_csv("data/raw/association_tables/subject_associated_project.tsv", sep="\t")
    # checked membership and counts ex.
    # subject_project[subject_project['associated_project'].isin(idc_pdc.PDC_pdc_study_id)].associated_project.unique()

    cds_gdc = cds_gdc.rename(columns={'GDC_project_id': 'project_a', 'CDS_study_id': 'project_b', 'CDS_program_acronym': 'sub_program'})[['project_a', 'project_b', 'sub_program']]
    cds_gdc['program_a'] = 'GDC'
    cds_gdc['program_b'] = 'CDS'

    cds_idc = cds_idc.rename(columns={'CDS_study_id': 'project_a', 'IDC_collection_id': 'project_b', 'CDS_program_acronym': 'sub_program'})[['project_a', 'project_b', 'sub_program']]
    cds_idc['program_a'] = 'CDS'
    cds_idc['program_b'] = 'IDC'

    cds_pdc = cds_pdc.rename(columns={'CDS_study_id': 'project_a', 'PDC_pdc_study_id': 'project_b', 'CDS_program_acronym': 'sub_program'})[['project_a', 'project_b', 'sub_program']]
    cds_pdc['program_a'] = 'CDS'
    cds_pdc['program_b'] = 'PDC'

    gdc_idc = gdc_idc.rename(columns={'GDC_project_id': 'project_a', 'IDC_collection_id': 'project_b', 'GDC_program_name': 'sub_program'})[['project_a', 'project_b', 'sub_program']]
    gdc_idc['program_a'] = 'GDC'
    gdc_idc['program_b'] = 'IDC'

    gdc_pdc = gdc_pdc.rename(columns={'GDC_project_id': 'project_a', 'PDC_pdc_study_id': 'project_b', 'GDC_program_name': 'sub_program'})[['project_a', 'project_b', 'sub_program']]
    gdc_pdc['program_a'] = 'GDC'
    gdc_pdc['program_b'] = 'PDC'

    icdc_idc = icdc_idc.rename(columns={'ICDC_study_id': 'project_a', 'IDC_collection_id': 'project_b', 'ICDC_program_acronym': 'sub_program'})[['project_a', 'project_b', 'sub_program']]
    icdc_idc['program_a'] = 'ICDC'
    icdc_idc['program_b'] = 'IDC'

    idc_pdc = idc_pdc.rename(columns={'IDC_collection_id': 'project_a', 'PDC_pdc_study_id': 'project_b', 'GDC_program_name': 'sub_program'})[['project_a', 'project_b', 'sub_program']]
    idc_pdc['program_a'] = 'IDC'
    idc_pdc['program_b'] = 'PDC'

    project_relations_df = pd.concat([cds_gdc, cds_idc, cds_pdc, gdc_idc, gdc_pdc, icdc_idc, idc_pdc], ignore_index=True)
    project_relations_df.drop_duplicates(inplace=True)

    if os.path.exists(path):
        _file_name = "project_program_relations.csv"
        file_path = os.path.join(path, _file_name)
        project_relations_df.to_csv(file_path, index=False)
        print(f"Successfully saved projects relations at: {file_path}")
        return project_relations_df
    else:
        print(f"The directory '{path}' does not exist.")


def initial_project_program_relations():
    df = create_project_program_relations(path="data/raw/Identifier_maps")
    gdc_projects = list(set(df.project_a[df.program_a.isin(['GDC'])]))
    gdc_projects.sort()
    _summary = pd.DataFrame(data={'project_gdc': gdc_projects})
    # df[df.program_a.isin(['GDC'])][df.program_b.isin(['PDC'])][['project_a' , 'project_b']].shape[0]

    _summary = pd.merge(_summary, df[df.program_a.isin(['GDC'])][df.program_b.isin(['PDC'])][['project_a', 'project_b']], left_on='project_gdc', right_on='project_a', how='left')[['project_gdc', 'project_b']]
    _summary.rename(columns={'project_b': 'project_pdc'}, inplace=True)

    df_idc_pdc = df[df.program_a.isin(['IDC'])][df.program_b.isin(['PDC'])]
    df_idc_pdc.rename(columns={'project_a': 'project_idc'}, inplace=True)
    _summary = pd.merge(_summary, df_idc_pdc, left_on='project_pdc', right_on='project_b', how='left')[['project_gdc', 'project_pdc', 'project_idc']]

    df_gdc_cds = df[df.program_a.isin(['GDC'])][df.program_b.isin(['CDS'])]
    df_gdc_cds.rename(columns={'project_b': 'project_cds'}, inplace=True)
    _summary = pd.merge(_summary, df_gdc_cds, left_on='project_gdc', right_on='project_a', how='left')[['project_gdc', 'project_pdc', 'project_idc']]
    pd.merge(_summary, df_gdc_cds, left_on='project_gdc', right_on='project_a', how='left')[['project_gdc', 'project_pdc', 'project_idc', 'project_cds']].to_csv("data/raw/Identifier_maps/project_program_relations_summary.csv")


def is_valid_fhir_resource_type(resource_type):
    try:
        model_class = get_fhir_model_class(resource_type)
        return model_class is not None
    except KeyError:
        return False


def create_or_extend(new_items, folder_path='META', resource_type='Observation', update_existing=False):
    assert is_valid_fhir_resource_type(resource_type), f"Invalid resource type: {resource_type}"

    file_name = "".join([resource_type, ".ndjson"])
    file_path = os.path.join(folder_path, file_name)

    file_existed = os.path.exists(file_path)

    existing_data = {}

    if file_existed:
        with open(file_path, 'r') as file:
            for line in file:
                try:
                    item = orjson.loads(line)
                    existing_data[item.get("id")] = item
                except orjson.JSONDecodeError:
                    continue

    for new_item in new_items:
        new_item_id = new_item["id"]
        if new_item_id not in existing_data or update_existing:
            existing_data[new_item_id] = new_item

    with open(file_path, 'w') as file:
        for item in existing_data.values():
            file.write(orjson.dumps(item).decode('utf-8') + '\n')

    if file_existed:
        if update_existing:
            print(f"{file_name} has new updates to existing data.")
        else:
            print(f"{file_name} has been extended, without updating existing data.")
    else:
        print(f"{file_name} has been created.")