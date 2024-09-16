import json
import gzip
import os


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

