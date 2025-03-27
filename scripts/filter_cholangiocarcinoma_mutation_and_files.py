import json
from pathlib import Path
import importlib.resources

RAW_DATA_PATH = Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw_022025')

diagnosis_file = RAW_DATA_PATH / 'diagnosis.json'
diagnosis_research_subject_file = RAW_DATA_PATH / 'researchsubject_diagnosis.json'
subject_researchsubject_file = RAW_DATA_PATH / 'subject_researchsubject.json'
subject_mutation_file = RAW_DATA_PATH / 'subject_mutation.json'
mutation_file = RAW_DATA_PATH / 'mutation.json'

# ------------------
def load_json_file(file_path: Path):
    with open(file_path, 'r') as f:
        return json.load(f)

def save_json_file(data, file_path: Path):
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=4)

def combine_json_files(directory: Path, combined_filename: str = None) -> list:
    """
    combine all JSON files in a directory - optionally save the combined list to a file.
    """
    combined_data = []
    json_files = list(directory.glob('*.json'))
    for file in json_files:
        data = load_json_file(file)
        if isinstance(data, list):
            combined_data.extend(data)
        else:
            raise ValueError(f"File {file} does not contain a list as expected.")
    if combined_filename:
        output_path = directory / combined_filename
        save_json_file(combined_data, output_path)
        print(f"Combined data saved in {output_path}. Total records: {len(combined_data)}")
    return combined_data

def filter_list_by_key(data: list, key: str, allowed_values: set) -> list:
    """list of dicts from data where dict[key] is in allowed_values."""
    return [item for item in data if item.get(key) in allowed_values]

# ------------------
diagnoses = load_json_file(diagnosis_file)
filtered_diagnosis_indices = {
    i for i, diag in enumerate(diagnoses)
    if diag.get('primary_diagnosis') and 'cholangiocarcinoma' in diag.get('primary_diagnosis').lower()
}

researchsubject_diagnosis = load_json_file(diagnosis_research_subject_file)
filtered_research_subject_aliases = {
    mapping['researchsubject_alias']
    for mapping in researchsubject_diagnosis
    if mapping.get('diagnosis_alias') in filtered_diagnosis_indices
}

subject_researchsubject_mappings = load_json_file(subject_researchsubject_file)
filtered_subject_aliases = {
    mapping['subject_alias']
    for mapping in subject_researchsubject_mappings
    if mapping.get('researchsubject_alias') in filtered_research_subject_aliases
}


mutation_mappings = load_json_file(subject_mutation_file)
filtered_mutation_mappings = [
    mapping for mapping in mutation_mappings
    if mapping.get('subject_alias') in filtered_subject_aliases
]

files_dir = RAW_DATA_PATH / 'files'
combined_files_data = combine_json_files(files_dir, 'files.json')

file_subjects_dir = RAW_DATA_PATH / 'file_subjects'
combined_file_subject = combine_json_files(file_subjects_dir, 'file_subject.json')

filtered_file_subjects = filter_list_by_key(combined_file_subject, 'subject_alias', filtered_subject_aliases)

file_aliases = {entry['file_alias'] for entry in filtered_file_subjects}
filtered_combined_files = [
    record for record in combined_files_data
    if record.get('integer_id_alias') in file_aliases
]

file_specimen = RAW_DATA_PATH / 'file_specimen.json'
file_specimen_mappings = load_json_file(file_specimen)

filtered_file_specimen = [
    record for record in file_specimen_mappings
    if record.get('file_alias') in file_aliases
]
output_reduced_file_specimen = RAW_DATA_PATH / 'reduced_file_specimen.json'
output_reduced_file_subject = RAW_DATA_PATH / 'reduced_file_subject.json'

save_json_file(filtered_file_specimen, output_reduced_file_specimen)
save_json_file(filtered_file_subjects, output_reduced_file_subject)

# ------------------

mutations = load_json_file(mutation_file)
mutation_aliases = {entry['mutation_alias'] for entry in filtered_mutation_mappings}
reduced_mutations = [
    record for record in mutations
    if record.get('integer_id_alias') in mutation_aliases
]
# ------------------
output_mutations_file = RAW_DATA_PATH / 'cholangiocarcinoma_mutations.json'
save_json_file(reduced_mutations, output_mutations_file)

output_files_file = RAW_DATA_PATH / 'cholangiocarcinoma_files.json'
save_json_file(filtered_combined_files, output_files_file)
