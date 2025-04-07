import json
from pathlib import Path
import importlib.resources

# Was able to get to Cholangiocarcinoma mutations with id strings vs. json relations.

RAW_DATA_PATH = Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw_022025')

diagnosis_file = RAW_DATA_PATH / 'diagnosis.json'
diagnosis_research_subject_file = RAW_DATA_PATH / 'researchsubject_diagnosis.json'
subject_researchsubject_file = RAW_DATA_PATH / 'subject_researchsubject.json'
subject_mutation_file = RAW_DATA_PATH / 'subject_mutation.json'
mutation_file = RAW_DATA_PATH / 'mutation.json'
subject_identifier_file = RAW_DATA_PATH / "subject_identifier.json"
subject_cholangiocarcinoma_mutation_file = RAW_DATA_PATH / "subject_cholangiocarcinoma_mutation2.json"

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
subj_identifier = load_json_file(subject_identifier_file)

filtered_diagnosis_aliases = {
    diag.get('integer_id_alias')
    for diag in diagnoses
    if diag.get('primary_diagnosis') and 'cholangiocarcinoma' in diag.get('primary_diagnosis').lower()
}

filtered_diagnosis_objects = [
    diag
    for diag in diagnoses
    if diag.get('primary_diagnosis') and 'cholangiocarcinoma' in diag.get('primary_diagnosis').lower()
]

subject_submitter_ids = [
    diag['id'].split('.')[1]
    for diag in diagnoses
    if 'id' in diag and len(diag['id'].split('.')) >= 2
]

filtered_subject_identifiers = [
    mapping
    for mapping in subj_identifier
    if mapping.get('value') in subject_submitter_ids
]

subject_values = {mapping["value"] for mapping in filtered_subject_identifiers if "value" in mapping}

mutations = load_json_file(mutation_file)
filtered_mutations = [
    mutation for mutation in mutations
    if mutation.get("case_barcode") in subject_values
]

output_mutations_file = RAW_DATA_PATH / 'cholangiocarcinoma_mutations2.json'
save_json_file(filtered_mutations, output_mutations_file)


subject_value_to_alias = {
    mapping['value']: mapping['subject_alias']
    for mapping in filtered_subject_identifiers
    if 'value' in mapping and 'subject_alias' in mapping
}

subject_alias_to_mutation_aliases = {}

for mutation in filtered_mutations:
    case_barcode = mutation.get("case_barcode")
    if case_barcode in subject_value_to_alias:
        subject_alias = subject_value_to_alias[case_barcode]
        mutation_alias = mutation.get("integer_id_alias")
        if subject_alias not in subject_alias_to_mutation_aliases:
            subject_alias_to_mutation_aliases[subject_alias] = []
        subject_alias_to_mutation_aliases[subject_alias].append(mutation_alias)

for subject_alias, mutation_aliases in subject_alias_to_mutation_aliases.items():
    print(f"Subject alias {subject_alias} -> Mutation alias(es): {mutation_aliases}")


relations = []
for subject_alias, mutation_aliases in subject_alias_to_mutation_aliases.items():
    for mutation_alias in mutation_aliases:
        relations.append({
            "subject_alias": subject_alias,
            "mutation_alias": mutation_alias
        })

save_json_file(relations, subject_cholangiocarcinoma_mutation_file)
