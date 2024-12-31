import json
import os
import sys

# gunzip -c file.*.json.gz | jq -s '[.[][]]' > files.json # if files are separated

_dir = sys.argv[1] if len(sys.argv) > 1 else 'data/raw/'

file_path = os.path.join(_dir, 'file.json')
associated_project_path = os.path.join(_dir, 'file_associated_project.json')
out_dir = os.path.join(_dir, 'files_subset') # data/raw/file_subset needs to exist
error_log = os.path.join(_dir, 'invalid_entries.json')

os.makedirs(out_dir, exist_ok=True)
def validate_json(data):
    """validate file and save invalid entries for CDA folks to review"""
    valid_entries = []
    invalid_entries = []

    for index, item in enumerate(data):
        if not isinstance(item, dict):
            print(f"warning: file json entry {index} is not a valid json object.")
            invalid_entries.append(item)
            continue
        if 'integer_id_alias' not in item:
            print(f"warning: file json entry {index} is missing 'integer_id_alias' field.")
            invalid_entries.append(item)
            continue
        valid_entries.append(item)
    return valid_entries, invalid_entries

# def project2file(project, file_entry):
#     """for a CDA project write json file"""
#     filename = os.path.join(out_dir, f'files_project_{project}.json')
#     with open(filename, 'a') as project_file:
#         json.dump(file_entry, project_file)
#         project_file.write("\n")


def project2file(project, file_entry, out_dir):
    """for a CDA project, write a JSON file with entries formatted as a JSON array."""
    filename = os.path.join(out_dir, f'files_project_{project}.json')
    _array = []

    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as project_file:
            try:
                _array = json.load(project_file)
            except json.JSONDecodeError as e:
                print(f"Error reading existing JSON data from {filename}: {e}")

    _array.append(file_entry)

    with open(filename, 'w', encoding='utf-8') as project_file:
        json.dump(_array, project_file, indent=4)
    print(f"Entry added and file updated: {filename}")


try:
    with open(file_path, 'r') as f:
        file_data = json.load(f)
        valid_data, invalid_data = validate_json(file_data)

        if invalid_data:
            with open(error_log, 'w') as _file:
                json.dump(invalid_data, _file, indent=4)
            print(f"{len(invalid_data)} invalid entries found and saved to {error_log}.")

        if not valid_data:
            print("there are no valid entries found in the file.")
            sys.exit(1)

except json.JSONDecodeError as e:
    print(f"error parsing JSON: {e}")
    sys.exit(1)

with open(associated_project_path, 'r') as f:
    project_data = json.load(f)

alias_to_project = {entry['file_alias']: entry['associated_project'] for entry in project_data}

for file_entry in valid_data:
    alias = file_entry.get('integer_id_alias')
    if alias is not None:
        project = alias_to_project.get(alias)
        if project:
            project2file(project, file_entry, out_dir)

print(f'files have been divided/saved by CDA projects in folder: {out_dir}.')
