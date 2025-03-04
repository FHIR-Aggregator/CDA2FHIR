# # gunzip -c file.*.json.gz | jq -s '[.[][]]' > files.json # if files are separated

import json
import os
import sys
from collections import defaultdict

_dir = sys.argv[1] if len(sys.argv) > 1 else 'data/raw/'
file_path = os.path.join(_dir, 'human_file.json')
associated_project_path = os.path.join(_dir, 'file_associated_project.json')
out_dir = os.path.join(_dir, 'files_subset')
error_log = os.path.join(_dir, 'invalid_entries.json')

os.makedirs(out_dir, exist_ok=True)


def validate_json(data):
    """validate file and save invalid entries for CDA folks to review"""
    valid_entries = []
    invalid_entries = []

    for index, item in enumerate(data):
        if not isinstance(item, dict):
            print(f"Warning: file JSON entry {index} is not a valid JSON object.")
            invalid_entries.append(item)
            continue
        if 'integer_id_alias' not in item:
            print(f"Warning: file JSON entry {index} is missing 'integer_id_alias' field.")
            invalid_entries.append(item)
            continue
        valid_entries.append(item)
    return valid_entries, invalid_entries


try:
    with open(file_path, 'r') as f:
        file_data = json.load(f)
        valid_data, invalid_data = validate_json(file_data)

        if invalid_data:
            with open(error_log, 'w') as _file:
                json.dump(invalid_data, _file, indent=4)
            print(f"{len(invalid_data)} invalid entries found and saved to {error_log}.")

        if not valid_data:
            print("No valid entries found in the file.")
            sys.exit(1)

except json.JSONDecodeError as e:
    print(f"Error parsing JSON: {e}")
    sys.exit(1)

with open(associated_project_path, 'r') as f:
    project_data = json.load(f)

alias_to_project = {entry['file_alias']: entry['associated_project'] for entry in project_data}
project_groups = defaultdict(list)

for file_entry in valid_data:
    alias = file_entry.get('integer_id_alias')
    if alias:
        project = alias_to_project.get(alias)
        if project:
            project_groups[project].append(file_entry)

for project, entries in project_groups.items():
    output_path = os.path.join(out_dir, f'files_project_{project}.json')
    with open(output_path, 'w', encoding='utf-8') as project_file:
        json.dump(entries, project_file, indent=4)
    print(f"Saved {len(entries)} entries to {output_path}")

print(f"Files have been divided/saved by CDA projects in folder: {out_dir}.")
