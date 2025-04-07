import json
import glob
import os

def _convert(input_file, output_file):
    """converts a file with dict objects on each line to a comma-separated json array."""
    with open(input_file, 'r', encoding='utf-8') as infile:
        lines = infile.readlines()
    _array = []

    for line in lines:
        line = line.strip()
        if line:
            try:
                json_obj = json.loads(line)
                _array.append(json_obj)
            except json.JSONDecodeError as e:
                print(f"error decoding JSON on this line: {line}\n error: {e}")

    with open(output_file, 'w', encoding='utf-8') as outfile:
        json.dump(_array, outfile, indent=4)
    print(f"conversion completed - file saved to: {output_file}")

def process_files(input_folder, output_suffix="_converted"):
    """Glob all json files and convert them to list of dict with a new file-name naming convention tag."""
    file_paths = glob.glob(os.path.join(input_folder, '*.json'))
    print(f"Globbed: {file_paths}")
    for file_path in file_paths:
        output_file = file_path.replace('.json', f'{output_suffix}.json')
        _convert(file_path, output_file)

# TODO: cli
_folder = 'data/raw/file_subset'
process_files(_folder)
