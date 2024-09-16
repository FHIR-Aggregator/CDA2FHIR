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
