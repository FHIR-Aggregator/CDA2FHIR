import json
import csv
from pathlib import Path

subject_file = Path("../data/raw/subject.json")
file_subject_file = Path("../data/raw/association_tables/file_subject.tsv")
file_json_file = Path("../data/raw/file.json")

filtered_file_subject_file = Path("../data/raw/human_file_subject.tsv")
filtered_file_json_file = Path("../data/raw/human_file.json")
removed_file_ids_file = Path("../data/raw/removed_file_ids.json")


def extract_human_subject_ids(subject_file):
    human_subject_ids = set()
    with open(subject_file, 'r', encoding='utf-8') as f:
        subjects = json.load(f)
        for subject in subjects:
            if subject.get("species") in {"Human", "Homo sapiens"}:
                human_subject_ids.add(subject["id"])
    return human_subject_ids


def filter_file_subject(file_subject_file, human_subject_ids):
    valid_file_ids = set()
    with open(file_subject_file, 'r', encoding='utf-8') as infile, \
         open(filtered_file_subject_file, 'w', encoding='utf-8', newline='') as outfile:
        reader = csv.reader(infile, delimiter='\t')
        writer = csv.writer(outfile, delimiter='\t')
        
        for row in reader:
            file_id, subject_id = row
            if subject_id in human_subject_ids:
                valid_file_ids.add(file_id)
                writer.writerow(row)
    return valid_file_ids


def filter_file_json(file_json_file, valid_file_ids):
    removed_file_ids = []
    with open(file_json_file, 'r', encoding='utf-8') as infile, \
         open(filtered_file_json_file, 'w', encoding='utf-8') as outfile:
        files = json.load(infile)
        filtered_files = [file_record for file_record in files if file_record["id"] in valid_file_ids]
        removed_file_ids = [file_record["id"] for file_record in files if file_record["id"] not in valid_file_ids]
        json.dump(filtered_files, outfile, indent=2)
    return removed_file_ids


if __name__ == "__main__":
    print("Extracting human subject ids...")
    human_subject_ids = extract_human_subject_ids(subject_file)
    print(f"Found {len(human_subject_ids)} human subject ids.")

    print("Filtering file_subject.tsv...")
    human_file_ids = filter_file_subject(file_subject_file, human_subject_ids)
    print(f"Filtered file_subject.tsv to {len(human_file_ids)} from file subject relation table.")

    print("Filtering file.json...")
    removed_file_ids = filter_file_json(file_json_file, human_file_ids)
    print(f"Removed {len(removed_file_ids)} non-human files.")

    with open(removed_file_ids_file, 'w', encoding='utf-8') as f:
        json.dump(removed_file_ids, f, indent=2)
    print("Filtering completed.")

