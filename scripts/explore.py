import json


def read_json(path):
    """Read in json file"""
    try:
        with open(path, encoding='utf-8') as f:
            this_json = json.load(f)
            return this_json
    except json.JSONDecodeError as e:
        print("Error decoding JSON: {}".format(e))


def fetch_unique_values(entity):
    unique_values = {}

    for record in entity:
        for key, value in record.items():
            if key not in unique_values:
                unique_values[key] = set()
            unique_values[key].add(value)

    unique_values = {key: list(values) for key, values in unique_values.items()}
    return unique_values


subject = read_json("./data/raw/subject.json")
diagnosis = read_json("./data/raw/diagnosis.json")
researchsubject = read_json("./data/raw/researchsubject.json")


subject_unique_values = fetch_unique_values(subject)
diagnosis_unique_values = fetch_unique_values(diagnosis)
researchsubject_unique_values = fetch_unique_values(researchsubject)


subject_unique_values['species']
subject_unique_values['sex']
subject_unique_values['ethnicity']
subject_unique_values['race']
subject_unique_values['cause_of_death'] # loinc code https://loinc.org/79378-6/
subject_unique_values['vital_status']

diagnosis_unique_values['primary_diagnosis'] # requires harmonization -> snomed code, MONDO, ICD10

researchsubject_unique_values['primary_diagnosis_condition'] # requires harmonization -> snomed code
researchsubject_unique_values['primary_diagnosis_site'] # requires harmonization -> snomed code