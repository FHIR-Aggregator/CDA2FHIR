import json


def read_json(path):
    """Read in json file"""
    try:
        with open(path, encoding='utf-8') as f:
            this_json = json.load(f)
            return this_json
    except json.JSONDecodeError as e:
        print("Error decoding JSON: {}".format(e))


subject = read_json("../data/raw/subject.json")

unique_values = {}

for record in subject:
    for key, value in record.items():
        if key not in unique_values:
            unique_values[key] = set()
        unique_values[key].add(value)

unique_values = {key: list(values) for key, values in unique_values.items()}

unique_values['species']
unique_values['sex']
unique_values['ethnicity']
unique_values['race']
unique_values['cause_of_death'] # loinc code https://loinc.org/79378-6/ 
unique_values['vital_status']


