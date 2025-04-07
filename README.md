## CDA2FHIR
![Status](https://img.shields.io/badge/Status-Build%20Passing-lgreen)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

<img src="./img/img.jpg" alt="img" width="400"/>

Translating Cancer Data Commons (CDA) to 🔥 FHIR (Fast Healthcare Interoperability Resources) format.


## Usage 
### Installation

- from source 
```commandline
# clone repo & setup virtual env
python3 -m venv venv
. venv/bin/activate
pip install -e .
```
### Transform to FHIR 

### Data 
To run the transformer, ensure that [CDA](https://cda.readthedocs.io/en/latest/) raw data is located in the ./data/raw/ directory. If you need to retrieve the raw data, please contact cancerdataaggregator @ gmail.

``` 
Usage: cda2fhir transform [OPTIONS]

Options:
  -s, --save                 Save FHIR ndjson to CDA2FHIR/data/META folder.
                             [default: True]
  -v, --verbose
  -ns, --n_samples TEXT      Number of samples to randomly select - max 100.
  -nd, --n_diagnosis TEXT    Number of diagnosis to randomly select - max 100.
  -nf, --n_files TEXT        Number of files to randomly select - max 100.
  -f, --transform_files      Transform CDA files to FHIR DocumentReference and
                             Group.
  -t, --transform_treatment  Transform CDA treatment to all sub-hierarchy of
                             FHIR MedicationAdministration ->
                             SubstanceDefinitionRepresentation.
  -c, --transform_condition  Transform CDA disease to Condition
  -m, --transform_mutation   Transform CDA mutation to Observation
  -p, --path TEXT            Path to save the FHIR NDJSON files. default is
                             CDA2FHIR/data/META.
  --help                     Show this message and exit.
``` 

- example 
``` 
cda2fhir transform 
``` 

### FHIR data validation 

#### Run validate
```
 cda2fhir validate --path data/META
{'summary': {'Specimen': 742505, 'Medication': 214, 'Observation': 832864, 'ResearchStudy': 429, 'SubstanceDefinition': 214, 'BodyStructure': 135, 'Condition': 114804, 'ResearchSubject': 184888, 'MedicationAdministration': 38267, 'Patient': 159047, 'Substance': 214}}
```

This command will validate your FHIR entities and their reference relations to each other. It will also generate a summary count of all entities in each ndjson file. 

NOTE: This process may take _**5 minutes**_ or more, depending on your platform or compute power due to the size of the current data.

#### Check for a field ex. extension
```bash
awk '!/extension/ {exit 1}' data/META/ResearchSubject.ndjson && echo "Every line contains 'extension'" || echo "Not every line contains 'extension'"
```


### Testing
Current integration testing runs on all data and may take approximately _**2 hours**_.

```
pytest -cov 
```