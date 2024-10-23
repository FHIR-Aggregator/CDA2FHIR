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
  -s, --save               Save FHIR ndjson to CDA2FHIR/data/META folder.
                           [default: True]
  -v, --verbose
  -ns, --n_samples TEXT    Number of samples to randomly select - max 100.
  -nd, --n_diagnosis TEXT  Number of diagnosis to randomly select - max 100.
  -nf, --n_files TEXT      Number of files to randomly select - max 100.
  -f, --transform_files    Transform CDA files to FHIR DocumentReference and Group.
  -p, --path TEXT          Path to save the FHIR NDJSON files. default is
                           CDA2FHIR/data/META.
  --help                   Show this message and exit.
``` 

- example 
``` 
cda2fhir transform 
``` 

NOTE: in-case of interest in validating your FHIR data with GEN3, you will need to go through the [user-guide, setup, and documentation of GEN3 tracker](https://aced-idp.github.io/requirements/) before running the ```cda2fhir``` commands.

### FHIR data validation 

Before running the ```cda2fhir``` commands, you will need to walk through the [GEN3 user-guide, setup, and documentation](https://aced-idp.github.io/requirements/).
You may choose to remove/leave the _.g3t_ folder in this directory. The UUIDs of each entity will be co-dependent on the _project_id_ of the .g3t/config.yaml file. 

to validate generated data run: 
```
g3t meta validate <path to data/META folder with ndjson files> 
>>>> resources={'summary': {'Specimen': 715864, 'Observation': 724999, 'ResearchStudy': 423, 'BodyStructure': 180, 'Condition': 95288, 'ResearchSubject': 160662, 'Patient': 137522}}
```

This command will validate your FHIR entities and their reference relations to each other. It will also generate a summary count of all entities in each ndjson file. 

NOTE: This process may take _**5 minutes**_ or more, depending on your platform or compute power due to the size of the current data.


### Testing
Current integration testing runs on all data and may take approximately _**2 hours**_.

```
pytest -cov 
```