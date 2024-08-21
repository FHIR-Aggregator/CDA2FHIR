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
To run the transformer, please make sure [CDA](https://cda.readthedocs.io/en/latest/) raw data is in ./data/raw/ directory.
Contact cancerdataaggregator @ gmail for retrieving the raw data.  

``` 
Usage: cda2fhir transform [OPTIONS]

Options:
  -s, --save               Save FHIR ndjson to CDA2FHIR/data/META folder.
                           [default: True]
  -v, --verbose
  -ns, --n_samples TEXT    Number of samples to randomly select - max 100.
  -nd, --n_diagnosis TEXT  Number of diagnosis to randomly select - max 100.
  -p, --path TEXT          Path to save the FHIR NDJSON files. default is
                           CDA2FHIR/data/META.
  --help                   Show this message and exit.
``` 

- example 
``` 
cda2fhir transform 
``` 

### Testing
Current integration testing runs on all data and may take up to ~2hrs. 

```
pytest -cov 
```
### FHIR data validation 
For FHIR data validation please run: 
```
g3t meta validate <path to data/META folder with ndjson files> 
```
This process may take up to 5+ minutes due to the size of current data.