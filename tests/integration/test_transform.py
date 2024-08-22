import os
import pytest
from cda2fhir import cda2fhir
from pathlib import Path
import importlib.resources


def test_transform():
    """testing the entire workflow - takes ~ 5min"""
    path = Path(importlib.resources.files('cda2fhir').parent / 'tests' / 'fixtures' / 'META')
    cda2fhir.cda2fhir(path=path, n_samples=2, n_diagnosis=2, save=True, verbose=False)

    files = ["Specimen.ndjson", "BodyStructure.ndjson", "Patient.ndjson", "ResearchStudy.ndjson",
             "ResearchSubject.ndjson", "Condition.ndjson", "Observation.ndjson"]

    for f in files:
        file_path = path / f
        assert os.path.exists(file_path), f"Expected file {f} does not exist."
