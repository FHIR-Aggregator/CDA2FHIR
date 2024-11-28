import os
import pytest
from click.testing import CliRunner

from cda2fhir import cda2fhir
from pathlib import Path
import importlib.resources

from tests import run


@pytest.fixture
def runner() -> CliRunner:
    """Fixture for creating a CliRunner instance."""
    return CliRunner()


def test_valid(runner):
    """This should validate"""
    path = Path(importlib.resources.files('cda2fhir').parent / 'tests' / 'fixtures' / 'valid' / 'META')
    result = run(runner, ["validate", "-p", str(path)])
    assert result.exit_code == 0


def test_invalid(runner):
    """This should fail"""
    path = Path(importlib.resources.files('cda2fhir').parent / 'tests' / 'fixtures' / 'invalid-missing-patient' / 'META')
    result = run(runner, ["validate", "-p", str(path)], expected_exit_code=1)
    assert result.exit_code == 1

