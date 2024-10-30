import sys

from gen3_tracker.common import ERROR_COLOR, INFO_COLOR

from cda2fhir import cda2fhir
import click
import os
from pathlib import Path
import importlib.resources

@click.group()
def cli():
    """Cancer Data Aggregator (CDA) FHIR schema Key and Content Mapping"""
    pass


@cli.command('transform')
@click.option('-s', '--save', required=False, is_flag=True, default=True, show_default=True,
              help="Save FHIR ndjson to CDA2FHIR/data/META folder.")
@click.option('-v', '--verbose', required=False, is_flag=True, default=False, show_default=True)
@click.option('-ns', '--n_samples', required=False, help="Number of samples to randomly select - max 100.")
@click.option('-nd', '--n_diagnosis', required=False, help="Number of diagnosis to randomly select - max 100.")
@click.option("-p", "--path", default=None,
              help="Path to save the FHIR NDJSON files. default is CDA2FHIR/data/META.")
def transform(n_samples, n_diagnosis, save, verbose, path):
    assert os.path.exists(str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'raw'))), "Please make sure CDA data is in CDA2FHIR/data/raw directory before pursuing."

    if n_samples:
        assert int(n_samples) <= 100, "Please provide sample number less than 100"
    if n_diagnosis:
        assert int(n_diagnosis) <= 100, "Please provide diagnosis number less than 100"

    if save and path:
        if not os.path.isdir(path):
            raise ValueError(f"Path: '{path}' is not a valid directory.")

    cda2fhir.cda2fhir(path, n_samples, n_diagnosis, save, verbose)


@cli.command('validate')
@click.option("-d", "--debug", is_flag=True, default=False,
              help="Run in debug mode.")
@click.option("-p", "--path", default=None,
              help="Path to read the FHIR NDJSON files. default is CDA2FHIR/data/META.")
def validate(debug: bool, path):
    """Validate the output FHIR ndjson files."""
    from gen3_tracker.git import run_command

    if not path:
        path = str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'META'))
    if not os.path.isdir(path):
        raise ValueError(f"Path: '{path}' is not a valid directory.")

    try:
        from gen3_tracker.meta.validator import validate as validate_dir
        from halo import Halo
        with Halo(text='Validating', spinner='line', placement='right', color='white'):
            result = validate_dir(path)
        click.secho(result.resources, fg=INFO_COLOR, file=sys.stderr)
    except Exception as e:
        click.secho(str(e), fg=ERROR_COLOR, file=sys.stderr)
        if debug:
            raise


if __name__ == '__main__':
    cli()
