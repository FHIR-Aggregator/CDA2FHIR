from cda2fhir import cda2fhir
import click
import os


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
    if n_samples:
        assert int(n_samples) <= 100, "Please provide sample number less than 100"
    if n_diagnosis:
        assert int(n_diagnosis) <= 100, "Please provide diagnosis number less than 100"

    if save and path:
        if not os.path.isdir(path):
            raise ValueError(f"Path: '{path}' is not a valid directory.")

    cda2fhir.cda2fhir(path, n_samples, n_diagnosis, save, verbose)


if __name__ == '__main__':
    cli()
