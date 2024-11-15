from setuptools import setup, find_packages

__version__ = '1.3.1'

setup(
    name='cda2fhir',
    version=__version__,
    description="Translating Cancer Data Commons (CDA) to FHIR (Fast Healthcare Interoperability Resources) format.",
    long_description=open('README.md').read(),
    url='https://github.com/FHIR-Aggregator/CDA2FHIR',
    author='https://ellrottlab.org/',
    packages=find_packages(),
    entry_points={
        'console_scripts': ['cda2fhir = cda2fhir.cli:cli']
    },
    install_requires=[
        'charset_normalizer',
        'idna',
        'certifi',
        'requests',
        'pydantic',
        'pytest',
        'click',
        'pathlib',
        'orjson',
        'tqdm',
        'uuid',
        'openpyxl',
        'pandas',
        'inflection',
        'iteration_utilities',
        'gen3-tracker>=0.0.7rc1',
        'fhir.resources>=7.1.0',  # FHIR® (Release R5, version 5.0.0)
        'sqlalchemy>=2.0.31'
    ],
    # package_data={'cda2fhir': []},
    tests_require=['pytest'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.12',
        'License :: OSI Approved :: MIT License',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: Bio-Informatics'
    ],
    platforms=['any'],
    python_requires='>=3.12, <4.0',
)
