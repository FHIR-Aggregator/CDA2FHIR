import os
import sqlite3
import pandas as pd
import orjson
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# for analysis
import numpy as np
from scipy.stats import spearmanr
from lifelines import KaplanMeierFitter
import matplotlib.pyplot as plt

# prior cmds ------------------------------------------------
# ! pip install fhir-aggregator-client --no-cache-dir --quiet
# ! pip install lifelines --no-cache-dir --quiet
# ! pip freeze | grep fhir_aggregator_client
# rm <root>/.fhir-aggregator/fhir-graph.sqlite
#
# export FHIR_BASE=https://google-fhir.test-fhir-aggregator.org/
#
# fq run file2patients '/ResearchStudy?_id=32885d16-7445-57ce-9b10-4445ac93184f'
# ------------------------------------------------------------
# other:
# query test:  https://google-fhir.test-fhir-aggregator.org/DocumentReference?part-of-study=ResearchStudy/32885d16-7445-57ce-9b10-4445ac93184f&category:code=Transcriptome%20Profiling&category:code=STAR%20-%20Counts&category:code=RNA-Seq&type:code=TSV&category:code=open
# graph definition: https://github.com/FHIR-Aggregator/fhir-aggregator-client/pull/34/files#diff-833bc6caa613ad6e8db8864ab73e80a015839474de4d12e0719b4dbff6288a81
# GDC cohort: https://portal.gdc.cancer.gov/projects/TCGA-LUAD

def load_resources_from_db(db_path):
    # load resources from db
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query("select * from resources", conn)
    finally:
        conn.close()
    return df


def flatten_json(y, parent_key='', sep='_'):
    # flatten nested json
    items = {}
    if isinstance(y, dict):
        for k, v in y.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            items.update(flatten_json(v, new_key, sep=sep))
    elif isinstance(y, list):
        # join simple list items ex. [1,2,3]
        if all(not isinstance(i, (dict, list)) for i in y):
            items[parent_key] = ", ".join(map(str, y))
        else:
            for i, v in enumerate(y):
                new_key = f"{parent_key}{sep}{i}"
                items.update(flatten_json(v, new_key, sep=sep))
    else:
        items[parent_key] = y
    return items


def flatten_resource(resource):
    # if resource is a string, parse and flatten it
    if isinstance(resource, str):
        try:
            resource = orjson.loads(resource)
        except Exception:
            return {}
    return flatten_json(resource)


def build_resource_dataframes(df_raw):
    # group by resource_type and flatten each resource
    resource_dfs = {}
    for rtype, group in df_raw.groupby("resource_type"):
        flattened = group["resource"].apply(flatten_resource)
        df_flat = pd.DataFrame(flattened.tolist())
        df_flat = df_flat.dropna(axis=1, how='all')
        resource_dfs[rtype] = df_flat
    return resource_dfs


def load_patient_demographics(tsv_file):
    # load patient demographics from tsv; drop duplicate patient_id rows
    df = pd.read_csv(tsv_file, sep='\t')
    columns = [
        'patient_identifier', 'patient_case_id', 'patient_deceasedBoolean', 'patient_gender',
        'patient_id', 'patient_us_core_birthsex', 'patient_us_core_race',
        'patient_us_core_ethnicity', 'patient_patient_extensions_patient_age',
        'patient_part_of_study',
        'patient_observation_days_between_birth_and_diagnosis',
        'patient_observation_days_between_diagnosis_and_death',
        'patient_observation_smoking_history_pack_years',
        'patient_observation_observation_code',
        'patient_observation_year_of_death',
        'patient_observation_year_of_birth',
        'patient_observation_history_of_alcohol_use',
        'patient_observation_how_many_cigarettes_do_you_smoke_per_day_now',
        'patient_observation_number_of_days_between_index_date_and_last_follow_up',
        'patient_observation_number_of_days_between_index_date_and_diagnosis'
    ]
    return df[columns].drop_duplicates(subset=['patient_id'])


def merge_document_reference_and_demographics(resource_dfs, df_patient_demographics):
    # merge documentreference with demographics using patient_id from subject_reference
    df_dr = resource_dfs.get("DocumentReference", pd.DataFrame()).copy()
    if not df_dr.empty and "subject_reference" in df_dr.columns:
        df_dr['patient_id'] = df_dr['subject_reference'].str.split('/').str[1]
    return df_dr.merge(df_patient_demographics, on='patient_id', how='left')


def setup_requests_session():
    # setup requests session with retry logic
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def process_gene_file(row, session, download_folder, download_missing=True, gene_subset=None):
    # download (or load) a gene file and process it
    file_url = row["content_0_attachment_url"]
    file_name = row["content_0_attachment_title"]
    patient_id = row["patient_identifier"]  # ex. "TCGA-44-7662"
    local_path = os.path.join(download_folder, file_name)

    if not os.path.exists(local_path):
        if download_missing:
            print(f"downloading {file_name} for patient {patient_id}...")
            try:
                response = session.get(file_url, timeout=15)
                response.raise_for_status()
                with open(local_path, "wb") as f:
                    f.write(response.content)
            except Exception as e:
                print(f"error downloading {file_name}: {e}")
                return None
        else:
            return None
    else:
        if download_missing:
            print(f"{file_name} exists. skipping download.")

    try:
        temp_df = pd.read_csv(local_path, sep="\t", comment="#", header=0)
    except Exception as e:
        print(f"error reading {local_path}: {e}")
        return None

    if "gene_id" in temp_df.columns:
        mask = temp_df["gene_id"].str.startswith("n_")
        temp_df = temp_df[~mask]
    else:
        print(f"'gene_id' not found in {file_name}. skipping.")
        return None

    # filter for gene_subset if provided
    if gene_subset is not None:
        temp_df = temp_df[temp_df["gene_name"].isin(gene_subset)]

    keep_cols = ["gene_name", "tpm_unstranded"]
    if not all(col in temp_df.columns for col in keep_cols):
        print(f"{file_name} missing {keep_cols}. skipping.")
        return None

    temp_df = temp_df[keep_cols]
    temp_df.rename(columns={"tpm_unstranded": patient_id}, inplace=True)
    return temp_df


def build_gene_tpm_matrix(df_files, download_folder, download_missing=True, gene_subset=None):
    # process gene files and merge to build tpm matrix
    session = setup_requests_session()
    dfs = []
    for _, row in df_files.iterrows():
        temp_df = process_gene_file(row, session, download_folder, download_missing=download_missing,
                                    gene_subset=gene_subset)
        if temp_df is not None and not temp_df.empty:
            dfs.append(temp_df)
    master_df = None
    for df_temp in dfs:
        if master_df is None:
            master_df = df_temp
        else:
            master_df = pd.merge(master_df, df_temp, on="gene_name", how="outer")
    if master_df is not None and not master_df.empty:
        master_df.set_index("gene_name", inplace=True)
        print("\nfinal gene x patient tpm matrix (preview):")
        print(master_df.head())
        output_file = "gene_by_tpm_matrix.tsv"
        master_df.to_csv(output_file, sep="\t")
        print(f"saved matrix to {output_file}.")
        return master_df
    else:
        print("no data to build matrix.")
        return None


# workflow wrappers for jupyter/ipython ------------------------------------------------

def run_full_workflow(db_path, tsv_file, download_folder,
                      export_patient_matrix=True, download_missing=False,
                      gene_subset=None, sample_patients=None):
    # run full workflow: load db & tsv, merge, optionally sample patients,
    # export patient matrix, build expression matrix using gene_subset if provided
    print("running full workflow ...")
    df_raw = load_resources_from_db(db_path)
    resource_dfs = build_resource_dataframes(df_raw)
    df_patients = load_patient_demographics(tsv_file)
    df_files = merge_document_reference_and_demographics(resource_dfs, df_patients)

    if sample_patients is not None:
        df_files = df_files.sample(n=sample_patients)

    if export_patient_matrix:
        patient_file = "patient_matrix.tsv"
        df_files.to_csv(patient_file, sep="\t", index=False)
        print(f"saved patient matrix to {patient_file}.")

    expr_matrix = build_gene_tpm_matrix(df_files, download_folder,
                                        download_missing=download_missing,
                                        gene_subset=gene_subset)
    return df_files, expr_matrix


def run_matrix_only_workflow(mapping_csv, download_folder,
                             download_missing=False, export_patient_matrix=False,
                             gene_subset=None):
    # run matrix-only workflow: load mapping csv and build tpm matrix using gene_subset if provided
    print("running matrix-only workflow ...")
    if not os.path.exists(mapping_csv):
        print(f"mapping csv '{mapping_csv}' not found. exiting.")
        return None, None
    df_files = pd.read_csv(mapping_csv, sep='\t')

    if export_patient_matrix:
        patient_file = "patient_matrix.tsv"
        df_files.to_csv(patient_file, sep="\t", index=False)
        print(f"saved patient matrix to {patient_file}.")

    expr_matrix = build_gene_tpm_matrix(df_files, download_folder,
                                        download_missing=download_missing,
                                        gene_subset=gene_subset)
    return df_files, expr_matrix


# -- run full workflow with gene subsetting and patient sampling --
# parameters
db_path = "<root>/.fhir-aggregator/fhir-graph.sqlite"  # adjust if needed
tsv_file = "fhir-graph.tsv"  # adjust if needed
download_folder = "gdc_downloads"

# define list of gene names (this list is our focus)
gene_names = [
    "BAP1", "PBRM1", "TP53", "GPR98", "TTN", "PCF11", "IDH1", "MUC5B", "MUC16", "DNAH5",
    "MPDZ", "ARID1A", "KMT2C", "EPHA2", "ITPR2", "BCOR", "APOB", "BIRC6", "XKR4", "LRP1",
    "FGFR2", "ALB", "MAGEA1", "COL6A3", "FMN2", "ADAM30", "AHNAK", "LRP6", "MYOM1", "ATP13A3",
    "NUP98", "CHD7", "RABGAP1", "KIF1B", "PLXNA4", "ARID1B", "KIF19", "PKHD1", "IDH2", "KRAS",
    "SMG1", "MLLT4", "FAM184A", "CDKN2A", "IMPG2", "ZHX3", "NEB", "GBF1", "ARFGAP2",
    "PIK3CA", "ELF3", "RALGAPB", "COL7A1", "MME", "ROR1", "FBN2", "NYAP2", "KMT2D", "HMGCL",
    "LAMA3", "THOC5", "SUPT20H", "CXorf57", "EP400", "SDK2", "PHKA1", "KEAP1", "DNAH8",
    "CGGBP1", "OBSCN", "SELL", "CALCR", "ZNF598", "CHAMP1", "CTNNA2", "TPRN", "ABCA13",
    "GPS2", "USP9X", "PCLO", "CCNB3", "ZNF609", "ATXN7L3B", "NID1", "MLXIPL", "WDFY4",
    "VNN1", "SPHK2", "DAPK1", "PAIP2", "STAT1", "C12orf55", "EIF4G1", "LEO1", "BCL3",
    "ZNF136", "DDB2", "EDEM2", "KIAA0754", "AIM1", "GALNT4", "EIF3H", "TYK2", "GLG1",
    "PSG5", "DNAH6", "UBR5", "UHMK1", "PHF20L1", "MARCH6", "DOCK10", "ODF2", "TMC4",
    "PUM1", "PCDHGA4", "DICER1", "LRP1B", "PKHD1L1"
]

# run full workflow with:
# - download_missing false (skip downloading)
# - gene_subset = gene_names (process only genes from  )
# - sample_patients = 10 (only 10 random patients)
patient_matrix, expression_matrix = run_full_workflow(
    db_path, tsv_file, download_folder,
    export_patient_matrix=True,
    download_missing=False,
    gene_subset=gene_names,
    sample_patients=10
)

# Phenotype to Genotype example analyses (this is for demo - normalization wasn't taken into account)
# -- spearman correlation analysis --------------------------------------------------

# map patient id -> age (using column "patient_patient_extensions_patient_age")
valid_patients = patient_matrix[patient_matrix["patient_identifier"].isin(expression_matrix.columns)]

age_mapping = valid_patients.set_index("patient_identifier")["patient_patient_extensions_patient_age"].apply(
    lambda x: float(x)).to_dict()

common_patients = [p for p in expression_matrix.columns if p in age_mapping]
ages = np.array([age_mapping[p] for p in common_patients])

correlations = []
for gene in expression_matrix.index:
    expr_values = expression_matrix.loc[gene, common_patients].values.astype(float)
    corr, pval = spearmanr(expr_values, ages)
    correlations.append({"gene": gene, "spearman_corr": corr, "pval": pval})

corr_df = pd.DataFrame(correlations)
print("\nspearman correlation results (sorted by pval):")
print(corr_df.sort_values("pval").head())



# -- kaplan meier survival analysis with conversion fix --

valid_patients = patient_matrix.dropna(subset=[
    "patient_observation_days_between_diagnosis_and_death",
    "patient_deceasedBoolean",
    "patient_patient_extensions_patient_age"
])

def parse_time(val):
    try:
        # time is given as "number unit" - "1357.0 days"
        return float(str(val).split()[0])
    except Exception as e:
        return np.nan

valid_patients["time"] = valid_patients["patient_observation_days_between_diagnosis_and_death"].apply(parse_time)
valid_patients["event"] = valid_patients["patient_deceasedBoolean"].apply(lambda x: 1 if str(x).strip().lower() == "true" else 0)
def parse_age(val):
    try:
        return float(str(val).split()[0])
    except Exception as e:
        return np.nan

valid_patients["age"] = valid_patients["patient_patient_extensions_patient_age"].apply(parse_age)

valid_patients = valid_patients.dropna(subset=["time", "age"])

median_age = valid_patients["age"].median()
valid_patients["age_group"] = valid_patients["age"].apply(lambda x: "high" if x >= median_age else "low")

plt.figure(figsize=(8,6))
kmf = KaplanMeierFitter()

for group, df_group in valid_patients.groupby("age_group"):
    kmf.fit(df_group["time"], event_observed=df_group["event"], label=group)
    kmf.plot_survival_function()

plt.title("kaplan-meier curve by age group (median age of the patient sample)")
plt.xlabel("days")
plt.ylabel("survival probability")
plt.show()
