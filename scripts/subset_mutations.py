import json
from collections import defaultdict

project_names = ['BEATAML1.0-COHORT', 'CDDP_EAGLE-1', 'CGCI-BLGSP', 'CGCI-HTMCP-CC',
       'CMI-MPC', 'CMI-ASC', 'CMI-MBC', 'CPTAC-3', 'CPTAC-2',
       'EXCEPTIONAL_RESPONDERS-ER', 'HCMI-CMDC', 'MMRF-COMMPASS',
       'TARGET-ALL-P2', 'TARGET-AML', 'TARGET-NBL', 'TARGET-OS',
       'TARGET-WT', 'TARGET-ALL-P3', 'TCGA-GBM', 'TCGA-OV', 'TCGA-LUAD',
       'TCGA-LUSC', 'TCGA-BLCA', 'TCGA-TGCT', 'TCGA-ESCA', 'TCGA-PAAD',
       'TCGA-CESC', 'TCGA-LIHC', 'TCGA-KIRP', 'TCGA-SARC', 'TCGA-BRCA',
       'TCGA-MESO', 'TCGA-STAD', 'TCGA-SKCM', 'TCGA-UCEC', 'TCGA-COAD',
       'TCGA-KIRC', 'TCGA-LAML', 'TCGA-READ', 'TCGA-HNSC', 'TCGA-THCA',
       'TCGA-PRAD', 'TCGA-LGG', 'TCGA-DLBC', 'TCGA-KICH', 'TCGA-UCS',
       'TCGA-ACC', 'TCGA-PCPG', 'TCGA-UVM', 'TCGA-THYM', 'TCGA-CHOL']

with open('data/raw/mutation.json', 'r') as f:
    data = json.load(f)

projects = defaultdict(list)
for record in data:
    project_name = record['project_short_name']
    if project_name in project_names:
        projects[project_name].append(record)

for project_name, records in projects.items():
    filepath = f"data/raw/project_mutations/{project_name.replace(' ', '_')}_mutations.json"
    with open(filepath, 'w') as f:
        json.dump(records, f, indent=4)


"""
# counts per project
for project in _mut_projects:
    print(project, df[df.project_short_name.isin([project])].shape)

BEATAML1.0-COHORT (6148, 32)
CDDP_EAGLE-1 (22753, 32)
CGCI-BLGSP (366, 32)
CGCI-HTMCP-CC (6029, 32)
CMI-MPC (3062, 32)
CMI-ASC (11326, 32)
CMI-MBC (16058, 32)
CPTAC-3 (414357, 32)
CPTAC-2 (103773, 32)
EXCEPTIONAL_RESPONDERS-ER (17297, 32)
HCMI-CMDC (179417, 32)
MMRF-COMMPASS (154588, 32)
TARGET-ALL-P2 (17651, 32)
TARGET-AML (2178, 32)
TARGET-NBL (5726, 32)
TARGET-OS (4243, 32)
TARGET-WT (421, 32)
TARGET-ALL-P3 (2224, 32)
TCGA-GBM (55177, 32)
TCGA-OV (39628, 32)
TCGA-LUAD (194729, 32)
TCGA-LUSC (172826, 32)
TCGA-BLCA (117053, 32)
TCGA-TGCT (2706, 32)
TCGA-ESCA (29760, 32)
TCGA-PAAD (24849, 32)
TCGA-CESC (68893, 32)
TCGA-LIHC (45440, 32)
TCGA-KIRP (19284, 32)
TCGA-SARC (17453, 32)
TCGA-BRCA (89568, 32)
TCGA-MESO (2709, 32)
TCGA-STAD (183107, 32)
TCGA-SKCM (353450, 32)
TCGA-UCEC (626945, 32)
TCGA-COAD (252773, 32)
TCGA-KIRC (25722, 32)
TCGA-LAML (3900, 32)
TCGA-READ (57699, 32)
TCGA-HNSC (87967, 32)
TCGA-THCA (5834, 32)
TCGA-PRAD (24779, 32)
TCGA-LGG (32780, 32)
TCGA-DLBC (6629, 32)
TCGA-KICH (2286, 32)
TCGA-UCS (8774, 32)
TCGA-ACC (8226, 32)
TCGA-PCPG (1946, 32)
TCGA-UVM (1490, 32)
TCGA-THYM (2396, 32)
TCGA-CHOL (3764, 32)
"""