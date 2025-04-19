import pandas as pd

df = pd.read_json("cholangiocarcinoma_mutations.json")
grouped = df.groupby(["project_short_name", "case_id", "hugo_symbol"]).size().reset_index(name="count")

# TCGA-CHOL study
df_tcga_chol = df[df['project_short_name'] == "TCGA-CHOL"]
mutation_counts = df_tcga_chol['hugo_symbol'].value_counts()
print(mutation_counts.head(20))

# mutation_counts.head(20).plot(kind='bar')
# plt.xlabel("Hugo Symbol (Gene)")
# plt.ylabel("Frequency")
# plt.title("Top 20 Most Frequent Mutations in TCGA-CHOL")
# plt.show()

# over all studies 
mutation_counts_all = df['hugo_symbol'].value_counts().reset_index()
mutation_counts_all.columns = ['hugo_symbol', 'frequency']
frequent_mutations_all = mutation_counts_all[mutation_counts_all.frequency > 1200]
",".join(frequent_mutations_all.hugo_symbol)
print(mutation_counts_all.head(20))


