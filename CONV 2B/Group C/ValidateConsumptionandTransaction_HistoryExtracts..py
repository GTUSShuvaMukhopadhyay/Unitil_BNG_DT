import pandas as pd

# Load CSVs
file_path1 = r"C:\Users\US82783\Downloads\725_STAGE_CONSUMPTION_HIST.csv"
file_path2 = r"C:\Users\US82783\Downloads\725_STAGE_TRANSACTIONAL_HIST (1).csv"

# Read files with dtype=str and usecols if you want to limit memory
df1 = pd.read_csv(file_path1, dtype=str)
df2 = pd.read_csv(file_path2, dtype=str)

# Filter df2 for TRANSACTIONTYPE == '2'
#df2_filtered = df2[df2.iloc[:, 7].str.strip() == '2'].copy()
print(f"\n✅ Filtered with Transaction Type 2: {len(df2):,}")

# Define column positions
df1_cols = df1.iloc[:, [0, 1, 20]].copy()
df2_cols = df2.iloc[:, [1, 2, 6]].copy()

# Vectorized cleanup: strip + remove trailing '.0'
def clean(df):
    return df.apply(lambda col: col.str.strip().str.replace(r'\.0$', '', regex=True))

df1_clean = clean(df1_cols)
df2_clean = clean(df2_cols)

# Create keys (faster using agg and sep)
df1_keys = df1_clean.astype(str).agg('|'.join, axis=1)
df2_keys = df2_clean.astype(str).agg('|'.join, axis=1)

# Use Series.isin() for fast matching
matched = df1_keys.isin(set(df2_keys))
match_count = matched.sum()

print(f"\n✅ Matching unique records: {match_count:,}")

matching_keys = df1_keys[matched].drop_duplicates().head(100)
print("\n🔑 Sample Distinct Matching Keys:")
print(matching_keys.to_string(index=False))

matched_df = df1[matched].copy()
matched_df.to_csv("matched_records.csv", index=False)