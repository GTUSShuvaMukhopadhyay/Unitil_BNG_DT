# CONV2_TENDERTYPE_STAGE_TRANSACTIONAL_HIST_05232025_1327PM.py
import pandas as pd
import os
import csv
from datetime import datetime

# CSV Staging File Checklist
CHECKLIST = [
    "✅ Filename must match the entry in Column D of the All Tables tab.",
    "✅ Filename must be in uppercase except for '.csv' extension.",
    "✅ The first record in the file must be the header row.",
    "✅ Ensure no extraneous rows (including blank rows) are present in the file.",
    "✅ All non-numeric fields must be enclosed in double quotes.",
    "✅ The last row in the file must be 'TRAILER' followed by commas.",
    "✅ Replace all CRLF (X'0d0a') in customer notes with ~^[",
    "✅ Ensure all dates are in 'YYYY-MM-DD' format.",
]

def print_checklist():
    print("CSV Staging File Validation Checklist:")
    for item in CHECKLIST:
        print(item)

print_checklist()

# Define file paths
file_paths = {
    "DFKKZP": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_TRANSACTIONAL_HIST\dfkkzp 05092025.XLSX",   
    "EVER": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_TRANSACTIONAL_HIST\EVER.XLSX",
    "ZDM_PREMDETAILS": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_TRANSACTIONAL_HIST\ZDM_PREMDETAILS.XLSX",
}
 
# Initialize data_sources dictionary to hold our data
data_sources = {}
 
# Function to read an Excel file
def read_excel_file(name, path):
    try:
        df = pd.read_excel(path, sheet_name="Sheet1", engine="openpyxl")
        print(f"Successfully loaded {name}: {df.shape[0]} rows, {df.shape[1]} columns")
        return df
    except Exception as e:
        print(f"Error loading {name}: {e}")
        return None

# Load data sources
print("\nLoading data sources...")
for name, path in file_paths.items():
    data_sources[name] = read_excel_file(name, path)

# Verify all data sources loaded successfully
failed_sources = [name for name, df in data_sources.items() if df is None]
if failed_sources:
    print(f"Error: Failed to load data sources: {', '.join(failed_sources)}")
    exit(1)

# --------------------------
# Start with DFKKZP as base
# --------------------------
print("\nStarting transformation with DFKKZP as base...")
df_new = data_sources["DFKKZP"].copy()
print(f"Base DFKKZP records: {len(df_new)}")

# Print column names to verify
print("\nDFKKZP columns:", df_new.columns.tolist())

# --------------------------
# Map DT values to TENDERTYPE and TRANSACTIONDESCRIPTION
# --------------------------
print("\nMapping DT values to TENDERTYPE and TRANSACTIONDESCRIPTION...")

# Define mappings
dt_to_tendertype = {
    "CA": 70,
    "CK": 71,
    "CR": 81,
    "WD": 81,
    "UB": 81,
    "IB": 79,
    "UK": 80,
    "IK": 77,
    "CP": 7
}

dt_to_description = {
    "CA": "CASH BNG",
    "CK": "CHECK BNG",
    "CR": "WEB CC BNG",
    "WD": "EFT PYMT BNG",
    "UB": "WEB CC BNG",
    "IB": "IVR CC BNG",
    "UK": "WEB ACH BNG",
    "IK": "IVR ACH BNG",
    "CP": "ACH-ALL"
}

# Apply mappings with special case for R column
df_new["TENDERTYPE"] = df_new.apply(
    lambda row: 94 if row["R"] in [1, 2] else dt_to_tendertype.get(row["DT"], ""),
    axis=1
)

df_new["TRANSACTIONDESCRIPTION"] = df_new.apply(
    lambda row: "Customer Returned Payment" if row["R"] in [1, 2] else dt_to_description.get(row["DT"], ""),
    axis=1
)

# --------------------------
# Extract CUSTOMERID and LOCATIONID through joins
# --------------------------
print("\nPerforming multi-step joins for CUSTOMERID and LOCATIONID...")

# Step 1: Prepare join keys - handle data type conversions
print("\nPreparing join keys...")
# Convert to string and remove decimals
df_new["Selection Value 1"] = df_new["Selection Value 1"].astype(str).str.strip()
data_sources["EVER"]["Cont.Account"] = data_sources["EVER"]["Cont.Account"].astype(str).str.strip()

# For ZDM, remove the .0 from float values
data_sources["ZDM_PREMDETAILS"]["Contract Account"] = data_sources["ZDM_PREMDETAILS"]["Contract Account"].apply(
    lambda x: str(int(float(x))) if pd.notna(x) else ""
)

# Debug: Show sample values
print("\nSample DFKKZP Selection Value 1:", df_new["Selection Value 1"].head(5).tolist())
print("Sample EVER Cont.Account:", data_sources["EVER"]["Cont.Account"].head(5).tolist())
print("Sample ZDM Contract Account:", data_sources["ZDM_PREMDETAILS"]["Contract Account"].head(5).tolist())

# Step 2: First join - DFKKZP to EVER
print("\nJoining DFKKZP to EVER on Selection Value 1 -> Cont.Account...")
df_merged1 = pd.merge(
    df_new,
    data_sources["EVER"][["Cont.Account"]].drop_duplicates(),
    left_on="Selection Value 1",
    right_on="Cont.Account",
    how="left"
)
matches_ever = df_merged1["Cont.Account"].notna().sum()
print(f"Matched {matches_ever} out of {len(df_merged1)} records with EVER")

# Debug: Show matched Cont.Account values
print("\nSample matched Cont.Account values from EVER:")
matched_samples = df_merged1[df_merged1["Cont.Account"].notna()]["Cont.Account"].head(10).tolist()
print(matched_samples)

# Step 3: Second join - Result to ZDM_PREMDETAILS
print("\nJoining result to ZDM_PREMDETAILS on Cont.Account -> Contract Account...")

# Try to understand why join is failing
ever_ca_values = set(df_merged1[df_merged1["Cont.Account"].notna()]["Cont.Account"].unique())
zdm_ca_values = set(data_sources["ZDM_PREMDETAILS"]["Contract Account"].unique())

print(f"\nUnique Cont.Account values from EVER: {len(ever_ca_values)}")
print(f"Unique Contract Account values from ZDM: {len(zdm_ca_values)}")

# Check if there's any overlap
overlap = ever_ca_values.intersection(zdm_ca_values)
print(f"Overlapping values: {len(overlap)}")

if len(overlap) == 0:
    print("\nNo overlap found after conversion. Let's proceed anyway...")
    
# Always do the join regardless of overlap
df_merged2 = pd.merge(
    df_merged1,
    data_sources["ZDM_PREMDETAILS"][["Contract Account", "Business Partener", "Premise"]].drop_duplicates(),
    left_on="Cont.Account",
    right_on="Contract Account",
    how="left"
)

matches_zdm = df_merged2["Contract Account"].notna().sum()
print(f"Matched {matches_zdm} out of {len(df_merged2)} records with ZDM_PREMDETAILS")

# Step 4: Extract final values
df_new = df_merged2.copy()

# Remove decimals from CUSTOMERID and LOCATIONID
df_new["CUSTOMERID"] = df_new["Business Partener"].apply(
    lambda x: str(int(float(x))) if pd.notna(x) and str(x) != "" else ""
)
df_new["LOCATIONID"] = df_new["Premise"].apply(
    lambda x: str(int(float(x))) if pd.notna(x) and str(x) != "" else ""
)

print(f"\nCUSTOMERID populated: {(df_new['CUSTOMERID'] != '').sum()} records")
print(f"LOCATIONID populated: {(df_new['LOCATIONID'] != '').sum()} records")

# --------------------------
# Set other fields
# --------------------------
print("\nSetting remaining fields...")

# Fixed values
df_new["TAXYEAR"] = ""
df_new["TRANSACTIONTYPE"] = 1
df_new["APPLICATION"] = "5"
df_new["BILLTYPE"] = ""
df_new["UPDATEDATE"] = ""

# Empty fields
df_new["BILLINGDATE"] = ""
df_new["DUEDATE"] = ""
df_new["BILLORINVOICENUMBER"] = ""

# Date field
df_new["TRANSACTIONDATE"] = pd.to_datetime(
    df_new["Post. Date"],
    errors='coerce'
).dt.strftime('%Y-%m-%d')

# Transaction amount with negative handling
df_new["TRANSACTIONAMOUNT"] = df_new.apply(
    lambda row: str(row["Payment amount"]) if row["TENDERTYPE"] == 94 
                else "-" + str(row["Payment amount"]),
    axis=1
)

# --------------------------
# Format values with proper quoting
# --------------------------
print("\nFormatting field values...")
def custom_quote(val):
    if pd.isna(val) or val in ["", " "]:
        return ""
    return f'"{val}"'
    
def selective_custom_quote(val, column_name):
    # Numeric fields that should not be quoted
    numeric_columns = ['TAXYEAR', 'TRANSACTIONTYPE', 'TRANSACTIONAMOUNT', 'APPLICATION', 
                      'BILLTYPE', 'TENDERTYPE']
    
    if column_name in numeric_columns:
        return str(val) if pd.notna(val) else ""
    return "" if pd.isna(val) or str(val) in ['nan', 'NaN', 'NAN', ''] else custom_quote(val)

# Apply formatting to all columns
for col in ['TAXYEAR', 'CUSTOMERID', 'LOCATIONID', 'TRANSACTIONDATE', 'BILLINGDATE',
            'DUEDATE', 'BILLORINVOICENUMBER', 'TRANSACTIONTYPE', 'TRANSACTIONAMOUNT',
            'TRANSACTIONDESCRIPTION', 'APPLICATION', 'BILLTYPE', 'TENDERTYPE', 'UPDATEDATE']:
    if col in df_new.columns:
        df_new[col] = df_new[col].apply(lambda x: selective_custom_quote(x, col))

# --------------------------
# Data validation - Remove records missing key fields
# --------------------------
print("\nValidating data...")
initial_count = len(df_new)

# Check missing values before filtering
missing_customerid = (df_new['CUSTOMERID'] == "").sum()
missing_locationid = (df_new['LOCATIONID'] == "").sum()
missing_transdate = (df_new['TRANSACTIONDATE'] == "").sum()

print(f"Records missing CUSTOMERID: {missing_customerid}")
print(f"Records missing LOCATIONID: {missing_locationid}")
print(f"Records missing TRANSACTIONDATE: {missing_transdate}")




# For now, let's see what we have before filtering everything out
print("\nSample of data before filtering:")
print(df_new[['CUSTOMERID', 'LOCATIONID', 'TRANSACTIONDATE', 'TRANSACTIONAMOUNT']].head(10))

# Remove records missing required fields
# Check for empty strings since we converted NaN to ""
required_columns = ['CUSTOMERID', 'LOCATIONID']
for col in required_columns:
    df_new = df_new[df_new[col] != ""]

print(f"\nRemoved records missing required fields")
print(f"Remaining records after filtering: {len(df_new)}")

"""

# Remove records missing required fields
# For testing, let's be less strict - only require TRANSACTIONDATE
df_new_strict = df_new[(df_new['CUSTOMERID'] != "") & 
                       (df_new['LOCATIONID'] != "") & 
                       (df_new['TRANSACTIONDATE'] != "")]

df_new_relaxed = df_new[df_new['TRANSACTIONDATE'] != ""]

print(f"\nStrict filtering (all 3 fields): {len(df_new_strict)} records")
print(f"Relaxed filtering (only TRANSACTIONDATE): {len(df_new_relaxed)} records")

# Use relaxed filtering for now to see some output
df_new = df_new_relaxed
print(f"\nUsing relaxed filtering. Remaining records: {len(df_new)}")
"""
# --------------------------
# Reorder columns based on target format
# --------------------------
column_order = [
    "TAXYEAR", "CUSTOMERID", "LOCATIONID", "TRANSACTIONDATE", "BILLINGDATE", 
    "DUEDATE", "BILLORINVOICENUMBER", "TRANSACTIONTYPE", "TRANSACTIONAMOUNT", 
    "TRANSACTIONDESCRIPTION", "APPLICATION", "BILLTYPE", "TENDERTYPE", "UPDATEDATE"
]

# Keep only the required columns
df_new = df_new[column_order]
print(f"\nOrdered columns according to target format. Final columns: {len(df_new.columns)}")

# --------------------------
# Add trailer row
# --------------------------
trailer_row = pd.DataFrame([["TRAILER"] + [''] * (len(df_new.columns) - 1)], columns=df_new.columns)
df_new = pd.concat([df_new, trailer_row], ignore_index=True)
print(f"Added trailer row. Final row count: {len(df_new)}")

# --------------------------
# Save to CSV
# --------------------------
output_path = os.path.join(os.path.dirname(list(file_paths.values())[0]), 
                          'TENDERTYPE_STAGE_TRANSACTIONAL_HIST_05232025_1336PM.csv')
df_new.to_csv(output_path, index=False, header=True, quoting=csv.QUOTE_NONE, escapechar='\\')
print(f"\nCSV file saved at: {output_path}")

# Print summary
print("\n" + "="*50)
print("TRANSFORMATION COMPLETE")
print("="*50)
print(f"Total records processed: {len(df_new) - 1}")  # Minus trailer row
print(f"Output file: {os.path.basename(output_path)}")