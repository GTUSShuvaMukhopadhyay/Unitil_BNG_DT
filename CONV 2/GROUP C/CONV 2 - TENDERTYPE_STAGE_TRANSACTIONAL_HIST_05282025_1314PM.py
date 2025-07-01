# CONV 2 - TENDERTYPE_STAGE_TRANSACTIONAL_HIST_05282025_1314PM.py
# 
# 
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
# Define file paths
file_paths = {
    "DFKKZP": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_TRANSACTIONAL_HIST\dfkkzp 05092025.XLSX",   
    "ZMECON1": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_TRANSACTIONAL_HIST\ZMECON 2015 to 2020.xlsx",
    "ZMECON2": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_TRANSACTIONAL_HIST\ZMECON 2021 to 03272025.xlsx",
}
 
# Initialize data_sources dictionary to hold our data
data_sources = {}
# Function to read an Excel file
def read_excel_file(name, path):
    try:
        # For ZMECON files, try to read the first sheet regardless of name
        if name.startswith("ZMECON"):
            df = pd.read_excel(path, sheet_name=0, engine="openpyxl")  # 0 means first sheet
        else:
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

# Combine all ZMECON files into a single dataframe
zmecon_keys = [key for key in data_sources.keys() if key.startswith("ZMECON")]
if zmecon_keys:
    zmecon_dfs = [data_sources[key] for key in zmecon_keys if data_sources[key] is not None]
    if zmecon_dfs:
        data_sources["ZMECON"] = pd.concat(zmecon_dfs, ignore_index=True)
        print(f"Combined {len(zmecon_dfs)} ZMECON files into a single dataframe with {len(data_sources['ZMECON'])} rows")
    else:
        print("Warning: No valid ZMECON dataframes found to combine")
else:
    print("Warning: No ZMECON files found in data_sources")

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
# Extract CUSTOMERID and LOCATIONID through joins
# --------------------------
print("\nPerforming join for CUSTOMERID and LOCATIONID using ZMECON...")

if data_sources.get("ZMECON") is not None:
    # Step 1: Prepare join keys
    print("\nPreparing join keys...")
    
    # Get ZMECON dataframe
    zmecon_df = data_sources["ZMECON"].copy()
    
    # Identify columns - using your specified mapping
    contract_account_col = zmecon_df.columns[2]  # Column C (Cont. Acct)
    partner_col = zmecon_df.columns[0]  # Column A (Partner)
    premise_col = zmecon_df.columns[25]  # Column Z (Premise/LOCATIONID)
    
    print(f"Using ZMECON columns:")
    print(f"  Contract Account: '{contract_account_col}' (column C)")
    print(f"  Partner: '{partner_col}' (column A)")
    print(f"  Premise: '{premise_col}' (column Z)")
    
    # Standardize Selection Value 1 from DFKKZP
    df_new["Selection_Value_Clean"] = df_new["Selection Value 1"].apply(
        lambda x: str(int(float(x))) if pd.notna(x) and str(x).replace('.', '').strip() else str(x)
    )
    
    # Standardize Contract Account from ZMECON (column C)
    zmecon_df["Contract_Account_Clean"] = zmecon_df[contract_account_col].apply(
        lambda x: str(int(float(x))) if pd.notna(x) and str(x).replace('.', '').strip() else str(x)
    )
    
    # Debug: Show sample values
    print("\nSample DFKKZP Selection Value 1:", df_new["Selection_Value_Clean"].head(5).tolist())
    print("Sample ZMECON Contract Account:", zmecon_df["Contract_Account_Clean"].head(5).tolist())
    
    # Create mapping from Contract Account to Partner and Premise
    ca_to_info = {}
    for _, row in zmecon_df.iterrows():
        ca = row["Contract_Account_Clean"]
        partner = str(row[partner_col]).strip() if pd.notna(row[partner_col]) else ""
        premise = str(row[premise_col]).strip() if pd.notna(row[premise_col]) else ""
        
        if pd.notna(ca) and ca:
            # Remove .0 from partner and premise if they're float values
            try:
                partner = str(int(float(partner))) if partner and '.' in partner else partner
            except:
                pass
            try:
                premise = str(int(float(premise))) if premise and '.' in premise else premise
            except:
                pass
                
            ca_to_info[ca] = {"partner": partner, "premise": premise}
    
    print(f"\nCreated mapping with {len(ca_to_info)} Contract Account entries")
    
    # Apply mapping to get CUSTOMERID and LOCATIONID
    df_new["CUSTOMERID"] = ""
    df_new["LOCATIONID"] = ""
    
    matched_count = 0
    for idx in df_new.index:
        sv = df_new.at[idx, "Selection_Value_Clean"]
        if sv in ca_to_info:
            df_new.at[idx, "CUSTOMERID"] = ca_to_info[sv]["partner"]
            df_new.at[idx, "LOCATIONID"] = ca_to_info[sv]["premise"]
            matched_count += 1
    
    print(f"\nMatched {matched_count} out of {len(df_new)} records")
    print(f"CUSTOMERID populated: {(df_new['CUSTOMERID'] != '').sum()} records")
    print(f"LOCATIONID populated: {(df_new['LOCATIONID'] != '').sum()} records")
    
    # Clean up temporary column
    df_new.drop("Selection_Value_Clean", axis=1, inplace=True)
    
else:
    print("ZMECON not available, setting CUSTOMERID and LOCATIONID to empty")
    df_new["CUSTOMERID"] = ""
    df_new["LOCATIONID"] = ""
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
                          'TENDERTYPE_STAGE_TRANSACTIONAL_HIST_05282025_1314PM.csv')
df_new.to_csv(output_path, index=False, header=True, quoting=csv.QUOTE_NONE, escapechar='\\')
print(f"\nCSV file saved at: {output_path}")

# Print summary
print("\n" + "="*50)
print("TRANSFORMATION COMPLETE")
print("="*50)
print(f"Total records processed: {len(df_new) - 1}")  # Minus trailer row
print(f"Output file: {os.path.basename(output_path)}")