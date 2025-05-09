#310amsTAGE_BILLING_ACC_NODUPS.py
#New logic added for inactive customers, Max due date, Changes for Penalty code and tax code based on ZMECON
#Date:09May2025
#Time:02:AM CST

import pandas as pd
import os
import sys
import csv
import time
from datetime import datetime

# Record start time for performance tracking
start_time = time.time()
last_time = start_time

# === File paths ===
# Define file paths directly in the script
file_paths = {
    "prem": r"C:\Users\us85360\Documents\STAGE_BILLING_ACCT\ZDM_PREMDETAILS.XLSX",
    "ever": r"C:\Users\us85360\Documents\STAGE_BILLING_ACCT\EVER.XLSX",
    "active": r"C:\Users\us85360\Documents\STAGE_BILLING_ACCT\ZNC_ACTIVE_CUS.XLSX",
    "writeoff": r"C:\Users\us85360\Documents\STAGE_BILLING_ACCT\Write off customer history.XLSX",
    "erdk": r"C:\Users\us85360\Documents\STAGE_BILLING_ACCT\ERDK.XLSX",
    "zmecon1": r"C:\Users\us85360\Documents\STAGE_BILLING_ACCT\ZMECON 01012021 to 02132025.xlsx",
    "zmecon2": r"C:\Users\us85360\Documents\STAGE_BILLING_ACCT\ZMECON 01012015 to 12312020.xlsx"
}

# Simple logging for tracking progress
def log_info(message):
    print(f"INFO: {message}")

def log_error(message):
    print(f"ERROR: {message}")

def print_elapsed_time(message):
    global last_time
    elapsed_time = time.time() - start_time
    interval_time = time.time() - last_time
    last_time = time.time()
    print(message + " Elapsed Time: ", time.strftime("%H:%M:%S", time.gmtime(elapsed_time)), 
          " Interval Time: ", time.strftime("%H:%M:%S", time.gmtime(interval_time)))

# === Load Data ===
def normalize_acct(x):
    try:
        return str(int(float(x)))
    except:
        return ''

log_info("Starting STAGE_BILLING_ACC.py script...")
print_elapsed_time("Script started")

# Load files with error handling
read_opts = {"engine": "openpyxl"}

try:
    log_info("Loading ZMECON data...")
    # Load ZMECON files with full columns for customer/location data
    df_zmecon1 = pd.read_excel(file_paths["zmecon1"], sheet_name='ZMECON', **read_opts)
    df_zmecon2 = pd.read_excel(file_paths["zmecon2"], sheet_name='ZMECON 2', **read_opts)
    
    print(f"ZMECON1 has {len(df_zmecon1)} rows, {len(df_zmecon1.columns)} columns")
    
    # Find account number, customer ID, and location ID columns in ZMECON1
    print("\nSample rows from ZMECON1:")
    print(df_zmecon1.iloc[:3, :10])
    
    # Look for account 210796547 to debug
    if '210796547' in df_zmecon1.iloc[:, 2].astype(str).values:
        sample_idx = df_zmecon1.iloc[:, 2].astype(str).str.contains('210796547').idxmax()
        print(f"\nFound account 210796547 in ZMECON1 at row {sample_idx}")
        sample_row = df_zmecon1.iloc[sample_idx]
        print("Sample row data for selected columns:")
        for col_idx in [0, 2, 26]:  # Customer ID, Account Number, Location ID
            print(f"Column {col_idx}: {sample_row.iloc[col_idx]}")
    
    # Combine ZMECON files
    df_ZMECON_full = pd.concat([df_zmecon1, df_zmecon2], ignore_index=True)
    
    # Extract ACCOUNTNUMBER, CUSTOMERID, and LOCATIONID from ZMECON files
    df_ZMECON_full["ACCOUNTNUMBER"] = df_ZMECON_full.iloc[:, 2].apply(normalize_acct).str.slice(0, 15)
    
    # Use the correct column indices
    df_ZMECON_full["ZMECON_CUSTOMERID"] = df_ZMECON_full.iloc[:, 0].apply(lambda x: str(x) if pd.notna(x) else "")
    df_ZMECON_full["ZMECON_LOCATIONID"] = df_ZMECON_full.iloc[:, 26].apply(lambda x: str(x) if pd.notna(x) else "")
    
    # For debugging - check if columns have data
    print(f"\nZMECON_CUSTOMERID non-empty values: {(df_ZMECON_full['ZMECON_CUSTOMERID'] != '').sum()}")
    print(f"ZMECON_LOCATIONID non-empty values: {(df_ZMECON_full['ZMECON_LOCATIONID'] != '').sum()}")
    
    # Create a combined ZMECON dataset with all needed columns
    df_ZMECON = df_ZMECON_full[["ACCOUNTNUMBER", "ZMECON_CUSTOMERID", "ZMECON_LOCATIONID"]].copy()
    
    # Add penalty column
    df_ZMECON["penalty_val"] = df_ZMECON_full.iloc[:, 24].apply(lambda x: str(x).strip().upper() if pd.notna(x) else "")
    
    # Remove duplicates
    df_ZMECON = df_ZMECON.drop_duplicates(subset="ACCOUNTNUMBER")
    
    print_elapsed_time("ZMECON data loaded and processed")
except Exception as e:
    log_error(f"Error processing ZMECON data: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

try:
    log_info("Loading ERDK data...")
    df_ERDK = pd.read_excel(file_paths["erdk"], **read_opts)
    df_ERDK = df_ERDK.iloc[:, [0, 4]].copy()
    df_ERDK["acct_key"] = df_ERDK.iloc[:, 0].apply(normalize_acct)
    df_ERDK["due_date_raw"] = pd.to_datetime(df_ERDK.iloc[:, 1], errors='coerce')
    df_ERDK = df_ERDK.sort_values("due_date_raw", ascending=False).dropna(subset=["due_date_raw"])
    df_ERDK = df_ERDK.drop_duplicates(subset=["acct_key"])
    print_elapsed_time("ERDK data loaded and processed")
except Exception as e:
    log_error(f"Error processing ERDK data: {e}")
    sys.exit(1)

try:
    log_info("Loading EVER data...")
    df_EVER = pd.read_excel(file_paths["ever"], **read_opts)
    df_EVER = df_EVER.iloc[:, [79, 83, 84]].copy()
    df_EVER["acct_key"] = df_EVER.iloc[:, 0].apply(normalize_acct)
    df_EVER["open_date"] = df_EVER.iloc[:, 1]
    df_EVER["term_date"] = df_EVER.iloc[:, 2]
    df_EVER = df_EVER.drop_duplicates("acct_key")
    print_elapsed_time("EVER data loaded and processed")
except Exception as e:
    log_error(f"Error processing EVER data: {e}")
    sys.exit(1)

try:
    log_info("Loading PREM data...")
    df_Prem = pd.read_excel(file_paths["prem"], **read_opts)
    
    print(f"PREM data loaded with {len(df_Prem)} rows and {len(df_Prem.columns)} columns")
    print("First few rows sample:")
    print(df_Prem.iloc[:3, :10])
    
    # Extract key columns
    df_Prem = df_Prem.copy()  # Make a copy to avoid issues
    df_Prem["acct_key"] = df_Prem.iloc[:, 9].apply(normalize_acct)  # Contract Account
    df_Prem["raw_loc"] = df_Prem.iloc[:, 2].apply(lambda x: str(x) if pd.notna(x) else "")  # Premise
    df_Prem["raw_cust"] = df_Prem.iloc[:, 7].apply(lambda x: str(x) if pd.notna(x) else "")  # Business Partner
    
    # Drop duplicates to avoid 1:many join problems
    df_Prem = df_Prem.drop_duplicates(subset=["acct_key"])
    
    print_elapsed_time("PREM data loaded and processed")
except Exception as e:
    log_error(f"Error processing PREM data: {e}")
    sys.exit(1)

try:
    log_info("Loading WriteOff data...")
    df_WriteOff = pd.read_excel(file_paths["writeoff"], **read_opts)
    df_WriteOff["acct_key"] = df_WriteOff.iloc[:, 1].apply(normalize_acct)
    writeoff_set = set(df_WriteOff["acct_key"])
    print_elapsed_time("WriteOff data loaded and processed")
except Exception as e:
    log_error(f"Error processing WriteOff data: {e}")
    sys.exit(1)

# Function to format dates
def format_date(val):
    try:
        if pd.isna(val) or val in ["", "0"]:
            return None
        return pd.to_datetime(val).strftime('%Y-%m-%d')
    except:
        return None

# Cleanse string function for IDs
def cleanse_string(value, max_length=None):
    """Clean a string value, optionally truncating to max_length"""
    if pd.isna(value) or value == 'nan':
        return ""
    
    # For numeric values, convert to int if possible
    if isinstance(value, (int, float)):
        try:
            return str(int(value))
        except:
            return str(value)
    
    # For string values, strip and truncate
    result = str(value).strip()
    if max_length is not None:
        result = result[:max_length]
    
    return result

# Determine active code
def calculate_active_code(row, writeoff_set):
    if pd.notna(row.get("term_date")) and hasattr(row["term_date"], 'year') and row["term_date"].year == 9999:
        return 0
    elif row.get("acct_key") in writeoff_set:
        return 4
    else:
        return 2

# === Start building the output dataset ===
log_info("Building output dataset...")

# Create initial dataframe from ZMECON accounts without using any index
df_new = df_ZMECON.copy()
print(f"Initial dataset: {len(df_new)} rows")

# Create dictionaries for lookups - this avoids the duplication problem
prem_cust_dict = dict(zip(df_Prem["acct_key"], df_Prem["raw_cust"]))
prem_loc_dict = dict(zip(df_Prem["acct_key"], df_Prem["raw_loc"]))
erdk_due_dict = df_ERDK.set_index("acct_key")["due_date_raw"].to_dict()
ever_open_dict = df_EVER.set_index("acct_key")["open_date"].to_dict()
ever_term_dict = df_EVER.set_index("acct_key")["term_date"].to_dict()

# Apply lookups without merging
print("Applying lookups...")
df_new["acct_key"] = df_new["ACCOUNTNUMBER"]  # Create acct_key for lookups and active code
df_new["open_date"] = df_new["acct_key"].map(ever_open_dict)
df_new["term_date"] = df_new["acct_key"].map(ever_term_dict)
df_new["due_date_raw"] = df_new["acct_key"].map(erdk_due_dict)
df_new["raw_cust"] = df_new["acct_key"].map(prem_cust_dict)
df_new["raw_loc"] = df_new["acct_key"].map(prem_loc_dict)

# Calculate values
print("Calculating fields...")
df_new["ACTIVECODE"] = df_new.apply(lambda row: calculate_active_code(row, writeoff_set), axis=1)
df_new["OPENDATE"] = df_new["open_date"].apply(format_date)
df_new["TERMINATEDDATE"] = df_new["term_date"].apply(format_date)
df_new["DUEDATE"] = df_new["due_date_raw"].apply(format_date)

# Add final CUSTOMERID and LOCATIONID using priority order
# 1. First try PREM data 
# 2. If not available, use ZMECON data
df_new["CUSTOMERID"] = df_new["raw_cust"].fillna(df_new["ZMECON_CUSTOMERID"])
df_new["LOCATIONID"] = df_new["raw_loc"].fillna(df_new["ZMECON_LOCATIONID"])

# Clean up the IDs
df_new["CUSTOMERID"] = df_new["CUSTOMERID"].apply(lambda x: cleanse_string(x, max_length=15))
df_new["LOCATIONID"] = df_new["LOCATIONID"].apply(cleanse_string)

# Check missing values after all filling methods
empty_cust_ids = (df_new["CUSTOMERID"] == "").sum()
empty_loc_ids = (df_new["LOCATIONID"] == "").sum()
print(f"Empty CUSTOMERID values after all fills: {empty_cust_ids} ({empty_cust_ids/len(df_new):.2%})")
print(f"Empty LOCATIONID values after all fills: {empty_loc_ids} ({empty_loc_ids/len(df_new):.2%})")

# Check for specific account
if '210796547' in df_new["ACCOUNTNUMBER"].values:
    print("\nAccount 210796547 data:")
    print(df_new[df_new["ACCOUNTNUMBER"] == "210796547"][["ACCOUNTNUMBER", "CUSTOMERID", "LOCATIONID", "raw_cust", "raw_loc", "ZMECON_CUSTOMERID", "ZMECON_LOCATIONID"]])

# Add PENALTYCODE and TAXTYPE
def get_penalty_tax(row):
    if row["penalty_val"] == "RES":
        return (53, 0)
    else:
        return (55, 1)

df_new[["PENALTYCODE", "TAXTYPE"]] = df_new.apply(lambda row: pd.Series(get_penalty_tax(row)), axis=1)

# Clean up temp columns
df_new = df_new.drop(columns=["acct_key", "open_date", "term_date", "due_date_raw", 
                             "raw_cust", "raw_loc", "ZMECON_CUSTOMERID", "ZMECON_LOCATIONID", "penalty_val"])

print_elapsed_time("All fields calculated")

# === Static values and blank columns ===
defaults = {
    "STATUSCODE": 0, 
    "ADDRESSSEQ": 1, 
    "TAXCODE": 0, 
    "ARCODE": 8, 
    "BANKCODE": 8,
    "DWELLINGUNITS": 1, 
    "STOPSHUTOFF": 0, 
    "STOPPENALTY": 0,
    "SICCODE": "", 
    "BUNCHCODE": "", 
    "SHUTOFFDATE": "", 
    "PIN": "", 
    "DEFERREDDUEDATE": "",
    "LASTNOTICECODE": 0, 
    "LASTNOTICEDATE": "", 
    "CASHONLY": "", 
    "NEMLASTTRUEUPDATE": "",
    "NEMNEXTTRUEUPDATE": "", 
    "ENGINEERNUM": "", 
    "SERVICEADDRESS3": "", 
    "UPDATEDATE": datetime.today().strftime('%Y-%m-%d')
}

for col, val in defaults.items():
    if col not in df_new.columns:
        df_new[col] = val

print_elapsed_time("Default values added")

# === Primary Key for deduplication ===
print(f"Before deduplication: {len(df_new)} rows")
df_new["PRIMARY_KEY"] = df_new["ACCOUNTNUMBER"] + df_new["CUSTOMERID"].fillna('') + df_new["LOCATIONID"].fillna('') + df_new["OPENDATE"].fillna('')
df_new = df_new.drop_duplicates(subset="PRIMARY_KEY")
df_new = df_new.drop(columns=["PRIMARY_KEY"])
print(f"After deduplication: {len(df_new)} rows")

print_elapsed_time("Deduplication completed")

# === Column Order ===
# Column order enforcement
desired_column_order = [
    "ACCOUNTNUMBER", "CUSTOMERID", "LOCATIONID", "ACTIVECODE", "STATUSCODE", "ADDRESSSEQ", "PENALTYCODE",
    "TAXCODE", "TAXTYPE", "ARCODE", "BANKCODE", "OPENDATE", "TERMINATEDDATE",
    "DWELLINGUNITS", "STOPSHUTOFF", "STOPPENALTY", "DUEDATE", "SICCODE",
    "BUNCHCODE", "SHUTOFFDATE", "PIN", "DEFERREDDUEDATE", "LASTNOTICECODE",
    "LASTNOTICEDATE", "CASHONLY", "NEMLASTTRUEUPDATE", "NEMNEXTTRUEUPDATE",
    "ENGINEERNUM", "SERVICEADDRESS3", "UPDATEDATE"
]

for col in desired_column_order:
    if col not in df_new.columns:
        df_new[col] = ""

df_new = df_new[desired_column_order]
print_elapsed_time("Column order enforced")

# === Trailer Row ===
df_new = pd.concat([df_new, pd.DataFrame([["TRAILER"] + [""] * (len(df_new.columns) - 1)], columns=df_new.columns)], ignore_index=True)
print_elapsed_time("Trailer row added")

# Output CSV
output_path = r"C:\Users\us85360\Documents\STAGE_BILLING_ACCT\STAGE_BILLING_ACC_NODUPS.csv"

# Ensure numeric columns are properly formatted
numeric_columns = ['ACTIVECODE', 'STATUSCODE', 'ADDRESSSEQ', 'PENALTYCODE', 'TAXCODE', 'TAXTYPE', 
                    'ARCODE', 'BANKCODE', 'DWELLINGUNITS', 'STOPSHUTOFF', 'STOPPENALTY', 'LASTNOTICECODE']

for col in numeric_columns:
    if col in df_new.columns:
        df_new[col] = pd.to_numeric(df_new[col], errors='coerce').fillna(0).astype(int)

print_elapsed_time("Numeric columns formatted")

# Convert date columns to strings
date_columns = ["OPENDATE", "TERMINATEDDATE", "DUEDATE", "UPDATEDATE"]
for col in date_columns:
    df_new[col] = df_new[col].fillna("").astype(str)

print_elapsed_time("Date columns formatted")

# Use QUOTE_NONNUMERIC to ensure all non-numeric fields (including dates) get quotes
df_new.to_csv(output_path, index=False, quoting=csv.QUOTE_NONNUMERIC)
print_elapsed_time("CSV file saved")

log_info(f"CSV file saved successfully at: {output_path}")
print_elapsed_time("Script completed")