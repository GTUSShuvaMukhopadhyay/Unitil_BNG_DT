
# STAGE_BILLING_ACCT202505190211V1.py
# New logic added for inactive customers, Max due date, Changes for Penalty code and tax code based on ZMECON
# Date:16May2025
# Time:10:20 CST
#2025-May-16 -conv2- remapped the iloc for Tax from 31 to 29
#2025-May-18 -conv2- changed logic to sort df_Prem by rate_category before dropping duplicates
#2025-May-19 - Conv2- changed iloc for zmecon for location ID to 25 from 26

import pandas as pd
import os
import sys
import csv
import time
from datetime import datetime

 # Add the parent directory to sys.path
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
import Conversion_Utils as cu 

cu.print_checklist()

# === File paths ===
# Define file paths directly in the script

# === Load Data ===
def normalize_acct(x):
    try:
        return str(int(float(x)))
    except:
        return ''

cu.log_info("Starting STAGE_BILLING_ACC.py script...")

# Load files with error handling
read_opts = {"engine": "openpyxl"}

try:
    cu.log_info("Loading ZMECON data...")
    # Load ZMECON files with full columns for customer/location data
    # df_zmecon1 = pd.read_excel(file_paths["zmecon1"], sheet_name='ZMECON', **read_opts)
    # df_zmecon2 = pd.read_excel(file_paths["zmecon2"], sheet_name='ZMECON 2', **read_opts)
   
    # print(f"ZMECON1 has {len(df_zmecon1)} rows, {len(df_zmecon1.columns)} columns")
   
    # Find account number, customer ID, and location ID columns in ZMECON1
    # print("\nSample rows from ZMECON1:")
    # print(df_zmecon1.iloc[:3, :10])
   
    # Look for account 210796547 to debug
    # if '210796547' in df_zmecon1.iloc[:, 2].astype(str).values:
        # sample_idx = df_zmecon1.iloc[:, 2].astype(str).str.contains('210796547').idxmax()
        # print(f"\nFound account 210796547 in ZMECON1 at row {sample_idx}")
        # sample_row = df_zmecon1.iloc[sample_idx]
        # print("Sample row data for selected columns:")
        # for col_idx in [0, 2, 25]:  # Customer ID, Account Number, Location ID
            # print(f"Column {col_idx}: {sample_row.iloc[col_idx]}")
   
    # Combine ZMECON files
    # df_ZMECON_full = pd.concat([df_zmecon1, df_zmecon2], ignore_index=True)
    df_ZMECON_full = cu.get_file("zmecon")
   
    # Extract ACCOUNTNUMBER, CUSTOMERID, and LOCATIONID from ZMECON files
    df_ZMECON_full["ACCOUNTNUMBER"] = df_ZMECON_full.iloc[:, 2].apply(normalize_acct).str.slice(0, 15)
   
    # Use the correct column indices
    df_ZMECON_full["ZMECON_CUSTOMERID"] = df_ZMECON_full.iloc[:, 0].apply(lambda x: str(x) if pd.notna(x) else "")
    df_ZMECON_full["ZMECON_LOCATIONID"] = df_ZMECON_full.iloc[:, 25].apply(lambda x: str(x) if pd.notna(x) else "")
   
    # For debugging - check if columns have data
    cu.log_debug(f"\nZMECON_CUSTOMERID non-empty values: {(df_ZMECON_full['ZMECON_CUSTOMERID'] != '').sum()}")
    cu.log_debug(f"ZMECON_LOCATIONID non-empty values: {(df_ZMECON_full['ZMECON_LOCATIONID'] != '').sum()}")
   
    # Sort by date to ensure the latest data is used
    df_ZMECON_full = df_ZMECON_full.sort_values(by=["Business Partner", "Date from #1"], ascending=False)

    # Create a combined ZMECON dataset with all needed columns
    df_ZMECON = df_ZMECON_full[["ACCOUNTNUMBER", "ZMECON_CUSTOMERID", "ZMECON_LOCATIONID"]].copy()
   
    # Add penalty column
    df_ZMECON["penalty_val"] = df_ZMECON_full.iloc[:, 24].apply(lambda x: str(x).strip().upper() if pd.notna(x) else "")
   
    # Remove duplicates
    df_ZMECON = df_ZMECON.drop_duplicates(subset="ACCOUNTNUMBER", keep='first')
   
    cu.log_debug("ZMECON data loaded and processed")
except Exception as e:
    cu.log_error(f"Error processing ZMECON data: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

try:
    cu.log_info("Loading ERDK data...")
    #df_ERDK = pd.read_excel(file_paths["erdk"], **read_opts)
    df_ERDK = cu.get_file("erdk")
    df_ERDK = df_ERDK.iloc[:, [0, 4]].copy()
    df_ERDK["acct_key"] = df_ERDK.iloc[:, 0].apply(normalize_acct)
    df_ERDK["due_date_raw"] = pd.to_datetime(df_ERDK.iloc[:, 1], errors='coerce')
    df_ERDK = df_ERDK.sort_values("due_date_raw", ascending=False).dropna(subset=["due_date_raw"])
    df_ERDK = df_ERDK.drop_duplicates(subset=["acct_key"])
    cu.log_debug("ERDK data loaded and processed")
except Exception as e:
    cu.log_error(f"Error processing ERDK data: {e}")
    sys.exit(1)

try:
    cu.log_info("Loading EVER data...")
    #df_EVER = pd.read_excel(file_paths["ever"], **read_opts)
    df_EVER = cu.get_file("ever")
    df_EVER = df_EVER.iloc[:, [79, 83, 84]].copy() # Columns CB, CF, CG  (Cont. Account, M/I Date, M/O Date)
    df_EVER["acct_key"] = df_EVER.iloc[:, 0].apply(normalize_acct) 
    df_EVER["open_date"] = df_EVER.iloc[:, 1]
    df_EVER["term_date"] = df_EVER.iloc[:, 2]
    df_EVER = df_EVER.sort_values(by=["acct_key", "term_date"], ascending=False).drop_duplicates("acct_key", keep='first')
    cu.log_debug("EVER data loaded and processed")
except Exception as e:
    cu.log_error(f"Error processing EVER data: {e}")
    sys.exit(1)

try:
    cu.log_info("Loading PREM data...")
    #df_Prem = pd.read_excel(file_paths["prem"], **read_opts)
    df_Prem = cu.get_file("prem")
    
    print(f"PREM data loaded with {len(df_Prem)} rows and {len(df_Prem.columns)} columns")
    print("First few rows sample:")
    print(df_Prem.iloc[:3, :10])
   
    # Extract key columns
    df_Prem = df_Prem.copy()  # Make a copy to avoid issues
    df_Prem["acct_key"] = df_Prem.iloc[:, 9].apply(normalize_acct)  # Contract Account
    df_Prem["raw_loc"] = df_Prem.iloc[:, 2].apply(lambda x: str(x) if pd.notna(x) else "")  # Premise
    df_Prem["raw_cust"] = df_Prem.iloc[:, 7].apply(lambda x: str(int(x)) if pd.notna(x) else "")  # Business Partner
    df_Prem["rate_category"] = df_Prem.iloc[:, 4].apply(lambda x: str(x) if pd.notna(x) else "")
    df_Prem["ca_adid"] = df_Prem.iloc[:, 11].apply(normalize_acct)
    df_Prem["tax_jurisdiction"] = df_Prem.iloc[:, 29].apply(lambda x: str(x) if pd.notna(x) else "")
    df_Prem = df_Prem.sort_values(by=["acct_key", "rate_category"], ascending=False)
   
    # Drop duplicates to avoid 1:many join problems
    df_Prem = df_Prem.drop_duplicates(subset=["acct_key"], keep='first')
   
    cu.log_debug("PREM data loaded and processed")
except Exception as e:
    cu.log_error(f"Error processing PREM data: {e}")
    sys.exit(1)

try:
    cu.log_info("Loading WriteOff data...")
    #df_WriteOff = pd.read_excel(file_paths["writeoff"], **read_opts)
    df_WriteOff = cu.get_file("writeoff")
    df_WriteOff["acct_key"] = df_WriteOff.iloc[:, 1].apply(normalize_acct)
    writeoff_set = set(df_WriteOff["acct_key"])
    cu.log_debug("WriteOff data loaded and processed")
except Exception as e:
    cu.log_error(f"Error processing WriteOff data: {e}")
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
cu.log_info("Building output dataset...")

# Create initial dataframe from ZMECON accounts without using any index
df_new = df_ZMECON.copy()
print(f"Initial dataset: {len(df_new)} rows")

# Create dictionaries for lookups - this avoids the duplication problem
prem_cust_dict = dict(zip(df_Prem["acct_key"], df_Prem["raw_cust"]))
prem_loc_dict = dict(zip(df_Prem["acct_key"], df_Prem["raw_loc"]))
prem_rate_category_dict = df_Prem.set_index("acct_key")["rate_category"].to_dict()
prem_ca_adid_dict = df_Prem.set_index("acct_key")["ca_adid"].to_dict()
prem_tax_jurisdiction_dict = df_Prem.set_index("acct_key")["tax_jurisdiction"].to_dict()
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
df_new["rate_category"] = df_new["acct_key"].map(prem_rate_category_dict)
df_new["ca_adid"] = df_new["acct_key"].map(prem_ca_adid_dict)
df_new["tax_jurisdiction"] = df_new["acct_key"].map(prem_tax_jurisdiction_dict)

# Calculate values
print("Calculating fields...")
df_new["ACTIVECODE"] = df_new.apply(lambda row: calculate_active_code(row, writeoff_set), axis=1)
df_new["OPENDATE"] = df_new["open_date"].apply(format_date)
df_new["TERMINATEDDATE"] = df_new["term_date"].apply(format_date)
df_new["DUEDATE"] = df_new["due_date_raw"].apply(format_date)

# Set empty OPENDATE values to 1950-01-01
df_new["OPENDATE"] = df_new["OPENDATE"].fillna("1950-01-01")

# Add final CUSTOMERID and LOCATIONID using priority order
# 1. First try PREM data
# 2. If not available, use ZMECON data
df_new["CUSTOMERID"] = df_new["raw_cust"].fillna(df_new["ZMECON_CUSTOMERID"])
df_new["LOCATIONID"] = df_new["raw_loc"].fillna(df_new["ZMECON_LOCATIONID"])

# Clean up the IDs
df_new["CUSTOMERID"] = df_new["CUSTOMERID"].apply(lambda x: cleanse_string(x, max_length=15))
df_new["LOCATIONID"] = df_new["LOCATIONID"].apply(cleanse_string)
df_new.loc[df_new["CUSTOMERID"] == "1005519", "LOCATIONID"] = "7000074010"

# Check missing values after all filling methods
empty_cust_ids = (df_new["CUSTOMERID"] == "").sum()
empty_loc_ids = (df_new["LOCATIONID"] == "").sum()
print(f"Empty CUSTOMERID values after all fills: {empty_cust_ids} ({empty_cust_ids/len(df_new):.2%})")
print(f"Empty LOCATIONID values after all fills: {empty_loc_ids} ({empty_loc_ids/len(df_new):.2%})")

# Check for specific account
if '210796547' in df_new["ACCOUNTNUMBER"].values:
    print("\nAccount 210796547 data:")
    print(df_new[df_new["ACCOUNTNUMBER"] == "210796547"][["ACCOUNTNUMBER", "CUSTOMERID", "LOCATIONID", "raw_cust", "raw_loc", "ZMECON_CUSTOMERID", "ZMECON_LOCATIONID"]])

# Add PENALTYCODE
def get_penalty(row):
    if row["penalty_val"] == "RES":
        return 53
    else:
        return 55

# Define Tax Code Mapping

tax_data = {
    ("T_ME_RESID", "1", "ME0000000"): (1, 0),
    ("T_ME_RESID", None, "ME0000000"): (1, 0),
    ("T_ME_LIHEA", "1", "ME0000000"): (1, 0),
    ("T_ME_LIHEA", None, "ME0000000"): (1, 0),
    ("T_ME_SCISL", None, "ME0000000"): (0, 1),
    ("T_ME_SCISL", "1", "ME0000000"): (0, 1),
    ("T_ME_SCISL", "2", "ME0000000"): (0, 1),
    ("T_ME_SCISL", "2", "EXME00000"): (0, 6),
    ("T_ME_SCISL", "3", "EXME00000"): (0, 6),
    ("T_ME_SCISL", "6", "ME0000000"): (0, 1),
    ("T_ME_SCISL", "7", "EXME00000"): (0, 6),
    ("T_ME_SCISL", "8", "EXME00000"): (0, 6),
    ("T_ME_SCISL", "8", "ME0000000"): (0, 1),
    ("T_ME_SCITR", "2", "ME0000000"): (2, 1),
    ("T_ME_SCITR", "3", "EXME00000"): (2, 6),
    ("T_ME_SCITR", "6", "ME0000000"): (2, 1),
    ("T_ME_LCISL", None, "ME0000000"): (0, 1),
    ("T_ME_LCISL", "2", "ME0000000"): (0, 1),
    ("T_ME_LCISL", "6", "ME0000000"): (0, 1),
    ("T_ME_LCITR", None, "ME0000000"): (2, 1),
    ("T_ME_LCITR", "2", "EXME00000"): (2, 6),
    ("T_ME_LCITR", "2", "ME0000000"): (2, 1),
    ("T_ME_LCITR", "3", "EXME00000"): (2, 6),
    ("T_ME_LCITR", "3", "ME0000000"): (2, 1),
    ("T_ME_LCITR", "6", "EXME00000"): (2, 6),
    ("T_ME_LCITR", "6", "ME0000000"): (2, 1),
    ("T_ME_LCITR", "8", "ME0000000"): (2, 1),
}

# Add TAXTYPE and TAXCODE
def get_tax_code_type(row):
    if row["ACTIVECODE"] == 0:
        result = tax_data.get((row["rate_category"], row["ca_adid"], row["tax_jurisdiction"]))
        return result
    else:
        if row["penalty_val"] == "RES":
            return (1,0)
        elif row["penalty_val"] == "SCI":
            return (0,1)
        elif row["penalty_val"] == "SCIT":
            return (2,1)
        elif row["penalty_val"] == "LCI":
            return (0,1)
        elif row["penalty_val"] == "LCIT":
            return (2,1)
        else:
            return (0,1)


df_new["PENALTYCODE"] = df_new.apply(lambda row: get_penalty(row), axis=1)
df_new[["TAXCODE", "TAXTYPE"]] = df_new.apply(lambda row: pd.Series(get_tax_code_type(row)), axis=1)

# Clean up temp columns
df_new = df_new.drop(columns=["acct_key", "open_date", "term_date", "due_date_raw",
                             "raw_cust", "raw_loc", "ZMECON_CUSTOMERID", "ZMECON_LOCATIONID", "penalty_val"])

cu.log_debug("All fields calculated")

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

cu.log_debug("Default values added")

# === Primary Key for deduplication ===
print(f"Before deduplication: {len(df_new)} rows")
df_new["PRIMARY_KEY"] = df_new["ACCOUNTNUMBER"] + df_new["CUSTOMERID"].fillna('') + df_new["LOCATIONID"].fillna('') + df_new["OPENDATE"].fillna('')
df_new = df_new.drop_duplicates(subset="PRIMARY_KEY")
df_new = df_new.drop(columns=["PRIMARY_KEY"])
print(f"After deduplication: {len(df_new)} rows")

cu.log_debug("Deduplication completed")

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
cu.log_debug("Column order enforced")

# === Trailer Row ===
df_new = pd.concat([df_new, pd.DataFrame([["TRAILER"] + [""] * (len(df_new.columns) - 1)], columns=df_new.columns)], ignore_index=True)
cu.log_debug("Trailer row added")

# Output CSV
# output_path = r"C:\Users\US82783\OneDrive - Grant Thornton Advisors LLC\Desktop\python\CONV 2B _ 2nd run\Group A\STAGE_BILLING_ACCT.csv"
output_path = cu.get_output_path("STAGE_BILLING_ACCT.csv")

# Ensure numeric columns are properly formatted
numeric_columns = ['ACTIVECODE', 'STATUSCODE', 'ADDRESSSEQ', 'PENALTYCODE', 'TAXCODE', 'TAXTYPE',
                    'ARCODE', 'BANKCODE', 'DWELLINGUNITS', 'STOPSHUTOFF', 'STOPPENALTY', 'LASTNOTICECODE']

for col in numeric_columns:
    if col in df_new.columns:
        df_new[col] = pd.to_numeric(df_new[col], errors='coerce').fillna(0).astype(int)

cu.log_debug("Numeric columns formatted")

# Convert date columns to strings
date_columns = ["OPENDATE", "TERMINATEDDATE", "DUEDATE", "UPDATEDATE"]
for col in date_columns:
    df_new[col] = df_new[col].fillna("").astype(str)

cu.log_debug("Date columns formatted")

# Use QUOTE_NONNUMERIC to ensure all non-numeric fields (including dates) get quotes
df_new.to_csv(output_path, index=False, quoting=csv.QUOTE_NONNUMERIC)

cu.log_info(f"CSV file saved successfully at: {output_path}")
cu.log_info("Script completed")
