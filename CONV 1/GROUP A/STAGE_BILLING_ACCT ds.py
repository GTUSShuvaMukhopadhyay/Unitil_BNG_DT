#STAGE_BILLING_ACC.py
#New logic added for inactive customers, Max due date, Changes for Penalty code and tax code based on ZMECON
#Date:05May2025
#Time:04:AM CST

############
# Transformation Logic:
# ACCOUNTNUMBER:  Extracted from ZMECON.ACCOUNTNUMBER
# CUSTOMERID:     Extracted from df_Prem (Column 7) and truncated to 15 characters.
# LOCATIONID:     Extracted from df_Prem (Column 2).
# ACTIVECODE:     Derived from df_EVER and df_ERDK. Default is 2, set to 0 if term_date is 9999, 4 if in writeoff_set.
# STATUSCODE:     Default is 0.
# ADDRESSSEQ:     Default is 1.
# PENALTYCODE:    Derived from df_ZMECON. Default is 55, set to 53 if RES.
# TAXCODE:       Default is 0.
# TAXTYPE:       Default is 0.
# ARCODE:        Default is 8.
# BANKCODE:      Default is 8.
# OPENDATE:      Derived from df_EVER (Column 83).
# TERMINATEDDATE: Derived from df_EVER (Column 84).
# DWELLINGUNITS: Default is 1.
# STOPSHUTOFF:   Default is 0.
# STOPPENALTY:   Default is 0.
# DUEDATE:       Derived from df_ERDK (Column 4).
# SICCODE:       Default is empty.
# BUNCHCODE:     Default is empty.
# SHUTOFFDATE:   Default is empty.
# PIN:           Default is empty.
# DEFERREDDUEDATE: Default is empty.
# LASTNOTICECODE: Default is 0.
# LASTNOTICEDATE: Default is empty.
# CASHONLY:      Default is empty.
# NEMLASTTRUEUPDATE: Default is empty.
# NEMNEXTTRUEUPDATE: Default is empty.
# ENGINEERNUM:   Default is empty.
# SERVICEADDRESS3: Default is empty.
# UPDATEDATE:    Default is current date.
#
# PRIMARY_KEY:   Created from ACCOUNTNUMBER, CUSTOMERID, LOCATIONID, and OPENDATE. Used for deduplication.
# Deduplicated based on PRIMARY_KEY.
# TRAILER:       Added at the end of the DataFrame.

import pandas as pd
import os
import sys
import csv
from datetime import datetime

# Add the parent directory to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

import Conversion_Utils as conv_utils  # Assuming this is a local module

conv_utils.log_info( "Starting script...")

def print_elapsed_time( message ):
    global last_time
    elapsed_time = time.time() - start_time
    interval_time = time.time() - last_time
    last_time = time.time()
    print( message + " Elapsed Time: ", time.strftime("%H:%M:%S", time.gmtime(elapsed_time)), " Interval Time: ", time.strftime("%H:%M:%S", time.gmtime(interval_time)))

# === Load Data ===
def normalize_acct(x):
    try:
        return str(int(float(x)))
    except:
        return ''

# Load and clean ZMECON data
df_ZMECON = conv_utils.get_file( "zmecon" ) 
df_ZMECON = df_ZMECON.iloc[:, [2, 24]].copy()
df_ZMECON["ACCOUNTNUMBER"] = df_ZMECON.iloc[:, 0].apply(normalize_acct).str.slice(0, 15)
# Calculate penalty code and tax type based on the value in column 24
df_ZMECON["penalty_val"] = df_ZMECON.iloc[:, 1].apply(lambda x: str(x).strip().upper())
df_ZMECON = df_ZMECON.drop_duplicates(subset="ACCOUNTNUMBER")

conv_utils.log_debug("Completed prepare for ZMECON")

# Clean up ERDK data
df_ERDK = conv_utils.get_file( "erdk" ) 
df_ERDK = df_ERDK.iloc[:, [0, 4]].copy()
df_ERDK["acct_key"] = df_ERDK.iloc[:, 0].apply(normalize_acct)
df_ERDK["due_date_raw"] = pd.to_datetime(df_ERDK.iloc[:, 1], errors='coerce')
df_ERDK = df_ERDK.sort_values("due_date_raw", ascending=False).dropna(subset=["due_date_raw"])
df_ERDK = df_ERDK.drop_duplicates(subset=["acct_key"]).set_index("acct_key")

conv_utils.log_debug("Completed prepare for ERDK")

# Prepare EVER data
df_EVER = conv_utils.get_file( "ever" ) 
df_EVER = df_EVER.iloc[:, [79, 83, 84]].copy()
df_EVER["acct_key"] = df_EVER.iloc[:, 0].apply(normalize_acct)
df_EVER["open_date"] = df_EVER.iloc[:, 1]
df_EVER["term_date"] = df_EVER.iloc[:, 2]
df_EVER = df_EVER.drop_duplicates("acct_key").set_index("acct_key")

conv_utils.log_debug("Completed prepare for EVER")

# Prepare PREM data
df_Prem = conv_utils.get_file( "prem" )
df_Prem = df_Prem.iloc[:, [2, 7, 9]].copy()
df_Prem["raw_loc"] = df_Prem.iloc[:, 0]
df_Prem["raw_cust"] = df_Prem.iloc[:, 1]
df_Prem["acct_key"] = df_Prem.iloc[:, 2].apply(normalize_acct)
df_Prem = df_Prem.set_index("acct_key")

conv_utils.log_debug("Completed prepare for PREM")

# Prepare writeoff data to create the set for writeoff accounts
df_WriteOff = conv_utils.get_file( "writeoff" ) 
df_WriteOff["acct_key"] = df_WriteOff.iloc[:, 1].apply(normalize_acct)
writeoff_set = set(df_WriteOff["acct_key"])

conv_utils.log_debug("Completed prepare for WriteOff")

# Function to format dates
def format_date(val):
    try:
        if pd.isna(val) or val in ["", "0"]:
            return None
        return pd.to_datetime(val).strftime('%Y-%m-%d')
    except:
        return None

# Determine active code
def calculate_active_code(row):
    if hasattr(row["term_date"], 'year') and row["term_date"].year == 9999:
        return 0
    #elif row["ACCOUNTNUMBER"] in writeoff_set:
    elif row.name in writeoff_set:
        return 4
    else:
        return 2
    
# Join the dataframes
df_new = pd.DataFrame()
df_new = df_ZMECON[[ "ACCOUNTNUMBER", "penalty_val"]].copy().set_index("ACCOUNTNUMBER", drop=False)
df_new = df_new.merge(df_EVER[["open_date", "term_date"]], left_index=True, right_index=True, how="left")
df_new = df_new.merge(df_ERDK[["due_date_raw"]], left_index=True, right_index=True, how="left")
df_new = df_new.merge(df_Prem[["raw_cust", "raw_loc"]], left_index=True, right_index=True, how="left")

conv_utils.log_debug("Join DataFrames")

# Calculate active code and format dates
df_new["ACTIVECODE"] = df_new.apply(calculate_active_code, axis=1).astype(int)
df_new["OPENDATE"] = df_new["open_date"].apply(format_date)
df_new["TERMINATEDDATE"] = df_new["term_date"].apply(format_date)
df_new["DUEDATE"] = df_new["due_date_raw"].apply(format_date)

conv_utils.log_debug("Calculate Active Code and Format Dates")

# Add CUSTOMERID and LOCATIONID
df_new["CUSTOMERID"] = df_new["raw_cust"].apply(conv_utils.cleanse_string, max_length=15)
df_new["LOCATIONID"] = df_new["raw_loc"].apply(conv_utils.cleanse_string)

conv_utils.log_debug("Add CUSTOMERID and LOCATIONID")

# Add PENALTYCODE and TAXTYPE 
def get_penalty_tax(row):
    if row["penalty_val"] == "RES":
        return pd.Series([53, 0])
    else:
        return pd.Series([55, 1])

df_new[["PENALTYCODE", "TAXTYPE"]] = df_new.apply(get_penalty_tax, axis=1)
df_new["PENALTYCODE"] = df_new["PENALTYCODE"].astype(int)
df_new["TAXTYPE"] = df_new["TAXTYPE"].astype(int)

conv_utils.log_debug("Add PENALTYCODE and TAXTYPE")

# === Static values and blank columns ===
defaults = {
    "STATUSCODE": "0", "ADDRESSSEQ": "1", "TAXCODE": "0", "ARCODE": "8", "BANKCODE": "8",
    "DWELLINGUNITS": "1", "STOPSHUTOFF": "0", "STOPPENALTY": "0",
    "SICCODE": "", "BUNCHCODE": "", "SHUTOFFDATE": "", "PIN": "", "DEFERREDDUEDATE": "",
    "LASTNOTICECODE": "0", "LASTNOTICEDATE": "", "CASHONLY": "", "NEMLASTTRUEUPDATE": "",
    "NEMNEXTTRUEUPDATE": "", "ENGINEERNUM": "", "SERVICEADDRESS3": "", "UPDATEDATE": datetime.today().strftime('%Y-%m-%d')
}
for col, val in defaults.items():
    if col not in df_new.columns:
        df_new[col] = val

# === Primary Key for deduplication ===
df_new["PRIMARY_KEY"] = df_new.index.name + df_new["CUSTOMERID"] + df_new["LOCATIONID"] + df_new["OPENDATE"]
df_new = df_new.drop_duplicates(subset="PRIMARY_KEY")
df_new = df_new.drop(columns=["PRIMARY_KEY"])

conv_utils.log_debug("Deduplication")

# === Column Order ===
# Column order enforcement
desired_column_order = [
    "ACCOUNTNUMBER", "CUSTOMERID", "LOCATIONID", "ACTIVECODE","STATUSCODE","ADDRESSSEQ", "PENALTYCODE",
    "TAXCODE", "TAXTYPE", "ARCODE", "BANKCODE", "OPENDATE", "TERMINATEDDATE",
    "DWELLINGUNITS", "STOPSHUTOFF", "STOPPENALTY", "DUEDATE", "SICCODE",
    "BUNCHCODE", "SHUTOFFDATE", "PIN", "DEFERREDDUEDATE", "LASTNOTICECODE",
    "LASTNOTICEDATE", "CASHONLY", "NEMLASTTRUEUPDATE", "NEMNEXTTRUEUPDATE",
    "ENGINEERNUM", "SERVICEADDRESS3", "UPDATEDATE"
]
for col in desired_column_order:
    if col not in df_new.columns:
        df_new[col] = ""
#df_new = df_new[desired_column_order + [col for col in df_new.columns if col not in desired_column_order]]
df_new = df_new[desired_column_order]
 
# === Trailer Row ===
df_new = pd.concat([df_new, pd.DataFrame([["TRAILER"] + [""] * (len(df_new.columns) - 1)], columns=df_new.columns)], ignore_index=True)

conv_utils.log_debug("Trailer Row")

# Output CSV
output_path = r"C:\DV\Unitil\STAGE_BILLING_ACCT.csv"

# Ensure numeric columns are properly formatted
numeric_columns = ['ACTIVECODE','STATUSCODE','ADDRESSSEQ', 'PENALTYCODE', 'TAXCODE', 'TAXTYPE', 'ARCODE', 'BANKCODE', 'DWELLINGUNITS',
                   'STOPSHUTOFF', 'STOPPENALTY', 'SICCODE', 'BUNCHCODE', 'LASTNOTICECODE', 'LASTNOTICEDATE',
                   'NEMLASTTRUEUPDATE', 'NEMNEXTTRUEUPDATE', 'ENGINEERNUM', 'SERVICEADDRESS3']
 
for col in numeric_columns:
    if col in df_new.columns:
        df_new[col] = pd.to_numeric(df_new[col], errors='coerce').fillna(0).astype(int)
 
conv_utils.log_debug("Numeric Columns")

# Convert date columns to strings
date_columns = ["OPENDATE", "TERMINATEDDATE", "DUEDATE", "UPDATEDATE"]
for col in date_columns:
    df_new[col] = df_new[col].fillna("").astype(str)
 
conv_utils.log_debug("Date Columns")

# Use QUOTE_NONNUMERIC to ensure all non-numeric fields (including dates) get quotes
df_new.to_csv(output_path, index=False, quoting=csv.QUOTE_NONNUMERIC) 
conv_utils.log_info("CSV file saved successfully at:"+ output_path)
conv_utils.log_info("End of Script")