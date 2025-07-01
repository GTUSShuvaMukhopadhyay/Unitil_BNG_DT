#STAGE_BILLING_ACC.py
#New logic added for inactive customers, Max due date, Changes for Penalty code and tax code based on ZMECON
#Date:05May2025
#Time:04:AM CST

import pandas as pd
import os
import csv
from datetime import datetime

# === File paths ===
file_paths = {
    "prem": r"C:\Users\US97684\Downloads\STAGE_BILLING_ACCT\ZDM_PREMDETAILS.XLSX",
    "ever": r"C:\Users\US97684\Downloads\STAGE_BILLING_ACCT\EVER.XLSX",
    "active": r"C:\Users\US97684\Downloads\STAGE_BILLING_ACCT\ZNC_ACTIVE_CUS.XLSX",
    "writeoff": r"C:\Users\US97684\Downloads\STAGE_BILLING_ACCT\Write off customer history.XLSX",
    "erdk": r"C:\Users\US97684\Downloads\STAGE_BILLING_ACCT\ERDK.XLSX",
    "zmecon1": r"C:\Users\US97684\Downloads\documents_20250317_New\Outbound\ZMECON\ZMECON\ZMECON 01012021 to 02132025.xlsx",
    "zmecon2": r"C:\Users\US97684\Downloads\documents_20250317_New\Outbound\ZMECON\ZMECON\ZMECON 01012015 to 12312020.xlsx"
}

# === Load Data ===
def normalize_acct(x):
    try:
        return str(int(float(x)))
    except:
        return ''

read_opts = {"engine": "openpyxl"}
df_Prem = pd.read_excel(file_paths["prem"], **read_opts)
df_EVER = pd.read_excel(file_paths["ever"], **read_opts)
df_ActiveCus = pd.read_excel(file_paths["active"], **read_opts)
df_WriteOff = pd.read_excel(file_paths["writeoff"], **read_opts)
df_ERDK = pd.read_excel(file_paths["erdk"], **read_opts)

df_ERDK["acct_key"] = df_ERDK.iloc[:, 0].apply(normalize_acct)
df_ERDK["due_date_raw"] = pd.to_datetime(df_ERDK.iloc[:, 4], errors='coerce')
df_ERDK = df_ERDK.sort_values("due_date_raw", ascending=False).dropna(subset=["due_date_raw"])
df_ERDK = df_ERDK.drop_duplicates(subset=["acct_key"]).set_index("acct_key")

df_ZMECON = pd.concat([
    pd.read_excel(file_paths["zmecon1"], sheet_name='ZMECON', **read_opts),
    pd.read_excel(file_paths["zmecon2"], sheet_name='ZMECON 2', **read_opts)
], ignore_index=True)

df_ZMECON["ACCOUNTNUMBER"] = df_ZMECON.iloc[:, 2].apply(normalize_acct).str.slice(0, 15)
df_ZMECON = df_ZMECON.drop_duplicates(subset="ACCOUNTNUMBER").set_index("ACCOUNTNUMBER")

df_new = pd.DataFrame()
df_new["ACCOUNTNUMBER"] = df_ZMECON.index

df_EVER["acct_key"] = df_EVER.iloc[:, 79].apply(normalize_acct)
df_EVER = df_EVER.drop_duplicates("acct_key").set_index("acct_key")

df_ActiveCus["acct_key"] = df_ActiveCus.iloc[:, 3].apply(normalize_acct)
df_ActiveCus = df_ActiveCus.drop_duplicates("acct_key").set_index("acct_key")

df_WriteOff["acct_key"] = df_WriteOff.iloc[:, 1].apply(normalize_acct)
writeoff_set = set(df_WriteOff["acct_key"])

def format_date(val):
    try:
        if pd.isna(val) or val in ["", "0"]:
            return None
        return pd.to_datetime(val).strftime('%Y-%m-%d')
    except:
        return None

active_codes = []
opendates = []
terminated_dates = []
duedates = []

for acct in df_new["ACCOUNTNUMBER"]:
    ever = df_EVER.loc[acct] if acct in df_EVER.index else None
    erdk = df_ERDK.loc[acct] if acct in df_ERDK.index else None

    open_date = term_date = due_date = None
    active_code = 2

    if ever is not None:
        open_date = ever.iloc[83] if len(ever) > 83 else None
        term_date = ever.iloc[84] if len(ever) > 84 else None
        if pd.notna(term_date) and hasattr(term_date, 'year') and term_date.year == 9999:
            active_code = 0
        elif acct in writeoff_set:
            active_code = 4

    if erdk is not None:
        due_date = erdk.iloc[4] if len(erdk) > 4 else None

    active_codes.append(active_code)
    opendates.append(format_date(open_date))
    terminated_dates.append(format_date(term_date))
    duedates.append(format_date(due_date))

df_new["ACTIVECODE"] = active_codes
df_new["OPENDATE"] = opendates
df_new["TERMINATEDDATE"] = terminated_dates
df_new["DUEDATE"] = duedates

# Extract CUSTOMERID and LOCATIONID only
def get_ids(row):
    idx = row.name
    customer_id = ''
    location_id = ''
    
    raw_cust = df_Prem.iloc[idx, 7] if idx < len(df_Prem) else ''
    raw_loc = df_Prem.iloc[idx, 2] if idx < len(df_Prem) else ''
    
    if pd.notna(raw_cust):
        customer_id = str(int(raw_cust)) if isinstance(raw_cust, (int, float)) else str(raw_cust)
        customer_id = customer_id[:15]
    if pd.notna(raw_loc):
        location_id = str(raw_loc)
    
    return pd.Series([customer_id, location_id])

df_new[["CUSTOMERID", "LOCATIONID"]] = df_new.apply(get_ids, axis=1)

# === Add PENALTYCODE and TAXTYPE from ZMECON ===
def get_penalty_tax(acct):
    try:
        row = df_ZMECON.loc[acct]
        val = row.iloc[24] if len(row) > 24 else ''
        if str(val).strip().upper() == "RES":
            return pd.Series([53, 0])
        else:
            return pd.Series([55, 1])
    except:
        return pd.Series([55, 1])  # default if not found or error

df_new[["PENALTYCODE", "TAXTYPE"]] = df_new["ACCOUNTNUMBER"].apply(get_penalty_tax)

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

# === Trailer Row ===
df_new = pd.concat([df_new, pd.DataFrame([["TRAILER"] + [""] * (len(df_new.columns) - 1)], columns=df_new.columns)], ignore_index=True)

# === Primary Key for deduplication ===
df_new["PRIMARY_KEY"] = df_new["ACCOUNTNUMBER"] + df_new["CUSTOMERID"] + df_new["LOCATIONID"] + df_new["OPENDATE"]
df_new = df_new.drop_duplicates(subset="PRIMARY_KEY")
df_new = df_new.drop(columns=["PRIMARY_KEY"])

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
df_new = df_new[desired_column_order + [col for col in df_new.columns if col not in desired_column_order]]
 
# Output CSV
output_path = r"C:\Users\US97684\Downloads\STAGE_BILLING_ACCT\STAGE_BILLING_ACCTMay05_newq.csv"
numeric_columns = ['ACTIVECODE','STATUSCODE','ADDRESSSEQ', 'PENALTYCODE', 'TAXCODE', 'TAXTYPE', 'ARCODE', 'BANKCODE', 'DWELLINGUNITS',
                   'STOPSHUTOFF', 'STOPPENALTY', 'SICCODE', 'BUNCHCODE', 'LASTNOTICECODE', 'LASTNOTICEDATE',
                   'NEMLASTTRUEUPDATE', 'NEMNEXTTRUEUPDATE', 'ENGINEERNUM', 'SERVICEADDRESS3']
 
# Ensure numeric columns are properly formatted
for col in numeric_columns:
    if col in df_new.columns:
        df_new[col] = pd.to_numeric(df_new[col], errors='coerce')
 
# Save the DataFrame to CSV
df_new.to_csv(output_path, index=False, quoting=csv.QUOTE_MINIMAL)
# Convert date columns to strings
date_columns = ["OPENDATE", "TERMINATEDDATE", "DUEDATE", "UPDATEDATE"]
for col in date_columns:
    df_new[col] = df_new[col].fillna("").astype(str)
 
# Ensure numeric columns are properly formatted as numbers
numeric_columns = ['ACTIVECODE','STATUSCODE','ADDRESSSEQ','PENALTYCODE',
                   'TAXCODE','TAXTYPE','ARCODE','BANKCODE','DWELLINGUNITS',
                   'STOPSHUTOFF','STOPPENALTY','LASTNOTICECODE']
for col in numeric_columns:
    if col in df_new.columns:
        df_new[col] = pd.to_numeric(df_new[col], errors='coerce').fillna(0).astype(int)
 
# Use QUOTE_NONNUMERIC to ensure all non-numeric fields (including dates) get quotes
df_new.to_csv(output_path, index=False, quoting=csv.QUOTE_NONNUMERIC) 
print("CSV file saved successfully at:", output_path)