# CONV1 - STAGE_BILLING_ACCT.py
# STAGE_BILLING_ACCT.py


import pandas as pd
import os
import re
import csv  # Import the correct CSV module

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
    "ZDM_PREMDETAILS": r"C:\Users\us85360\Desktop\STAGE_BILLING_ACCT\ZDM_PREMDETAILS.XLSX",
    "EVER": r"C:\Users\us85360\Desktop\STAGE_BILLING_ACCT\EVER.XLSX",
    "ZNC_ACTIVE_CUS": r"C:\Users\us85360\Desktop\STAGE_BILLING_ACCT\ZNC_ACTIVE_CUS.XLSX",
    "ERDK": r"C:\Users\us85360\Desktop\STAGE_BILLING_ACCT\ERDK.XLSX",
    "WRITE_OFF": r"C:\Users\us85360\Desktop\STAGE_BILLING_ACCT\Write off customer history.XLSX",
}

# Load the data from each spreadsheet
data_sources = {}
for name, path in file_paths.items():
    try:
        data_sources[name] = pd.read_excel(path, sheet_name="Sheet1", engine="openpyxl")
    except Exception as e:
        data_sources[name] = None
        print(f"Error loading {name}: {e}")

# Initialize df_new as an empty DataFrame
df_new = pd.DataFrame()

# Extract ACCOUNTNUMBER from ZDM_PREMDETAILS (column index 9)
if data_sources["ZDM_PREMDETAILS"] is not None:
    df_new["ACCOUNTNUMBER"] = data_sources["ZDM_PREMDETAILS"].iloc[:, 9].fillna('').apply(
        lambda x: str(int(x)) if isinstance(x, (int, float)) else str(x)
    ).str.slice(0, 15)

# Extract CUSTOMERID from ZDM_PREMDETAILS (column index 7)
if data_sources["ZDM_PREMDETAILS"] is not None:
    df_new["CUSTOMERID"] = data_sources["ZDM_PREMDETAILS"].iloc[:, 7].fillna('').apply(
        lambda x: str(int(x)) if isinstance(x, (int, float)) else str(x)
    ).str.slice(0, 15)

# Extract LOCATIONID from ZDM_PREMDETAILS (column index 2)
if data_sources["ZDM_PREMDETAILS"] is not None:
    df_new["LOCATIONID"] = data_sources["ZDM_PREMDETAILS"].iloc[:, 2].fillna('').astype(str)

# Assign hardcoded values
df_new["STATUSCODE"] = "0"
df_new["ADDRESSSEQ"] = "1"
df_new["TAXCODE"] = "0"
df_new["ARCODE"] = "8"
df_new["BANKCODE"] = "8"
df_new["DWELLINGUNITS"] = "1"
df_new["STOPSHUTOFF"] = "0"
df_new["STOPPENALTY"] = "0"
df_new["SICCODE"] = " "
df_new["BUNCHCODE"] = " "
df_new["SHUTOFFDATE"] = " "
df_new["PIN"] = " "
df_new["DEFERREDDUEDATE"] = " "
df_new["LASTNOTICECODE"] = "0"
df_new["LASTNOTICEDATE"] = " "
df_new["CASHONLY"] = " "
df_new["NEMLASTTRUEUPDATE"] = " "
df_new["NEMNEXTTRUEUPDATE"] = " "
df_new["ENGINEERNUM"] = " "
df_new["SERVICEADDRESS3"] = " "
df_new["UPDATEDATE"] = " "

# Process EVER file for OPENDATE and TERMINATEDDATE
if data_sources["EVER"] is not None:
    ever_data = data_sources["EVER"]
    # Create 'Cont.Account' from column index 79
    ever_data["Cont.Account"] = ever_data.iloc[:, 79].apply(
        lambda x: str(int(x)) if pd.notna(x) else ""
    ).str.strip()
    # Process M/I Date (column index 83) as OPENDATE
    ever_data["M/I Date"] = pd.to_datetime(ever_data.iloc[:, 83], errors='coerce')
    ever_data["M/I Date"] = ever_data["M/I Date"].dt.strftime('%Y-%m-%d')
    df_new["ACCOUNTNUMBER"] = df_new["ACCOUNTNUMBER"].astype(str).str.strip()
    df_new = df_new.merge(ever_data[["Cont.Account", "M/I Date"]], left_on="ACCOUNTNUMBER", right_on="Cont.Account", how="left")
    df_new.rename(columns={"M/I Date": "OPENDATE"}, inplace=True)
    df_new.drop(columns=["Cont.Account"], inplace=True)
    
    # Process M/O Date (column index 85) as TERMINATEDDATE
    ever_data["M/O Date"] = pd.to_datetime(ever_data.iloc[:, 85], errors='coerce')
    ever_data["M/O Date"] = ever_data["M/O Date"].dt.strftime('%Y-%m-%d')
    df_new = df_new.merge(ever_data[["Cont.Account", "M/O Date"]], left_on="ACCOUNTNUMBER", right_on="Cont.Account", how="left")
    df_new.rename(columns={"M/O Date": "TERMINATEDDATE"}, inplace=True)
    df_new.drop(columns=["Cont.Account"], inplace=True)

# Process ERDK file for DUEDATE
if data_sources["ERDK"] is not None:
    erdk_data = data_sources["ERDK"]
    erdk_data["Cont.Account"] = erdk_data.iloc[:, 0].astype(str).str.strip()
    erdk_data["Due"] = pd.to_datetime(erdk_data.iloc[:, 4], errors='coerce')
    erdk_data["Due"] = erdk_data["Due"].dt.strftime('%Y-%m-%d')
    df_new = df_new.merge(erdk_data[["Cont.Account", "Due"]], left_on="ACCOUNTNUMBER", right_on="Cont.Account", how="left")
    df_new.rename(columns={"Due": "DUEDATE"}, inplace=True)
    df_new.drop(columns=["Cont.Account"], inplace=True)

# Process WRITE_OFF file for write_off_accounts
if data_sources["WRITE_OFF"] is not None:
    write_off_accounts = data_sources["WRITE_OFF"].iloc[:, 1].astype(str).unique()
else:
    write_off_accounts = set()

# --- Merge raw ACTIVECODE from EVER (Column CV) ---
if data_sources["EVER"] is not None:
    # Assume CV is in column index 99 of ever_data
    ever_data["CV"] = ever_data.iloc[:, 99].fillna(0).astype(int)
    # Merge raw CV into df_new using "Cont.Account"
    # Make sure to recreate 'Cont.Account' if needed:
    if "Cont.Account" not in ever_data.columns:
        ever_data["Cont.Account"] = ever_data.iloc[:, 79].apply(lambda x: str(int(x)) if pd.notna(x) else "").str.strip()
    df_new = df_new.merge(ever_data[["Cont.Account", "CV"]], left_on="ACCOUNTNUMBER", right_on="Cont.Account", how="left")
    df_new.drop(columns=["Cont.Account"], inplace=True)

# --- Vectorized ACTIVECODE Assignment ---
# Logic:
#   - Use the raw value from column CV as baseline.
#   - If raw value is nonzero, set it to 2 (inactive); if zero, keep 0 (active).
#   - Then, if TERMINATEDDATE equals "12-31-9999", force ACTIVECODE to 0.
#   - Finally, if TERMINATEDDATE is not "12-31-9999" and ACCOUNTNUMBER is in write_off_accounts, set ACTIVECODE to 4.
df_new["ACTIVECODE"] = df_new["CV"].fillna(0)
df_new.loc[df_new["ACTIVECODE"] != 0, "ACTIVECODE"] = 2
df_new.loc[df_new["TERMINATEDDATE"].eq("12/31/9999"), "ACTIVECODE"] = 0
df_new.loc[
    (df_new["TERMINATEDDATE"] != "12/31/9999") & (df_new["ACCOUNTNUMBER"].isin(write_off_accounts)),
    "ACTIVECODE"
] = 4

# Convert ACTIVECODE to integer type to remove decimals
df_new["ACTIVECODE"] = df_new["ACTIVECODE"].astype(int)

# Optionally drop the raw "CV" column if no longer needed:
df_new.drop(columns=["CV"], inplace=True)

# --- Extract PENALTYCODE and TAXTYPE from ZNC_ACTIVE_CUS ---
if data_sources["ZNC_ACTIVE_CUS"] is not None:
    penalty_data = data_sources["ZNC_ACTIVE_CUS"]
    penalty_data["Cont.Account"] = penalty_data.iloc[:, 3].apply(
        lambda x: str(int(x)) if pd.notna(x) else ""
    ).str.strip()
    penalty_data["Fact grp"] = penalty_data.iloc[:, 22].astype(str).str.strip()
    df_new = df_new.merge(penalty_data[["Cont.Account", "Fact grp"]], left_on="ACCOUNTNUMBER", right_on="Cont.Account", how="left")
    penalty_mapping = {"RES": "53", "LCI": "55", "LCIT": "55", "SCI": "55", "SCIT": "55"}
    tax_mapping = {"RES": "0", "LCI": "1", "LCIT": "1", "SCI": "1", "SCIT": "1"}
    df_new["PENALTYCODE"] = df_new["Fact grp"].map(penalty_mapping).fillna("")
    df_new["TAXTYPE"] = df_new["Fact grp"].map(tax_mapping).fillna("")
    df_new.drop(columns=["Cont.Account", "Fact grp"], inplace=True)

# --- Wrap non-numeric values in double quotes (if needed) ---
def custom_quote(val):
    if pd.isna(val) or val in ["", " "]:
        return ""
    return f'"{val}"'
    
def selective_custom_quote(val, column_name):
    if column_name in ['ACTIVECODE','STATUSCODE','ADDRESSSEQ','PENALTYCODE','TAXCODE','TAXTYPE','ARCODE','BANKCODE','DWELLINGUNITS','STOPSHUTOFF','STOPPENALTY','LASTNOTICECODE']:
        return val
    return "" if val in [None, 'nan', 'NaN', 'NAN'] else custom_quote(val)
    
df_new = df_new.fillna("")
df_new = df_new.apply(lambda col: col.map(lambda x: selective_custom_quote(x, col.name)))

# Remove any records missing ACCOUNTNUMBER and drop duplicates
df_new = df_new[df_new['ACCOUNTNUMBER'] != ""]
df_new = df_new.drop_duplicates(subset=['ACCOUNTNUMBER','CUSTOMERID','LOCATIONID'], keep='first')

# Reorder columns based on user preference
column_order = [
    "ACCOUNTNUMBER", "CUSTOMERID", "LOCATIONID", "ACTIVECODE", "STATUSCODE", "ADDRESSSEQ",
    "PENALTYCODE", "TAXCODE", "TAXTYPE", "ARCODE", "BANKCODE", "OPENDATE", "TERMINATEDDATE",
    "DWELLINGUNITS", "STOPSHUTOFF", "STOPPENALTY", "DUEDATE", "SICCODE", "BUNCHCODE", "SHUTOFFDATE",
    "PIN", "DEFERREDDUEDATE", "LASTNOTICECODE", "LASTNOTICEDATE", "CASHONLY", "NEMLASTTRUEUPDATE",
    "NEMNEXTTRUEUPDATE", "ENGINEERNUM", "SERVICEADDRESS3", "UPDATEDATE"
]
df_new = df_new[column_order]

# Add a trailer row with default values
trailer_row = pd.DataFrame([["TRAILER"] + [""] * (len(df_new.columns) - 1)], columns=df_new.columns)
df_new = pd.concat([df_new, trailer_row], ignore_index=True)

# Define output path for the CSV file
output_path = os.path.join(os.path.dirname(list(file_paths.values())[0]), 'STAGE_BILLING_ACCT.csv')
df_new.to_csv(output_path, index=False, header=True, quoting=csv.QUOTE_NONE, escapechar='\\')
print(f"CSV file saved at {output_path}")
