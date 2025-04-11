# CONV1 - STAGE_DEPOSITS.py
# STAGE_DEPOSITS.py
 
#Issue is LOCATIONID:
#Issue has been resolved 1018am 03/24/2025
 
import pandas as pd
import os
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
    "FPD2": r"C:\Users\US97684\Downloads\Deposits\FPD2.XLSX",
    "ZDM_PREMDETAILS": r"C:\Users\US97684\Downloads\Deposits\ZDM_PREMDETAILS.XLSX",
    "ZMECON1": r"C:\Users\US97684\Downloads\Deposits\ZMECON 01012015 to 12312020.xlsx",
    "ZMECON2": r"C:\Users\US97684\Downloads\Deposits\ZMECON 01012021 to 02132025.xlsx",
    "DFKKOP1": r"C:\Users\US97684\Downloads\Deposits\DFKKOP 01012019 to 12312019.XLSX",
    "DFKKOP2": r"C:\Users\US97684\Downloads\Deposits\DFKKOP 01012020 to 12312020.XLSX",
    "DFKKOP3": r"C:\Users\US97684\Downloads\Deposits\DFKKOP 01012021 to 12312021.XLSX",
    "DFKKOP4": r"C:\Users\US97684\Downloads\Deposits\Dfkkop 01012022 to 12312022.XLSX",
    "DFKKOP5": r"C:\Users\US97684\Downloads\Deposits\Dfkkop 01012023 to 12312023.XLSX",
    "DFKKOP6": r"C:\Users\US97684\Downloads\Deposits\DFKKOP 01012024 to 02132025.XLSX",
}

# Load the data from each spreadsheet
data_sources = {}
for name, path in file_paths.items():
    try:
        data_sources[name] = pd.read_excel(path, sheet_name="Sheet1", engine="openpyxl")
    except Exception as e:
        data_sources[name] = None
        print(f"Error loading {name}: {e}")


data_sources["ZMECON"] = pd.concat([data_sources["ZMECON1"], data_sources["ZMECON2"]], ignore_index=True)
data_sources["DFKKOPA"] = pd.concat([data_sources["DFKKOP1"], data_sources["DFKKOP2"], data_sources["DFKKOP3"],
                                     data_sources["DFKKOP4"], data_sources["DFKKOP5"], data_sources["DFKKOP6"]], ignore_index=True)

data_sources["DFKKOP"] = data_sources["DFKKOPA"][(data_sources["DFKKOPA"].iloc[:, 4] == "0025") & (data_sources["DFKKOPA"].iloc[:, 5] == "0010")]

# Initialize df_new as an empty DataFrame
df_new = pd.DataFrame()
 
# Extract Business Partner from FPD2 (Column A = iloc[:, 1])
if data_sources["FPD2"] is not None:
    df_new["CUSTOMERID"] = data_sources["FPD2"].iloc[:, 0].apply(
        lambda x: str(int(x)) if pd.notna(x) and isinstance(x, (int, float)) else str(x)
    ).str.slice(0, 15)


if data_sources["ZDM_PREMDETAILS"] is not None:
    def map_locationid_to_customerid(customerid):  # Convert customerid to integer to remove leading zeros if present
        customerid_int = int(customerid)
 
        # Convert iloc(7) values to integer for comparison, handling NaN values
        matching_rows = data_sources["ZDM_PREMDETAILS"].dropna(subset=[data_sources["ZDM_PREMDETAILS"].columns[7]])
        # Convert the column 7 values to integers (removing decimals if any)
        matching_rows.iloc[:, 7] = matching_rows.iloc[:, 7].astype(float).astype(int)
        # Now, make sure customerid_int is treated as an integer and compare with the customerid in ZDM_PREMDETAILS
        matching_row = matching_rows[matching_rows.iloc[:, 7] == customerid_int]
        if not matching_row.empty:
            # Extract LOCATIONID from iloc(2)
            locationid = matching_row.iloc[0, 2]
            return str(int(locationid))  # Assuming iloc(2) corresponds to LOCATIONID
        return None

# Apply mapping to get LOCATIONID based on CUSTOMERID
df_new["LOCATIONID"] = df_new["CUSTOMERID"].apply(map_locationid_to_customerid)


# Create DEPOSITSTATUS based on 'Description of Security Deposit Status' (Column K in FPD2)
if data_sources["FPD2"] is not None:
    df_new["DEPOSITSTATUS"] = data_sources["FPD2"].iloc[:, 10].apply(lambda x: 2 if x == "Paid" else (90 if x == "Request" else 0))
 
# Extract DEPOSITDATE from FPD2
if data_sources["FPD2"] is not None:
    df_new["DEPOSITDATE"] = pd.to_datetime(data_sources["FPD2"].iloc[:, 4], errors='coerce').dt.strftime('%Y-%m-%d')
 
# Extract DEPOSITAMOUNT from FPD2
if data_sources["FPD2"] is not None:
    df_new["DEPOSITAMOUNT"] = pd.to_numeric(data_sources["FPD2"].iloc[:, 8], errors='coerce').fillna(0)
 

# Function to get DEPOSITINTERESTCALCDATE based on CUSTOMERID from merged DFKKOP
if data_sources["DFKKOP"] is not None:
    def get_deposit_interest_calcd_date1(customerid):
        # Ensure CUSTOMERID is treated as a string (same format as DFKKOP's CUSTOMERID)
        customerid_str = str(customerid).zfill(10)
 
        # Filter records for the given CUSTOMERID in the merged DFKKOP DataFrame
        matching_rows = data_sources["DFKKOP"][data_sources["DFKKOP"].iloc[:, 1].astype(str).str.zfill(7) == customerid_str]
 
        # If matching rows exist, get the value from iloc(11)
        if not matching_rows.empty:
            return matching_rows.iloc[0, 11]  # Assuming iloc(11) corresponds to the DEPOSITINTERESTCALCDATE
 
        return None  # Return None if no matching records
 
    # Apply the function to set DEPOSITINTERESTCALCDATE
    df_new["DEPOSITINTERESTCALCDATE"] = df_new["CUSTOMERID"].apply(get_deposit_interest_calcd_date1)
 
# Function to calculate DEPOSITREFUNDMONTHS based on the matching record in ZMECON
def calculate_refund_months(customerid):
    # Ensure CUSTOMERID is treated as a string (same format as ZMECON's CUSTOMERID)
    customerid_str = str(customerid).zfill(7)
    
    # Filter records for the given CUSTOMERID in ZMECON
    matching_rows = data_sources["ZMECON"][data_sources["ZMECON"].iloc[:, 0].astype(str).str.zfill(7) == customerid_str]
    
    # If matching rows exist, check the value in iloc[:, 24]
    if not matching_rows.empty:
        # Assuming iloc[:, 24] corresponds to the column you want to check for "RES"
        zmecon_status = matching_rows.iloc[0, 24]  # iloc(24) corresponds to the desired column
        # Return 12 if status is "RES", else 24
        return 12 if zmecon_status.strip() == "RES" else 24
    
    # If no matching rows in ZMECON, return 24 (default)
    return 24

# Apply the function to calculate DEPOSITREFUNDMONTHS based on CUSTOMERID
df_new["DEPOSITREFUNDMONTHS"] = df_new["CUSTOMERID"].apply(calculate_refund_months)
 
# Assign hardcoded values
df_new["APPLICATION"] = "5"
df_new["DEPOSITKIND"] = "CASH"
if data_sources["ZMECON"] is not None:
    def check_deposit_billed_flag(customerid, deposit_date):
        # Ensure CUSTOMERID is treated as a string (same format as ZMECON's CUSTOMERID)
        customerid_str = str(customerid).zfill(7)
 
        # Filter records for the given CUSTOMERID in ZMECON
        matching_rows = data_sources["ZMECON"][data_sources["ZMECON"].iloc[:, 0].astype(str).str.zfill(7) == customerid_str]
 
        # If matching rows exist, compare DEPOSITDATE (iloc(22) in ZMECON)
        if not matching_rows.empty:
            # Convert iloc(22) to a datetime object for comparison
            zmecon_date = pd.to_datetime(matching_rows.iloc[0, 22], errors='coerce').strftime('%Y-%m-%d')
            if pd.to_datetime(zmecon_date) > pd.to_datetime(deposit_date):
                return "Y"
        return "N"
 
    # Apply the function to set DEPOSITBILLEDFLAG
    df_new["DEPOSITBILLEDFLAG"] = df_new.apply(
        lambda row: check_deposit_billed_flag(row["CUSTOMERID"], row["DEPOSITDATE"]),
        axis=1
    )
df_new["DEPOSITACCRUEDINTEREST"] = ""
df_new["UPDATEDATE"] = " "
 
# Drop records where LOCATIONID is blank (either NaN or an empty string)
df_new = df_new[df_new['LOCATIONID'].notna() & (df_new['LOCATIONID'] != '')]
 
# Function to wrap values in double quotes, but leave blanks and NaN as they are
def custom_quote(val):
    if pd.isna(val) or val == "" or val == " ":
        return ''
    return f'"{val}"'
 
def selective_custom_quote(val, column_name):
    if column_name in ['APPLICATION','DEPOSITSTATUS','DEPOSITKIND','DEPOSITAMOUNT','DEPOSITACCRUEDINTEREST','DEPOSITREFUNDMONTHS']:
        return val
    return '' if val in [None, 'nan', 'NaN', 'NAN'] else custom_quote(val)
 
df_new = df_new.fillna('')
df_new = df_new.apply(lambda col: col.map(lambda x: selective_custom_quote(x, col.name)))
 
# Reorder columns
column_order = [
    "CUSTOMERID", "LOCATIONID", "APPLICATION", "DEPOSITSTATUS", "DEPOSITKIND",
    "DEPOSITDATE", "DEPOSITAMOUNT", "DEPOSITBILLEDFLAG", "DEPOSITACCRUEDINTEREST",
    "DEPOSITINTERESTCALCDATE", "DEPOSITREFUNDMONTHS", "UPDATEDATE"
]
df_new = df_new[column_order]
 
# Add trailer row
trailer_row = pd.DataFrame([["TRAILER"] + [''] * (len(df_new.columns) - 1)], columns=df_new.columns)
df_new = pd.concat([df_new, trailer_row], ignore_index=True)
 
# Save to CSV
output_path = os.path.join(os.path.dirname(list(file_paths.values())[0]), 'STAGE_DEPOSITS.csv')
df_new.to_csv(output_path, index=False, header=True, quoting=csv.QUOTE_NONE, escapechar='\\')
print(f"CSV file saved at {output_path}")