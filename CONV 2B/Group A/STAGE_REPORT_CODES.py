# CONV1 - STAGE_REPORT_CODES.py
# STAGE_REPORT_CODES.py

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
    "ZDM_PREMDETAILS": r"C:\Users\US82783\OneDrive - Grant Thornton Advisors LLC\Desktop\python\CONV 2B _ 2nd run\DATA SOURCES\ZDM_PREMDETAILS.XLSX",
    
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


# Extract CUSTOMERID from Contract Account where Rate Category is T_ME_LIHEA
# NEED to update for G as well
if data_sources["ZDM_PREMDETAILS"] is not None:
    df = data_sources["ZDM_PREMDETAILS"]
    
    # Filter for specific Rate Category values
    filtered_df = df[df.iloc[:, 4].isin(["T_ME_LIHEA"])]  # Column E (Rate Category)
    
    # Extract Contract Account (Column J)
    df_new["CUSTOMERID"] = filtered_df.iloc[:,7].fillna('').astype(str).apply(lambda x: str(int(float(x))) if x.replace('.', '', 1).isdigit() else x).str.slice(0, 15)
    
# Extract LOCATIONID from ZDM_PREMDETAILS
if data_sources["ZDM_PREMDETAILS"] is not None:
    df_new["LOCATIONID"] = data_sources["ZDM_PREMDETAILS"].iloc[:, 2].fillna('').astype(str)

"""
Need to validate this is the correct column

# Extract CUSTOMERID from ZDM_PREMDETAILS
if data_sources["ZDM_PREMDETAILS"] is not None:
    df_new["CUSTOMERID"] = data_sources["ZDM_PREMDETAILS"].iloc[:, 7].fillna('').apply(lambda x: str(int(x)) if isinstance(x, (int, float)) else str(x)).str.slice(0, 15)
"""
    
# Assign hardcoded values
df_new["TAXYEAR"] = " "
df_new["REPORTCODEFIELD"] = "15"
df_new["REPORTCODEVALUE"] = "18"
df_new["ACTIVEDATE"] = "2025-03-25"
df_new["UPDATEDATE"] = " "


# Function to wrap values in double quotes, but leave blanks and NaN as they are
def custom_quote(val):
    """Wraps all values in quotes except for blank or NaN ones."""
    # If the value is NaN, None, or blank, leave it empty
    if pd.isna(val) or val == "" or val == " ":
        return ''  # Return an empty string for NaN or blank fields
    return f'"{val}"'  # Wrap other values in double quotes
 
# Apply custom_quote function to all columns
df_new = df_new.fillna('')
 

def selective_custom_quote(val, column_name):
    if column_name in ['REPORTCODEFIELD','REPORTCODEVALUE']:
        return val  # Keep numeric values unquoted
    return '' if val in [None, 'nan', 'NaN', 'NAN'] else custom_quote(val)
 
df_new = df_new.apply(lambda col: col.map(lambda x: selective_custom_quote(x, col.name)))


# In case we need to do this
# Remove any records missing CUSTOMERID
df_new = df_new[df_new['CUSTOMERID'].notna() & (df_new['CUSTOMERID'] != '')]

# Drop duplicate records based on CUSTOMERID, LOCATIONID, and REPORTCODEFIELD
df_new = df_new.drop_duplicates(subset=['CUSTOMERID','LOCATIONID','REPORTCODEFIELD'], keep='first')


# Reorder columns based on user preference
column_order = [
    "TAXYEAR", "CUSTOMERID", "LOCATIONID", "REPORTCODEFIELD", "REPORTCODEVALUE", "ACTIVEDATE", "UPDATEDATE"
]
df_new = df_new[column_order]


# Add a trailer row with default values
trailer_row = pd.DataFrame([["TRAILER"] + [''] * (len(df_new.columns) - 1)], columns=df_new.columns)
df_new = pd.concat([df_new, trailer_row], ignore_index=True)


# Define output path for the CSV file
output_path = os.path.join(os.path.dirname(list(file_paths.values())[0]), 'STAGE_REPORT_CODES.csv')
 
# Save to CSV with proper quoting and escape character
df_new.to_csv(output_path, index=False, header=True, quoting=csv.QUOTE_NONE, escapechar='\\')
 
# Confirmation message
print(f"CSV file saved at {output_path}")