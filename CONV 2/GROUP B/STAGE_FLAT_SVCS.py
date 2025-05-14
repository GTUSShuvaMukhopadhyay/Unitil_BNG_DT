# CONV1 - STAGE_FLAT_SVCS.py
# STAGE_FLAT_SVCS.py


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
    "ZDM_PREMDETAILS":  r"ZDM_PREMDETAILS.XLSX",
    "ZNC_ACTIVE_CUS": r"ZNC_ACTIVE_CUS.XLSX",
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

# Extract CUSTOMERID from Contract Account where Rate Category is T_ME_SCITR or T_ME_LCITR
if data_sources["ZDM_PREMDETAILS"] is not None:
    df = data_sources["ZDM_PREMDETAILS"]
   
    # Filter for specific Rate Category values
    filtered_df = df[df.iloc[:, 4].isin(["T_ME_SCITR", "T_ME_LCITR"])]  # Column E (Rate Category)
   
    # Extract Contract Account (Column J)
    df_new["CUSTOMERID"] = filtered_df.iloc[:, 7].fillna('').astype(str).apply(lambda x: str(int(float(x))) if x.replace('.', '', 1).isdigit() else x)

# Extract LOCATIONID from ZDM_PREMDETAILS
if data_sources["ZDM_PREMDETAILS"] is not None:
    df_new["LOCATIONID"] = data_sources["ZDM_PREMDETAILS"].iloc[:, 2].fillna('').astype(str)

# Merge with ZNC_ACTIVE_CUS based on LOCATIONID to fetch INITSERVICEDATE
if data_sources["ZNC_ACTIVE_CUS"] is not None:
    active_customer_df = data_sources["ZNC_ACTIVE_CUS"]
    
    # Extract LOCATIONID and INITSERVICEDATE from ZNC_ACTIVE_CUS
    active_customer_df["LOCATIONID"] = active_customer_df.iloc[:, 1].fillna('').astype(str)  # Assuming LOCATIONID is in column 0
    active_customer_df["INITSERVICEDATE"] = active_customer_df.iloc[:, 7].fillna('').astype(str)  # Assuming INITSERVICEDATE is in column 7

    # Debugging print statements
    print("Unique LOCATIONIDs in df_new:")
    print(df_new["LOCATIONID"].unique())  # Check the unique LOCATIONIDs in df_new
    
    print("Unique LOCATIONIDs in active_customer_df:")
    print(active_customer_df["LOCATIONID"].unique())  # Check the unique LOCATIONIDs in active_customer_df
    
    # Merge the dataframes on LOCATIONID to bring INITSERVICEDATE into df_new
    merged_df = pd.merge(df_new, active_customer_df[['LOCATIONID', 'INITSERVICEDATE']], on='LOCATIONID', how='left')

    # Update df_new with merged data
    df_new = merged_df

    # Remove records where INITSERVICEDATE is blank or null
    df_new = df_new[df_new["INITSERVICEDATE"].notna() & (df_new["INITSERVICEDATE"] != '')]

    # Debugging: Check if INITSERVICEDATE was populated
    print("Rows where INITSERVICEDATE is NaT (Not a Time):")
    print(df_new[df_new["INITSERVICEDATE"].isna()])  # Check rows where the date is still missing
    
    # Convert INITSERVICEDATE to datetime and format as 'YYYY-MM-DD'
    df_new["INITSERVICEDATE"] = pd.to_datetime(df_new["INITSERVICEDATE"], errors='coerce').dt.strftime('%Y-%m-%d')

# Check the columns before reordering
print("Columns in df_new before reordering:")
print(df_new.columns)

# Assign hardcoded values
df_new["APPLICATION"] = "2"
df_new["SEQNO"] = "1"
df_new["SERVICENO"] = ""
df_new["ITEMCODE"] = "16"
df_new["SERVICESTATUS"] = "0"
df_new["BILLINGSTARTDATE"] = " "
df_new["BILLINGSTOPDATE"] = " "
df_new["BILLINGDRIVERRATE"] = "2506"
df_new["BILLINGFLATRATE"] = "8211"
df_new["SALESREVENUECLASS"] = "8211"
df_new["NUMBEROFITEMS"] = "1"
df_new["SERIALNUMBER"] = " "
df_new["COMMENTS"] = " "
df_new["RECEPTACLENO"] = " "
df_new["ITEMMAKE"] = " "
df_new["ITEMTYPE"] = " "
df_new["ITEMMODEL"] = " "
df_new["UPDATEDATE"] = " "

# Function to wrap values in double quotes, but leave blanks and NaN as they are
def custom_quote(val):
    """Wraps all values in quotes except for blank or NaN ones."""
    if pd.isna(val) or val == "" or val == " ":
        return ''  # Return an empty string for NaN or blank fields
    return f'"{val}"'  # Wrap other values in double quotes

# Apply custom_quote function to all columns
df_new = df_new.fillna('')

# Apply selective quoting
def selective_custom_quote(val, column_name):
    if column_name in ['APPLICATION', 'SEQNO', 'SERVICENO', 'ITEMCODE', 'SERVICESTATUS', 'BILLINGDRIVERRATE', 'BILLINGFLATRATE', 'SALESREVENUECLASS','NUMBEROFITEMS']:
        return val  # Keep numeric values unquoted
    return '' if val in [None, 'nan', 'NaN', 'NAN'] else custom_quote(val)

df_new = df_new.apply(lambda col: col.map(lambda x: selective_custom_quote(x, col.name)))

# Reorder columns based on user preference
column_order = [
    "CUSTOMERID", "LOCATIONID", "APPLICATION", "SEQNO", "SERVICENO",
    "ITEMCODE", "SERVICESTATUS", "INITSERVICEDATE", "BILLINGSTARTDATE",
    "BILLINGSTOPDATE", "BILLINGDRIVERRATE", "BILLINGFLATRATE", "SALESREVENUECLASS",
    "NUMBEROFITEMS", "SERIALNUMBER", "COMMENTS", "RECEPTACLENO", "ITEMMAKE",
    "ITEMTYPE", "ITEMMODEL", "UPDATEDATE"
]

# Ensure all necessary columns are present before attempting to reorder
missing_columns = [col for col in column_order if col not in df_new.columns]
if missing_columns:
    print(f"Missing columns: {missing_columns}")
else:
    df_new = df_new[column_order]

# Check the columns after reordering
print("Columns in df_new after reordering:")
print(df_new.columns)

# Add a trailer row with default values
trailer_row = pd.DataFrame([["TRAILER"] + [''] * (len(df_new.columns) - 1)], columns=df_new.columns)
df_new = pd.concat([df_new, trailer_row], ignore_index=True)

# Define output path for the CSV file
output_path = os.path.join(os.path.dirname(list(file_paths.values())[0]), 'STAGE_FLAT_SVCS.csv')

# Save to CSV with proper quoting and escape character
df_new.to_csv(output_path, index=False, header=True, quoting=csv.QUOTE_NONE, escapechar='\\')

# Confirmation message
print(f"CSV file saved at {output_path}")
