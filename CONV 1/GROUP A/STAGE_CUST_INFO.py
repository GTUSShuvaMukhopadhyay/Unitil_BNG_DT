# CONV1 - STAGE_CUST_INFO.py
# STAGE_CUST_INFO.py
 
# NOTES: Update formatting
 
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
 
# Define input file path
file_path = r"C:MA1_Extract.xlsx"
 
# Read the Excel file and load the specific sheet
df = pd.read_excel(file_path, sheet_name='Sheet1', engine='openpyxl')
 
# Initialize df_new using relevant columns
df_new = pd.DataFrame().fillna('')
 
# Extract the relevant columns
df_new['CUSTOMERID'] = df.iloc[:, 1].fillna('').apply(lambda x: str(int(x)) if isinstance(x, (int, float)) else str(x)).str.slice(0, 15)
 
# Function to generate FULLNAME
def generate_fullname(row):
    name_1 = str(row.iloc[2]).strip() if not pd.isna(row.iloc[2]) else ""
    first_name = str(row.iloc[4]).strip() if not pd.isna(row.iloc[4]) else ""
    last_name = str(row.iloc[5]).strip() if not pd.isna(row.iloc[5]) else ""
    if name_1:
        return name_1
    return f"{first_name} {last_name}".strip()
 
# Apply transformation logic for FULLNAME
df_new['FULLNAME'] = df.apply(generate_fullname, axis=1)
df_new['FULLNAME'] = df_new['FULLNAME'].str.slice(0, 50)
 
# Column 3: Column E (index 4)
df_new['FIRSTNAME'] = df.iloc[:, 4].astype(str).str.slice(0, 25)
 
df_new['MIDDLENAME'] = " "
 
# Function to generate LASTNAME
def generate_lastname(row):
    last_name = str(row.iloc[5]).strip() if not pd.isna(row.iloc[5]) else ""
    name_1 = str(row.iloc[2]).strip() if not pd.isna(row.iloc[2]) else ""
    return last_name if last_name else name_1
 
# Apply transformation logic for LASTNAME
df_new['LASTNAME'] = df.apply(generate_lastname, axis=1)
df_new['LASTNAME'] = df_new['LASTNAME'].str.slice(0, 50)
df_new['NAMETITLE'] = " "
 
# List of suffixes to check for
suffixes = ["ESQ", "JR", "SR", "II", "III", "IV", "V", "PHD", "MD", "DDS"]
 
df_new['NAMESUFFIX'] = df_new['LASTNAME'].apply(lambda x: next((s for s in suffixes if f", {s}" in x), ""))
df_new['DBA'] = " "
 
# Column 6: MUST BE NUMERIC -  CUSTTYPE
df_new['CUSTTYPE'] = df.iloc[:, 17].map({1: 0, 2: 1}).fillna(0).astype(int)
 
# Column 7: "TBD"
df_new['ACTIVECODE'] = "0"
 
# Additional Columns
df_new['MOTHERMAIDENNAME'] = " "
df_new['EMPLOYERNAME'] = " "
df_new['EMPLOYERPHONE'] = " "
df_new['EMPLOYERPHONEEXT'] = " "
df_new['OTHERIDTYPE1'] = " "
df_new['OTHERIDVALUE1'] = " "
df_new['OTHERIDTYPE2'] = " "
df_new['OTHERIDVALUE2'] = " "
df_new['OTHERIDTYPE3'] = " "
df_new['OTHERIDVALUE3'] = " "
df_new['UPDATEDATE'] = " "
 
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
    if column_name in ['CUSTTYPE', 'ACTIVECODE']:
        return val  # Keep numeric values unquoted
    return '' if val in [None, 'nan', 'NaN', 'NAN'] else custom_quote(val)
 
df_new = df_new.apply(lambda col: col.map(lambda x: selective_custom_quote(x, col.name)))
 
# Drop duplicate records based on CUSTOMERID
df_new = df_new.drop_duplicates(subset='CUSTOMERID', keep='first')
 
# Add a trailer row with default values
trailer_row = pd.DataFrame([["TRAILER"] + [''] * (len(df_new.columns) - 1)], columns=df_new.columns)
 
# Append the trailer row to the DataFrame
df_new = pd.concat([df_new, trailer_row], ignore_index=True)
 
# Define output path for the CSV file
output_path = os.path.join(os.path.dirname(file_path), 'STAGE_CUST_INFO.csv')
 
# Save to CSV with proper quoting and escape character
df_new.to_csv(output_path, index=False, header=True, quoting=csv.QUOTE_NONE, escapechar='\\')
 
# Confirmation message
print(f"CSV file saved at {output_path}")