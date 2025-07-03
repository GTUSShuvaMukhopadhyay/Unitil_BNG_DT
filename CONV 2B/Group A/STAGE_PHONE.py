# CONV1 - STAGE_PHONE.py
# STAGE_PHONE.py
 
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

# File path (Update accordingly)
file_path = r"C:\Users\US82783\OneDrive - Grant Thornton Advisors LLC\Desktop\python\CONV 2B _ 2nd run\DATA SOURCES\ZCAMPAIGN.XLSX"
 
# Read the Excel file and load the specific sheet
df = pd.read_excel(file_path, sheet_name='Sheet1', engine='openpyxl')
 
# Initialize df_new using relevant columns
df_new = pd.DataFrame()
 
# Extract relevant columns safely (Adjust column names if necessary)
df_new['CUSTOMERID'] = df.iloc[:, 1].fillna('').apply(lambda x: str(int(x)) if isinstance(x, (int, float)) else str(x)).str.slice(0, 15)
#df_new['PHONENUMBER'] = df.iloc[:, 8].fillna('').astype('str').str.slice(0, 10)
df_new['PHONENUMBER'] = (
    df.iloc[:, 8]
    .fillna('')
    .apply(lambda x: str(x).split('.')[0])         # Remove decimal if present
    .apply(lambda x: ''.join(filter(str.isdigit, x)))  # Remove any non-digits just in case
    .apply(lambda x: x[-10:] if len(x) > 10 else x)    # Keep only last 10 digits
)
# Ensure PHONETYPE and PHONEEXT are numeric
df_new['PHONETYPE'] = 1  # Default value as numeric
 
# Set PHONEEXT to be blank
df_new['PHONEEXT'] = ""  # Blank value for PHONEEXT
 
# Set PRIORITY to be 1
df_new['PRIORITY'] = 1  # Default value as 1 for PRIORITY
 
# Add additional columns with default blank values
df_new['CONTACT'] = ""
df_new['TITLE'] = ""
df_new['UPDATEDATE'] = ""
 
# Remove rows with blank phone numbers
df_new = df_new[df_new['PHONENUMBER'].str.strip() != '']
 
# Ensure CUSTOMERID, PHONENUMBER, and PHONETYPE are not null (required for the PK)
df_new = df_new.dropna(subset=['CUSTOMERID', 'PHONENUMBER', 'PHONETYPE'])
 
# Remove duplicates based on the combination of CUSTOMERID, PHONENUMBER, and PHONETYPE
df_new = df_new.drop_duplicates(subset=['CUSTOMERID', 'PHONENUMBER', 'PHONETYPE'], keep='first')
 
# Add trailer row properly
trailer_row = pd.DataFrame([['TRAILER'] + [''] * (len(df_new.columns) - 1)], columns=df_new.columns)
df_new = pd.concat([df_new, trailer_row], ignore_index=True)
 
# Reorder columns to match the desired order
df_new = df_new[['CUSTOMERID', 'PHONENUMBER', 'PHONETYPE', 'PHONEEXT', 'CONTACT', 'TITLE', 'PRIORITY', 'UPDATEDATE']]
 
# Function to format CSV output
def custom_quote(val):
    """Returns value wrapped in quotes if not numeric, else returns as is."""
    if isinstance(val, (int, float)):  # Do not quote numeric values
        return val
    elif isinstance(val, str) and val.strip():  # If non-empty string, wrap in quotes
        return f'"{val}"'
    return val  # If empty, return as is
 
# Apply quoting function only to non-numeric columns
for col in df_new.columns:
    if col not in ['PHONENUMBER', 'PHONETYPE', 'PHONEEXT']:
        df_new[col] = df_new[col].apply(custom_quote)
 
# Define output file path
output_csv = os.path.join(os.path.dirname(file_path), 'STAGE_PHONE.csv')
 
# Save to CSV with proper formatting
df_new.to_csv(output_csv, index=False, header=True, quoting=csv.QUOTE_NONE, escapechar='\\')
 
# Confirmation message
print(f"CSV file saved at {output_csv}")