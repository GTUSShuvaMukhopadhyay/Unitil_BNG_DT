# CONV1 - STAGE_EMAIL.py
# STAGE_EMAIL.py
 
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
 
# Assign CUSTOMERID, ensuring it is properly formatted (removes .0 from numbers but keeps trailing zeros intact)
df_new['CUSTOMERID'] = df.iloc[:, 1].fillna('').apply(lambda x: str(int(x)) if isinstance(x, (int, float)) and x == int(x) else str(x))
 
# Extract EMAILADDRESS, ensuring it does not exceed 254 characters
df_new['EMAILADDRESS'] = df.iloc[:, 9].fillna('').astype(str).str.slice(0, 254)
 
# Add EMAILCODE and PRIORITY as numeric (no quotes)
df_new['EMAILCODE'] = 1  # Numeric value, no quotes
df_new['PRIORITY'] = 1    # Numeric value, no quotes
 
# Add additional columns with empty values
df_new['UPDATEDATE'] = ""  # Use "" instead of " "
 
# Replace empty strings with NaN in CUSTOMERID, EMAILADDRESS, EMAILCODE columns
df_new['CUSTOMERID'].replace("", pd.NA, inplace=True)
df_new['EMAILADDRESS'].replace("", pd.NA, inplace=True)
df_new['EMAILCODE'].replace("", pd.NA, inplace=True)
 
# Remove rows where CUSTOMERID, EMAILADDRESS, or EMAILCODE are NaN (empty)
df_new = df_new.dropna(subset=['CUSTOMERID', 'EMAILADDRESS', 'EMAILCODE'], how='any')
 
# Function to wrap values in double quotes for non-numeric columns
def quote_wrap(val, column_name):
    """Wraps non-empty values in quotes, leaving empty values unquoted, except for numeric columns."""
    if column_name in ['EMAILCODE', 'PRIORITY']:  # Do not quote numeric columns
        return val
    return f'"{val}"' if val not in ["", None] else val
 
# Apply quoting function to all columns, pass column name for logic
df_new = df_new.apply(lambda col: col.apply(lambda val: quote_wrap(val, col.name)))
 
# Remove duplicates based on CUSTOMERID and EMAILADDRESS columns
df_new = df_new.drop_duplicates(subset=['CUSTOMERID', 'EMAILADDRESS'])
 
# Add trailer row with correct format (TRAILER followed by empty values)
trailer_row = pd.DataFrame([['TRAILER'] + [''] * (len(df_new.columns) - 1)], columns=df_new.columns)
df_new = pd.concat([df_new, trailer_row], ignore_index=True)
 
# Define output file path
output_csv = os.path.join(os.path.dirname(file_path), 'STAGE_EMAIL.csv')
 
# Save to CSV with correct formatting (escaping special characters)
df_new.to_csv(output_csv, index=False, header=True, quoting=csv.QUOTE_NONE, escapechar='\\')
 
# Confirmation message
print(f"CSV file saved at {output_csv}")
 