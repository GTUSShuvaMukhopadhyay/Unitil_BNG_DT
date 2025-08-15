# STAGE_TAXEXEMPT.py 
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
file_path = r"c:\Users\GTUSER1\Documents\CONV 3\Tax Exemption Details.xlsx"
 
# Read the Excel file and load the specific sheet
df = pd.read_excel(file_path, sheet_name='Tax Exemption Details', engine='openpyxl')
 
# Initialize df_new using relevant columns
df_new = pd.DataFrame()
 

df_new['CUSTOMERID'] = df.iloc[:, 3].fillna('').apply(lambda x: str(int(x)) if isinstance(x, (int, float)) else str(x)).str.slice(0, 15)
df_new["LOCATIONID"] = df.iloc[:, 4].fillna('').apply(lambda x: str(int(x)) if isinstance(x, (int, float)) else str(x)).str.slice(0, 15)
df_new["APPLICATION"] = "5"
df_new["EXEMPTTYPE"] = "D"
df_new["EFFECTIVEDATE"] = pd.to_datetime(df.iloc[:, 14], errors='coerce').dt.strftime('%Y-%m-%d')
df_new["EXPIRATIONDATE"] = pd.to_datetime(df.iloc[:, 15], errors='coerce').dt.strftime('%Y-%m-%d')

df_new["CERTNUMBER"] = " "
df_new["FEDERALID"] = " "
df_new["STATEID"] = " "
df_new["PERCENTTAXABLE"] =  df.iloc[:, 12]
df_new["ITEMORSERV"] = "S"
df_new["SERVICENUMBER"] = "1"
df_new["MISCTAXKIND"] = "0"
df_new["MISCTAXCODE"] = df.iloc[:, 9]
df_new["UPDATEDATE"] = " "

# --------------------------
# Format values with proper quoting
# --------------------------
print("\nFormatting field values...")
def custom_quote(val):
    if pd.isna(val) or val in ["", " "]:
        return ""
    return f'"{val}"'
    
def selective_custom_quote(val, column_name):
    if column_name in ['APPLICATION', 'PERCENTTAXABLE', 'SERVICENUMBER', 'MISCTAXKIND', 'MISCTAXCODE',
                       'UPDATEDATE']:
        return val
    return "" if val in [None, 'nan', 'NaN', 'NAN'] else custom_quote(val)
    
df_new = df_new.fillna("")
for col in df_new.columns:
    df_new[col] = df_new[col].apply(lambda x: selective_custom_quote(x, col))

# --------------------------
# Reorder columns based on target format
# --------------------------
column_order = [
    "CUSTOMERID", "LOCATIONID", "APPLICATION", "EXEMPTTYPE", "EFFECTIVEDATE",
    "EXPIRATIONDATE", "CERTNUMBER", "FEDERALID", "STATEID",
    "PERCENTTAXABLE", "ITEMORSERV", "SERVICENUMBER", "MISCTAXKIND", "MISCTAXCODE",
    "UPDATEDATE"
]

# Verify all required columns exist
missing_columns = [col for col in column_order if col not in df_new.columns]
if missing_columns:
    print(f"Warning: Missing required columns: {missing_columns}")
    for col in missing_columns:
        df_new[col] = ""

# Apply column ordering
df_new = df_new[column_order]
print(f"Ordered columns according to target format. Final columns: {len(df_new.columns)}")

# --------------------------
# Add trailer row
# --------------------------
trailer_row = pd.DataFrame([["TRAILER"] + [''] * (len(df_new.columns) - 1)], columns=df_new.columns)
df_new = pd.concat([df_new, trailer_row], ignore_index=True)
print(f"Added trailer row. Final row count: {len(df_new)}")

# --------------------------
# Save to CSV
# --------------------------
output_path = os.path.join(os.path.dirname(file_path), 'STAGE_TAXEXEMPT.csv')
output_path = r"C:\Users\GTUSER1\Documents\CONV 3\output\STAGE_TAXEXEMPT.csv"

df_new.to_csv(output_path, index=False, header=True, quoting=csv.QUOTE_NONE, escapechar='\\')
print(f"CSV file saved at {output_path}")
