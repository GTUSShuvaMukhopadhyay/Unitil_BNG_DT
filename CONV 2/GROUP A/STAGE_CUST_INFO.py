# CONV1 - STAGE_CUST_INFO.py
# STAGE_CUST_INFO.py
 
# NOTES: Update formatting
 
import pandas as pd
import os
import re
import csv  # Import the correct CSV module

# Add the parent directory to sys.path
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
import Conversion_Utils as cu 

cu.print_checklist()
 
# Define input file path
file_path = r"MA1_Extract.xlsx"
 
# Read the Excel file and load the specific sheet
#df = pd.read_excel(file_path, sheet_name='Sheet1', engine='openpyxl')
df=cu.get_file("mail")
 
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
df_new['FULLNAME'] = df_new['FULLNAME'].apply( cu.cleanse_string, 50 )
 
# Column 3: Column E (index 4)
df_new['FIRSTNAME'] = df.iloc[:, 4]
df_new['FIRSTNAME'] = df_new['FIRSTNAME'].apply( cu.cleanse_string, 25 )
 
df_new['MIDDLENAME'] = " "
 
# Function to generate LASTNAME
def generate_lastname(row):
    last_name = str(row.iloc[5]).strip() if not pd.isna(row.iloc[5]) else ""
    name_1 = str(row.iloc[2]).strip() if not pd.isna(row.iloc[2]) else ""
    return last_name if last_name else name_1
 
# Apply transformation logic for LASTNAME
df_new['LASTNAME'] = df.apply(generate_lastname, axis=1)
df_new['LASTNAME'] = df_new['LASTNAME'].apply( cu.cleanse_string, 50 )
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
cu.log_info("Wrote CSV file successfully at: " + output_path + " with " + str(len(df_new)) + " rows")