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
df_new['ACTIVECODE'] = 0
 
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
 
# Drop duplicate records based on CUSTOMERID
df_new = df_new.drop_duplicates(subset='CUSTOMERID', keep='first')
 
# Write the DataFrame to a CSV file
cu.write_csv(df_new, "STAGE_CUST_INFO.csv" )

