
# STAGE_MAIL_ADDR.py
 
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
 
# Read the Excel file and load the specific sheet
df = cu.get_file("mail")
 
# Initialize df_new using relevant columns
df_new = pd.DataFrame().fillna('')

# Extract the relevant columns
df_new['CUSTOMERID'] = df.iloc[:, 1].fillna('').apply(lambda x: str(int(x)) if isinstance(x, (int, float)) else str(x)).str.slice(0, 15)
df_new['ADDRESSSEQ'] = 1

# Function to generate MAILINGNAME
def generate_mailingname(row):
    name_1 = str(row.iloc[2]).strip() if not pd.isna(row.iloc[2]) else ""
    first_name = str(row.iloc[4]).strip() if not pd.isna(row.iloc[4]) else ""
    last_name = str(row.iloc[5]).strip() if not pd.isna(row.iloc[5]) else ""
    if name_1:
        return name_1
    return f"{first_name} {last_name}".strip()
 
# Apply transformation logic for MAILINGNAME
df_new['MAILINGNAME'] = df.apply(generate_mailingname, axis=1)
df_new['MAILINGNAME'] = df_new['MAILINGNAME'].apply(cu.cleanse_string, 50)

df_new['INCAREOF'] = df.iloc[:, 6].apply( cu.cleanse_string, 35)

# Function to generate ADDRESS1 from House No., Street, and PO Box
def generate_address1(row):
    house_no = str(row.iloc[7]).strip() if not pd.isna(row.iloc[7]) else ""
    street = str(row.iloc[8]).strip() if not pd.isna(row.iloc[8]) else ""
    po_box = str(row.iloc[9]).strip() if not pd.isna(row.iloc[9]) else ""

    # Ensure PO Box is treated as a string with proper labeling
    if po_box.isnumeric():
        po_box = f"PO BOX {po_box}"
    
    # Combine non-empty values with a space separator
    address_parts = [part for part in [house_no, street, po_box] if part and part.lower() != 'nan']
    return " ".join(address_parts) if address_parts else "UNKNOWN"

# Apply transformation for ADDRESS1
df_new['ADDRESS1'] = df.apply(generate_address1, axis=1)


# Apply transformation for ADDRESS1
df_new['ADDRESS1'] = df.apply(generate_address1, axis=1)

def split_address(address):
    """
    Splits an address into `ADDRESS1` (street) and `ADDRESS2` (suite/unit info).
    Extracts terms like SUITE, STE, APT, UNIT, ROOM, FL, BLDG followed by a number.
    """
    if not isinstance(address, str) or address.strip() == "":
        return address, ""  # Return original for empty or non-string values

    # Regex pattern to find SUITE, STE, APT, UNIT, ROOM, etc.
    pattern = re.compile(r'\b(SUITE|STE|UNIT|APT|BLDG|FL|ROOM)\s*\d+\b', re.IGNORECASE)

    match = pattern.search(address)

    if match:
        address1 = pattern.sub('', address).strip().rstrip(',')  # Remove the suite/unit part
        address2 = match.group(0)  # Extract the suite/unit part
        return address1, address2
    return address, ""  # If no suite/unit found, return full address as ADDRESS1, and ADDRESS2 as empty

df_new[['ADDRESS1', 'ADDRESS2']] = df_new['ADDRESS1'].apply(lambda x: pd.Series(split_address(x)))

df_new['CITY'] =  df.iloc[:, 10].astype(str).str.slice(0, 24)
df_new['STATE'] = df.iloc[:, 11].astype(str).str.slice(0, 2)
df_new['COUNTRY'] = df.iloc[:, 14].astype(str).str.slice(0, 2)
# df_new['POSTALCODE'] = "SM WIP"
df_new['POSTALCODE'] = df.iloc[:, 12].fillna(df.iloc[:, 13])

# df_stage_towns['ZIPCODE'] = df["Zip Code"].astype(str).str.strip().apply(lambda x: f"'0{x.zfill(4)}" if len(x) < 5 else f"'{x}")

df_new['UPDATEDATE'] = ""

# REVIEW THIS Drop duplicate records based on CUSTOMERID
df_new = df_new.drop_duplicates(subset='CUSTOMERID', keep='first')

# Write the DataFrame to a CSV file
cu.write_csv(df_new, "STAGE_MAIL_ADDR.csv" )
