
# STAGE_MAIL_ADDR.py
 
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
file_path = r"C:\Users\us85360\Downloads\MA1_Extract.xlsx"
 
# Read the Excel file and load the specific sheet
df = pd.read_excel(file_path, sheet_name='Sheet1', engine='openpyxl')
 
# Initialize df_new using relevant columns
df_new = pd.DataFrame().fillna('')

# Extract the relevant columns
df_new['CUSTOMERID'] = df.iloc[:, 1].fillna('').apply(lambda x: str(int(x)) if isinstance(x, (int, float)) else str(x)).str.slice(0, 15)
df_new['ADDRESSSEQ'] = "1"

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
df_new['MAILINGNAME'] = df_new['MAILINGNAME'].str.slice(0, 50)

df_new['INCAREOF'] = df.iloc[:, 6].astype(str).str.slice(0, 35)

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
df_new['COUNTRY'] = "US"
# df_new['POSTALCODE'] = "SM WIP"
df_new['POSTALCODE'] = df.iloc[:, 12].fillna(df.iloc[:, 13])

# df_stage_towns['ZIPCODE'] = df["Zip Code"].astype(str).str.strip().apply(lambda x: f"'0{x.zfill(4)}" if len(x) < 5 else f"'{x}")

df_new['UPDATEDATE'] = ""

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
    if column_name in ['ADDRESSSEQ']:
        return val  # Keep numeric values unquoted
    return '' if val in [None, 'nan', 'NaN', 'NAN'] else custom_quote(val)
 
df_new = df_new.apply(lambda col: col.map(lambda x: selective_custom_quote(x, col.name)))
 
# REVIEW THIS Drop duplicate records based on CUSTOMERID
df_new = df_new.drop_duplicates(subset='CUSTOMERID', keep='first')
 
# Add a trailer row with default values
trailer_row = pd.DataFrame([["TRAILER"] + [''] * (len(df_new.columns) - 1)], columns=df_new.columns)
 
# Append the trailer row to the DataFrame
df_new = pd.concat([df_new, trailer_row], ignore_index=True)
 
# Define output path for the CSV file
output_path = os.path.join(os.path.dirname(file_path), 'STAGE_MAIL_ADDR.csv')
 
# Save to CSV with proper quoting and escape character
df_new.to_csv(output_path, index=False, header=True, quoting=csv.QUOTE_NONE, escapechar='\\')
 
# Confirmation message
print(f"CSV file saved at {output_path}")