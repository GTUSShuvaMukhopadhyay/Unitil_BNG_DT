import pandas as pd 
import os
import csv
from datetime import datetime

# Define file paths
print("Defining file paths...")

file_path1 = r"C:\Users\US82783\OneDrive - Grant Thornton Advisors LLC\Desktop\python\CONV 2B _ 2nd run\DATA SOURCES\ZDM_PREMDETAILS.XLSX"
file_path2 = r"C:\Users\US82783\OneDrive - Grant Thornton Advisors LLC\Desktop\python\CONV 2B _ 2nd run\DATA SOURCES\Configuration 13.xlsx"
file_path3 = r"C:\Users\US82783\OneDrive - Grant Thornton Advisors LLC\Desktop\python\CONV 2B _ 2nd run\DATA SOURCES\ZMECON 010115 to 12312020.xlsx"
file_path4 = r"C:\Users\US82783\OneDrive - Grant Thornton Advisors LLC\Desktop\python\CONV 2B _ 2nd run\DATA SOURCES\ZMECON 010121 to 061425.xlsx"
file_pathA = r"C:\Users\US82783\OneDrive - Grant Thornton Advisors LLC\Desktop\python\CONV 2B _ 2nd run\DATA SOURCES\DFKKOP\DFKKOP 01012015 to 12312015.XLSX"
file_pathB = r"C:\Users\US82783\OneDrive - Grant Thornton Advisors LLC\Desktop\python\CONV 2B _ 2nd run\DATA SOURCES\DFKKOP\DFKKOP 01012016 TO 12312016.XLSX"
file_pathC = r"C:\Users\US82783\OneDrive - Grant Thornton Advisors LLC\Desktop\python\CONV 2B _ 2nd run\DATA SOURCES\DFKKOP\DFKKOP 01012017 TO 12312017.XLSX"
file_pathD = r"C:\Users\US82783\OneDrive - Grant Thornton Advisors LLC\Desktop\python\CONV 2B _ 2nd run\DATA SOURCES\DFKKOP\DFKKOP 01012018 TO 12312018.XLSX"
file_pathE = r"C:\Users\US82783\OneDrive - Grant Thornton Advisors LLC\Desktop\python\CONV 2B _ 2nd run\DATA SOURCES\DFKKOP\DFKKOP 01012019 TO 12312019.XLSX"
file_pathF = r"C:\Users\US82783\OneDrive - Grant Thornton Advisors LLC\Desktop\python\CONV 2B _ 2nd run\DATA SOURCES\DFKKOP\DFKKOP 01012020 TO 12312020.XLSX"
file_pathG = r"C:\Users\US82783\OneDrive - Grant Thornton Advisors LLC\Desktop\python\CONV 2B _ 2nd run\DATA SOURCES\DFKKOP\DFKKOP 01012021 TO 12312021.XLSX"
file_pathH = r"C:\Users\US82783\OneDrive - Grant Thornton Advisors LLC\Desktop\python\CONV 2B _ 2nd run\DATA SOURCES\DFKKOP\DFKKOP 01012022 TO 12312022.XLSX"
file_pathI = r"C:\Users\US82783\OneDrive - Grant Thornton Advisors LLC\Desktop\python\CONV 2B _ 2nd run\DATA SOURCES\DFKKOP\DFKKOP 01012023 TO 12312023.XLSX"
file_path  = r"C:\Users\US82783\OneDrive - Grant Thornton Advisors LLC\Desktop\python\CONV 2B _ 2nd run\DATA SOURCES\DFKKOP\DFKKOP 01012025 to 06172025.XLSX"

# Load Excel files
print("Reading ZDM_PREMDETAILS...")
df_Prem = pd.read_excel(file_path1, sheet_name='Sheet1', engine='openpyxl')
print(f"✅ Loaded ZDM_PREMDETAILS with {len(df_Prem)} rows")

print("Reading Configuration (RateCode)...")
df_Config = pd.read_excel(file_path2, sheet_name='RateCode', engine='openpyxl')
print(f"✅ Loaded Configuration RateCode with {len(df_Config)} rows")

print("Reading ZMECON 2015 to 2020...")
df_ZMECON1 = pd.read_excel(file_path3, sheet_name='ZMECON', engine='openpyxl')
print(f"✅ Loaded ZMECON 2015-2020 with {len(df_ZMECON1)} rows")

print("Reading ZMECON 2021 to 03272025...")
df_ZMECON2 = pd.read_excel(file_path4, sheet_name='ZMECON', engine='openpyxl')
print(f"✅ Loaded ZMECON 2021-2025 with {len(df_ZMECON2)} rows")

# Load all DFKKOP files
print("Reading all DFKKOP data files...")

df_list = []
for i, fp in enumerate([file_path, file_pathA, file_pathB, file_pathC, file_pathD, file_pathE, file_pathF, file_pathG, file_pathH, file_pathI]):
    df = pd.read_excel(fp, sheet_name='Sheet1', engine='openpyxl')
    df_list.append(df)
    print(f"✅ Loaded DFKKOP file {i+1} with {len(df)} rows from: {fp}")

# Combine DFKKOP datasets
print("Combining DFKKOP datasets...")
df_combined = pd.concat(df_list, ignore_index=True)
print(f"✅ Combined DFKKOP dataset created with {len(df_combined)} total rows")

# Filter records where Column K (index 10) is NaN
print("Filtering DFKKOP records with NaN in column index 10...")
df_filtered = df_combined[df_combined.iloc[:, 10].isna()]
print(f"✅ Filtered DFKKOP dataset with {len(df_filtered)} rows having NaN in column 11 (K)")

# Combine ZMECON datasets
print("Combining all ZMECON datasets...")
df_ZMECON = pd.concat([df_ZMECON1, df_ZMECON2], ignore_index=True)
print(f"✅ Combined ZMECON dataset created with {len(df_ZMECON)} total rows")
 
# Initialize an empty list to store the rows that will be added to df_new
rows_to_add = []
 
# Define valid combinations for APPLICATION - data from ARBalance sample AR to enQuesta Mapping for Apllication column These are Telemetering codes
valid_combinations = [
    ('0015', '0300'),
    ('0015', '0301'),
    ('0100', '0510'),
    ('0100', '0511'),
    ('0200', '0510'),
    ('0200', '0511')
]
 
# Function to get rate from premise (df_Prem)
def get_rate_from_premise(contaccount):
    prem_row = df_Prem[df_Prem.iloc[:, 9] == contaccount]
    if not prem_row.empty:
        t_values = [val for val in prem_row.iloc[:, 4] if str(val).startswith("T_")]
        if t_values:
            return t_values[0]  # return the first 'T_' value found
    return None
 
# Function to get ratepremise from df_ZMECON (this is a premise number)
def get_rate_from_zmacon(contaccount):
    zmecon_row = df_ZMECON[df_ZMECON.iloc[:, 2] == contaccount]
    if not zmecon_row.empty:
        return zmecon_row.iloc[0, 24]  # return the value from the 25th column of df_ZMECON
    return None

def get_ratepremise_from_zmacon(contaccount):
    zmecon_row = df_ZMECON[df_ZMECON.iloc[:, 2] == contaccount]
    if not zmecon_row.empty:
        return zmecon_row.iloc[0, 25]  # return the value from the 25th column of df_ZMECON
    return None

# Function to get rateCategoryPremise using the premise number from df_Prem
def get_rate_usingpremise_from_Premise(premise_number):
    prem1_row = df_Prem[df_Prem.iloc[:, 2] == premise_number]
    if not prem1_row.empty:
        t_values = [val for val in prem1_row.iloc[:, 4] if str(val).startswith("T_")]
        if t_values:
            return t_values[0]  # return the first 'T_' value found
    return None
#

# Function to prioritize 'T_' values only, searching in both methods
def get_t_value_only(contaccount):
    rate_from_premise = get_rate_from_premise(contaccount)
    if rate_from_premise:
        return rate_from_premise  # If 'T_' value is found, return it
    else:
        rate = get_rate_from_zmacon(contaccount)
        if rate:
            return rate.strip()
        else:
            premise_number = get_ratepremise_from_zmacon(contaccount)
            if premise_number:
                rate_from_usingpremise = get_rate_usingpremise_from_Premise(premise_number)
                if rate_from_usingpremise:
                    return rate_from_usingpremise  # Return 'T_' if found
            return None  # ✅ This line had the colon error

# Function to get iloc3 from Config DataFrame
def get_iloc3_from_config(value1, value2, value3):
    def normalize(val):
        try:
            return str(int(float(val))).zfill(4)  # e.g., 100 -> "0100"
        except (ValueError, TypeError):
            return str(val).strip().upper()       # for strings like "RES", "LCI"

    # Normalize input values
    value1 = normalize(value1)
    value2 = normalize(value2)
    value3 = normalize(value3)

    # Normalize columns in df_Config
    df_Config['Rate Category Norm'] = df_Config.iloc[:, 0].astype(str).str.strip().str.upper()
    df_Config['MTrans Norm'] = df_Config.iloc[:, 1].apply(normalize)
    df_Config['STrans Norm'] = df_Config.iloc[:, 2].apply(normalize)

    matching_row = df_Config[
        (df_Config['Rate Category Norm'] == value1) &
        (df_Config['MTrans Norm'] == value2) &
        (df_Config['STrans Norm'] == value3)
    ]

    if not matching_row.empty:
        return matching_row.iloc[0, 3]  # Assuming BBC is at index 3

    print(f"❌ No match for ({value1}, {value2}, {value3})")
    return None
 
# Loop through each row in df_filtered to process and add to df_new
for index, row in df_filtered.iterrows():
    # Find the matching LOCATIONID from df_ZMECON (Column 25)
    Location_id_from_zdmprem = df_Prem[df_Prem.iloc[:, 9] == row.iloc[0]]

    if not Location_id_from_zdmprem.empty:
        location_id = Location_id_from_zdmprem.iloc[0, 2]
    else:
        location_id_from_zmecon = df_ZMECON[df_ZMECON.iloc[:, 2] == row.iloc[0]]  # Match based on Contract Account
        if not location_id_from_zmecon.empty:
            location_id = location_id_from_zmecon.iloc[0, 25]

    # Create a new row in df_new
    balance_date = pd.to_datetime(row.iloc[11], errors='coerce').date()
    # Skip rows where BALANCEDATE is NaT (Not a Time)
    if pd.isna(balance_date):
        continue
    new_row = {
        'TAXYEAR': " ",
        'CUSTOMERID': int(row.iloc[1]) if not pd.isna(row.iloc[1]) else 0,  # Handle NaN for CUSTOMERID
        'LOCATIONID': int(location_id) if str(location_id).strip().isdigit() else 0,  # Handle NaN for LOCATIONID
        'APPLICATION': "2" if (row.iloc[4], row.iloc[5]) in valid_combinations else "5",
        'BALANCEDATE': balance_date,
        'BALANCEAMOUNT': round(row.iloc[6] - row.iloc[14], 2),  # Calculating BALANCEAMOUNT
        'RECEIVABLECODE': int(get_iloc3_from_config(get_t_value_only(row.iloc[0]), row.iloc[4], row.iloc[5])) if get_iloc3_from_config(get_t_value_only(row.iloc[0]), row.iloc[4], row.iloc[5]) is not None else 8098,
        'UPDATEDATE': ""
    }
    # Add the new row to the list
    rows_to_add.append(new_row)
    # If the tax value is greater than zero, add another row with modified values
    if row.iloc[14] != 0:
        tax_row = new_row.copy()  # Copy the current row
        # Set the BALANCEAMOUNT to the tax value and RECEIVABLECODE to 8444
        tax_row['BALANCEAMOUNT'] = round(row.iloc[14], 2)
        tax_row['RECEIVABLECODE'] = int(8444)
        # Add the modified tax row to the list
        rows_to_add.append(tax_row)
 
# Convert the list of rows to a DataFrame
df_new = pd.DataFrame(rows_to_add)
 
# Add a trailer row with default values
trailer_row = pd.DataFrame([['TRAILER'] + [''] * (len(df_new.columns) - 1)],
                           columns=df_new.columns)
 
# Append the trailer row to the DataFrame
df_new = pd.concat([df_new, trailer_row], ignore_index=True)
 
# Save to CSV with custom quoting and escape character
output_path = r"C:\Users\US82783\OneDrive - Grant Thornton Advisors LLC\Desktop\python\CONV 2B _ 2nd run\Group B\STAGE_AR_BALANCES.csv"
 
numeric_columns = [
    'TAXYEAR', 'APPLICATION', 'BALANCEAMOUNT', 'RECEIVABLECODE'
]
 
# Function to apply custom quoting for certain columns
def custom_quote(val, column):
    if column in numeric_columns:
        return val  # No quotes for numeric fields
    return f'"{val}"' if val not in ["", None] else val
 
df_new = df_new.apply(lambda col: col.apply(lambda val: custom_quote(val, col.name)))
df_new.to_csv(output_path, index=False, header=True, quoting=csv.QUOTE_NONE)
print(f"✅ STAGE_AR_BALANCES.csv is created with {len(df_new)} total rows")
print(f"File successfully saved to: {output_path}")