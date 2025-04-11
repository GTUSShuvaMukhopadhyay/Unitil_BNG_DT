import pandas as pd 
import os
import csv
from datetime import datetime

# Define the file paths
file_path = r"DFKKOP 01012024 to 02132025.XLSX"
file_path1 = r"ZDM_PREMDETAILS.XLSX"
file_path2 = r"Configuration.xlsx"
file_path3 = r"ZMECON 01012021 to 02132025.xlsx"

file_pathA = r"Dfkkop 01012023 to 12312023.XLSX"
file_pathB = r"Dfkkop 01012022 to 12312022.XLSX"
file_pathC = r"DFKKOP 01012021 to 12312021.XLSX"
file_pathD = r"DFKKOP 01012020 to 12312020.XLSX"
file_pathE = r"DFKKOP 01012019 to 12312019.XLSX"

file_path4 = r"ZMECON 01012015 to 12312020.xlsx"

# Read Excel files
df_Prem = pd.read_excel(file_path1, sheet_name='Sheet1', engine='openpyxl')
df_Config = pd.read_excel(file_path2, sheet_name='RateCode', engine='openpyxl')
df_ZMECON1= pd.read_excel(file_path3, sheet_name='ZMECON', engine='openpyxl')
df_ZMECON2 = pd.read_excel(file_path4, sheet_name='ZMECON 2', engine='openpyxl')

df = pd.read_excel(file_path, sheet_name='Sheet1', engine='openpyxl')
dfA = pd.read_excel(file_pathA, sheet_name='Sheet1', engine='openpyxl')
dfB = pd.read_excel(file_pathB, sheet_name='Sheet1', engine='openpyxl')
dfC = pd.read_excel(file_pathC, sheet_name='Sheet1', engine='openpyxl')
dfD = pd.read_excel(file_pathD, sheet_name='Sheet1', engine='openpyxl')
dfE = pd.read_excel(file_pathE, sheet_name='Sheet1', engine='openpyxl')

# Combine all dataframes into one
df_combined = pd.concat([df, dfA, dfB, dfC, dfD, dfE], ignore_index=True)

# Filter the combined DataFrame where the value in Column K (index 10) is NaN
df_filtered = df_combined[df_combined.iloc[:, 10].isna()]

df_ZMECON = pd.concat([df_ZMECON1, df_ZMECON2], ignore_index=True)

# Initialize an empty list to store the rows that will be added to df_new
rows_to_add = []

# Define valid combinations for APPLICATION - data from ARBalance sample AR to enQuesta Mapping for Apllication column
valid_combinations = [
    ('0015', '0300'),
    ('0015', '0301'),
    ('0100', '0510'),
    ('0100', '0511'),
    ('0200', '0510'),
    ('0200', '0511')
]

# Function to get rate from premise (df_Prem)
def get_rate_from_premise(customer_id):
    prem_row = df_Prem[df_Prem.iloc[:, 9] == customer_id]
    if not prem_row.empty:
        t_values = [val for val in prem_row.iloc[:, 4] if str(val).startswith("T_")]
        if t_values:
            return t_values[0]  # return the first 'T_' value found
    return None

# Function to get ratepremise from df_ZMECON (this is a premise number)
def get_ratepremise_from_zmacon(customer_id):
    zmecon_row = df_ZMECON[df_ZMECON.iloc[:, 2] == customer_id]
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

# Function to prioritize 'T_' values only, searching in both methods
def get_t_value_only(customer_id):
    rate_from_premise = get_rate_from_premise(customer_id)
    if rate_from_premise:
        return rate_from_premise  # If 'T_' value is found, return it
    
    premise_number = get_ratepremise_from_zmacon(customer_id)
    if premise_number:
        rate_from_usingpremise = get_rate_usingpremise_from_Premise(premise_number)
        if rate_from_usingpremise:
            return rate_from_usingpremise  # Return 'T_' if found
    return None

# Function to get iloc3 from Config DataFrame
def get_iloc3_from_config(value1, value2, value3):
    matching_row = df_Config[(df_Config.iloc[:, 0] == value1) & 
                             (df_Config.iloc[:, 1] == value2) & 
                             (df_Config.iloc[:, 2] == value3)]
    if not matching_row.empty:
        return matching_row.iloc[0, 3]
    return None

# Set today's date
today_date = datetime.today().strftime('%Y-%m-%d')

# Loop through each row in df_filtered to process and add to df_new
for index, row in df_filtered.iterrows():
    # Find the matching LOCATIONID from df_ZMECON (Column 25)
    location_id_from_zmecon = df_ZMECON[df_ZMECON.iloc[:, 2] == row.iloc[0]]  # Assuming match is based on customer ID (first column)
    
    if not location_id_from_zmecon.empty:
        location_id = location_id_from_zmecon.iloc[0, 25]  # Use the value from column 25 in ZMECON
    else:
        location_id = row.iloc[0]  # If no match, use the original LOCATIONID from df_filtered
    
    # Create a new row in df_new
    balance_date = pd.to_datetime(row.iloc[11], errors='coerce').date()
    
    # Skip rows where BALANCEDATE is NaT (Not a Time)
    if pd.isna(balance_date):
        continue
    
    new_row = {
        'TAXYEAR': " ",
        'CUSTOMERID': int(row.iloc[1]) if not pd.isna(row.iloc[1]) else 0,  # Handle NaN for CUSTOMERID
        'LOCATIONID': int(location_id) if not pd.isna(location_id) else 0,  # Handle NaN for LOCATIONID
        'APPLICATION': "2" if (row.iloc[4], row.iloc[5]) in valid_combinations else "5",
        'BALANCEDATE': balance_date,
        'BALANCEAMOUNT': round(row.iloc[6] - row.iloc[14], 2),  # Calculating BALANCEAMOUNT
        'RECEIVABLECODE': int(get_iloc3_from_config(get_t_value_only(row.iloc[0]), row.iloc[4], row.iloc[5])) if get_iloc3_from_config(get_t_value_only(row.iloc[0]), row.iloc[4], row.iloc[5]) is not None else 0,
        'UPDATEDATE': today_date
    }
    
    # Add the new row to the list
    rows_to_add.append(new_row)
    
    # If the tax value is greater than zero, add another row with modified values
    if row.iloc[14] > 0:
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
output_path = r"STAGE_AR_BALANCES.csv"

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

print(f"File successfully saved to: {output_path}")
