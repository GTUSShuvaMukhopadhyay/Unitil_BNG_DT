# STAGE_UNBILLED_READINGS.py
# 
# Created: 05202025
# This script extracts meter reading data from multiple source files
# and prepares it for export to the target system.

import pandas as pd
import os
import csv
import re
from datetime import datetime

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

# Define file paths - update these paths as needed
file_paths = {
    "ZDM_PREMDETAILS": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_UNBILLED_READINGS\ZDM_PREMDETAILS.XLSX",
    "EABL1": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_UNBILLED_READINGS\EABL 01012015 to 12312019.XLSX",
    "EABL2": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_UNBILLED_READINGS\EABL 01012020 to 03272025.XLSX",
}

# Load the data from each spreadsheet
data_sources = {}
for name, path in file_paths.items():
    try:
        data_sources[name] = pd.read_excel(path, sheet_name="Sheet1", engine="openpyxl")
        print(f"Successfully loaded {name} with {len(data_sources[name])} rows")
    except Exception as e:
        data_sources[name] = None
        print(f"Error loading {name}: {e}")

# Combine EABL1 and EABL2 into a single EABL dataframe
if data_sources["EABL1"] is not None and data_sources["EABL2"] is not None:
    data_sources["EABL"] = pd.concat([data_sources["EABL1"], data_sources["EABL2"]], ignore_index=True)
    print(f"Combined EABL data with {len(data_sources['EABL'])} total rows")
elif data_sources["EABL1"] is not None:
    data_sources["EABL"] = data_sources["EABL1"]
    print("Using only EABL1 data")
elif data_sources["EABL2"] is not None:
    data_sources["EABL"] = data_sources["EABL2"]
    print("Using only EABL2 data")
else:
    data_sources["EABL"] = None
    print("Warning: No EABL data available")

# Print column names to verify data structure
if data_sources["ZDM_PREMDETAILS"] is not None:
    print("\nZDM_PREMDETAILS columns:")
    for i, col_name in enumerate(data_sources["ZDM_PREMDETAILS"].columns):
        print(f"Column {i}: {col_name}")

if data_sources["EABL"] is not None:
    print("\nEABL columns:")
    for i, col_name in enumerate(data_sources["EABL"].columns):
        print(f"Column {i}: {col_name}")

# Initialize df_new as an empty DataFrame
df_new = pd.DataFrame()

# Process the data if both sources are available
if data_sources["ZDM_PREMDETAILS"] is not None and data_sources["EABL"] is not None:
    # Create a copy of the data sources for processing
    zdm_df = data_sources["ZDM_PREMDETAILS"].copy()
    eabl_df = data_sources["EABL"].copy()
    
    # Rename columns to match their description (for clarity in code)
    zdm_df.columns = [
        'MRU', 'Contract_Object', 'Premise', 'Installation', 
        'Rate_Category', 'Device_Location', 'Location', 'Business_Partner', 
        'Customer_Name', 'Contract_Account', 'Leg_Contract_Account', 'CA_ADID',
        'EBill', 'Contract', 'ADID', 'Phone_Number', 'Manufacturer', 'Material',
        'Serial_Number', 'Install_Date', 'Construction_Year', 'Meter_Size',
        'Pressure_Factor', 'ERT_Material', 'ERT_Serial_Number', 'ERT_Install_Date',
        'Service_Address', 'Zip_Code', 'Premise_Type', 'Tax_Jurisdiction'
    ]
    
    eabl_df.columns = [
        'MR_unit', 'MT', 'RR', 'Installat', 'Schd_MRD', 'Sched_BD', 
        'Device', 'Internal_MR_Doc_ID', 'Predecimal', 'UMR', 'RS'
    ]
    
    print(f"Number of rows in ZDM_PREMDETAILS: {len(zdm_df)}")
    print(f"Number of rows in EABL: {len(eabl_df)}")
    
    # Convert dates in EABL to datetime for comparison
    eabl_df['Schd_MRD'] = pd.to_datetime(eabl_df['Schd_MRD'], errors='coerce')
    
    # Drop rows with invalid dates
    eabl_df = eabl_df.dropna(subset=['Schd_MRD'])
    print(f"Number of rows in EABL after removing invalid dates: {len(eabl_df)}")
    
    # Create installation to predecimal and date mappings
    print("Creating mappings from installation to readings and dates...")
    
    # Dictionary to store current (most recent) readings and dates
    installation_to_curr_reading = {}
    installation_to_curr_date = {}
    
    # Dictionary to store previous (second most recent) readings and dates
    installation_to_prev_reading = {}
    installation_to_prev_date = {}
    
    # Group EABL data by installation ID
    for installation, group in eabl_df.groupby('Installat'):
        # Sort by date in descending order
        sorted_group = group.sort_values('Schd_MRD', ascending=False)
        
        # Current reading and date (most recent)
        if not sorted_group.empty:
            curr_row = sorted_group.iloc[0]
            installation_to_curr_reading[installation] = curr_row['Predecimal']
            installation_to_curr_date[installation] = curr_row['Schd_MRD'].strftime('%Y-%m-%d')
            
            # Previous reading and date (second most recent)
            if len(sorted_group) > 1:
                prev_row = sorted_group.iloc[1]
                installation_to_prev_reading[installation] = prev_row['Predecimal']
                installation_to_prev_date[installation] = prev_row['Schd_MRD'].strftime('%Y-%m-%d')
            else:
                # If there's only one record, set previous to None
                installation_to_prev_reading[installation] = None
                installation_to_prev_date[installation] = None
    
    print(f"Created mappings for {len(installation_to_curr_reading)} installations")
    
    # Create a mapping from installation to meter info from ZDM_PREMDETAILS
    installation_to_meter = {}
    installation_to_pressure_factor = {}
    
    for i, row in zdm_df.iterrows():
        installation = str(row['Installation']).strip()
        if installation:
            # Get the meter number from ZDM_PREMDETAILS
            # We will map this later to the joined data
            meter_number = str(row['Device_Location']).strip()
            pressure_factor = row['Pressure_Factor']
            
            installation_to_meter[installation] = meter_number
            installation_to_pressure_factor[installation] = pressure_factor
    
    print(f"Created meter mappings for {len(installation_to_meter)} installations")
    
    # Create a list of all unique installations from EABL
    all_installations = eabl_df['Installat'].unique()
    print(f"Found {len(all_installations)} unique installations in EABL")
    
    # Create rows in the output dataframe for each installation
    for installation in all_installations:
        if installation and str(installation).strip():
            # Create a new row
            new_row = {
                # Map CUSTOMERID from ZDM_PREMDETAILS (Business Partner)
                'CUSTOMERID': '',  # Will be populated based on the installation lookup
                
                # Map LOCATIONID from ZDM_PREMDETAILS (Premise)
                'LOCATIONID': '',  # Will be populated based on the installation lookup
                
                # Hardcoded values
                'APPLICATION': '5',
                'METERREGISTER': '1',
                'READINGCODE': '2',
                'READINGTYPE': '0',
                'UNITOFMEASURE': 'CF',
                'READERID': '',
                'UPDATEDATE': '',
                
                # Installation ID for reference
                'INSTALLATION_ID': installation,
                
                # METERNUMBER from the mapping
                'METERNUMBER': installation_to_meter.get(installation, ''),
                
                # Read dates
                'CURRREADDATE': installation_to_curr_date.get(installation, ''),
                'PREVREADDATE': installation_to_prev_date.get(installation, ''),
                
                # Readings
                'CURRREADING': installation_to_curr_reading.get(installation, ''),
                'PREVREADING': installation_to_prev_reading.get(installation, ''),
                
                # Meter multiplier
                'METERMULTIPLIER': installation_to_pressure_factor.get(installation, ''),
            }
            
            # Add the row to the dataframe
            df_new = pd.concat([df_new, pd.DataFrame([new_row])], ignore_index=True)



    
    print(f"Created {len(df_new)} rows in the output dataframe")
    
    # Calculate RAWUSAGE as CURRREADING - PREVREADING
    df_new['RAWUSAGE'] = df_new.apply(
        lambda row: float(row['CURRREADING']) - float(row['PREVREADING']) 
                   if pd.notna(row['CURRREADING']) and pd.notna(row['PREVREADING']) 
                   else '', 
        axis=1
    )
    
    # Calculate BILLINGUSAGE as RAWUSAGE * METERMULTIPLIER
    df_new['BILLINGUSAGE'] = df_new.apply(
        lambda row: float(row['RAWUSAGE']) * float(row['METERMULTIPLIER']) 
                   if pd.notna(row['RAWUSAGE']) and pd.notna(row['METERMULTIPLIER']) 
                   and row['RAWUSAGE'] != '' and row['METERMULTIPLIER'] != '' 
                   else '', 
        axis=1
    )
    
    # Now populate CUSTOMERID and LOCATIONID from ZDM_PREMDETAILS
    for i, row in zdm_df.iterrows():
        installation = str(row['Installation']).strip()
        if installation:
            # Find rows in df_new with this installation
            mask = df_new['INSTALLATION_ID'] == installation
            
            # Update CUSTOMERID (Business Partner)
            df_new.loc[mask, 'CUSTOMERID'] = str(row['Business_Partner']).strip()
            
            # Update LOCATIONID (Premise)
            df_new.loc[mask, 'LOCATIONID'] = str(row['Premise']).strip()
    
    # Count how many records have populated CUSTOMERID and LOCATIONID
    customerid_count = sum(df_new['CUSTOMERID'] != '')
    locationid_count = sum(df_new['LOCATIONID'] != '')
    print(f"Populated CUSTOMERID for {customerid_count} rows")
    print(f"Populated LOCATIONID for {locationid_count} rows")
    
    # Drop the temporary INSTALLATION_ID column used for joining
    df_new = df_new.drop('INSTALLATION_ID', axis=1)

# Function to wrap values in double quotes, but leave blanks and NaN as they are
def custom_quote(val):
    """Wraps all values in quotes except for blank or NaN ones."""
    if pd.isna(val) or val == "" or val == " ":
        return ''  # Return an empty string for NaN or blank fields
    return f'"{val}"'  # Wrap other values in double quotes

# Apply selective quoting
def selective_custom_quote(val, column_name):
    if column_name in ['APPLICATION', 'METERREGISTER', 'READINGCODE', 'READINGTYPE', 'RAWUSAGE', 'METERMULTIPLIER', 'BILLINGUSAGE']:
        return val  # Keep numeric values unquoted
    return '' if pd.isna(val) or val in ['nan', 'NaN', 'NAN'] else custom_quote(val)

# Apply custom_quote function to all columns
df_new = df_new.fillna('')

# Apply selective quoting to each column
for col in df_new.columns:
    df_new[col] = df_new[col].apply(lambda x: selective_custom_quote(x, col))

# Reorder columns
column_order = [
    "CUSTOMERID", "LOCATIONID", "APPLICATION", "METERNUMBER", "METERREGISTER",
    "READINGCODE", "READINGTYPE", "CURRREADDATE", "PREVREADDATE", "CURRREADING",
    "PREVREADING", "UNITOFMEASURE", "RAWUSAGE", "METERMULTIPLIER", "BILLINGUSAGE",
    "READERID", "UPDATEDATE"
]

df_new = df_new[column_order]

# Add a trailer row with default values
trailer_row = pd.DataFrame([["TRAILER"] + [''] * (len(df_new.columns) - 1)], columns=df_new.columns)
df_new = pd.concat([df_new, trailer_row], ignore_index=True)

# Define output path for the CSV file
output_path = os.path.join(os.path.dirname(list(file_paths.values())[0]), 'STAGE_UNBILLED_READINGS.csv')

# Save to CSV with proper quoting and escape character
df_new.to_csv(output_path, index=False, header=True, quoting=csv.QUOTE_NONE, escapechar='\\')

# Confirmation message
print(f"CSV file saved at {output_path}")
print(f"Total records exported: {len(df_new) - 1}")  # Subtract 1 to account for trailer row