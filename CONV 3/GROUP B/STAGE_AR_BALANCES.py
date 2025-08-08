# STAGE_AR_BALANCES.py

import pandas as pd 
import os
import csv
from datetime import datetime

# Define file paths
print("Defining file paths...")

file_path1 = r"c:\Users\GTUSER1\Documents\CONV 3\ZDM_PREMDETAILS.XLSX"
file_path2 = r"c:\Users\GTUSER1\Documents\CONV 3\Configuration 13.xlsx"

# ZMECON file paths
zmecon_file_path3 = r"c:\Users\GTUSER1\Documents\CONV 3\ZMECON 08012019 to 08012025.xlsx"


# DFKKOP file paths
dfkkop_file_pathA = r"c:\Users\GTUSER1\Documents\CONV 3\DFKKOP\DFKKOP 01012019 to 12312019.XLSX"
dfkkop_file_pathB = r"c:\Users\GTUSER1\Documents\CONV 3\DFKKOP\DFKKOP 01012020 to 12312020.XLSX"
dfkkop_file_pathC = r"c:\Users\GTUSER1\Documents\CONV 3\DFKKOP\DFKKOP 01012021 to 12312021.XLSX"
dfkkop_file_pathD = r"c:\Users\GTUSER1\Documents\CONV 3\DFKKOP\DFKKOP 01012022 to 12312022.XLSX"
dfkkop_file_pathE = r"c:\Users\GTUSER1\Documents\CONV 3\DFKKOP\DFKKOP 01012023 to 12312023.XLSX"
dfkkop_file_pathF = r"c:\Users\GTUSER1\Documents\CONV 3\DFKKOP\DFKKOP 01012024 to 12312024.XLSX"
dfkkop_file_pathG = r"c:\Users\GTUSER1\Documents\CONV 3\DFKKOP\DFKKOP 01012025 to 12312025.XLSX"


# Load Excel files
print("Reading ZDM_PREMDETAILS...")
df_Prem = pd.read_excel(file_path1, sheet_name='Sheet1', engine='openpyxl')
print(f"✅ Loaded ZDM_PREMDETAILS with {len(df_Prem)} rows")

print("Reading Configuration (RateCode)...")
df_Config = pd.read_excel(file_path2, sheet_name='RateCode', engine='openpyxl')
print(f"✅ Loaded Configuration RateCode with {len(df_Config)} rows")

# Load all ZMECON files
print("Reading all ZMECON data files...")
zmecon_files = [zmecon_file_path3]
df_zmecon_list = []

for i, fp in enumerate(zmecon_files):
    df = pd.read_excel(fp, sheet_name='ZMECON', engine='openpyxl')
    df_zmecon_list.append(df)
    print(f"✅ Loaded ZMECON file {i+1} with {len(df)} rows from: {fp}")

# Combine all ZMECON datasets
print("Combining all ZMECON datasets...")
df_ZMECON = pd.concat(df_zmecon_list, ignore_index=True)
print(f"✅ Combined ZMECON dataset created with {len(df_ZMECON)} total rows")

# Load all DFKKOP files
print("Reading all DFKKOP data files...")
dfkkop_files = [dfkkop_file_pathA, dfkkop_file_pathB, dfkkop_file_pathC, dfkkop_file_pathD, dfkkop_file_pathE, 
                dfkkop_file_pathF, dfkkop_file_pathG]
df_dfkkop_list = []

for i, fp in enumerate(dfkkop_files):
    df = pd.read_excel(fp, sheet_name='Sheet1', engine='openpyxl')
    df_dfkkop_list.append(df)
    print(f"✅ Loaded DFKKOP file {i+1} with {len(df)} rows from: {fp}")

# Combine DFKKOP datasets
print("Combining DFKKOP datasets...")
df_combined = pd.concat(df_dfkkop_list, ignore_index=True)
print(f"✅ Combined DFKKOP dataset created with {len(df_combined)} total rows")

# Filter records where Column K (index 10) is NaN
print("Filtering DFKKOP records with NaN in column index 10...")
df_filtered = df_combined[df_combined.iloc[:, 10].isna()]
print(f"✅ Filtered DFKKOP dataset with {len(df_filtered)} rows having NaN in column 11 (K)")
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
# Add this debugging section right after your loop starts to test a specific case
# Let's debug the LOCATIONID lookup for contract account 1078626

test_contract_account = 1078626  # The one we know should have LOCATIONID 7000066372

# Debug: Let's check what contract accounts exist in df_filtered to find a real example
print(f"\n=== DEBUGGING LOCATIONID LOOKUP ===")

# Check what contract accounts are actually in df_filtered (the ones we're processing)
print("Sample contract accounts in df_filtered (first 10):")
sample_accounts = df_filtered.iloc[:10, 0].tolist()
print(sample_accounts)
print(f"Data type in df_filtered column 0: {df_filtered.iloc[:, 0].dtype}")

# Check data types in our lookup tables
print(f"\nData type of df_Prem column 9: {df_Prem.iloc[:, 9].dtype}")
print(f"Data type of df_ZMECON column 2: {df_ZMECON.iloc[:, 2].dtype}")

# Check if any of the sample accounts exist in our lookup tables
test_contract_account = sample_accounts[0] if sample_accounts else None
print(f"\n--- Testing lookup for contract account {test_contract_account} ---")

if test_contract_account is not None:
    # Test df_Prem lookup with proper data type conversion
    try:
        # Convert df_Prem column 9 to int to match comparison
        prem_lookup = df_Prem[df_Prem.iloc[:, 9].fillna(0).astype(int) == int(test_contract_account)]
        print(f"df_Prem lookup result: {len(prem_lookup)} rows found")
        if not prem_lookup.empty:
            print(f"LOCATIONID from df_Prem (column 2): {prem_lookup.iloc[0, 2]}")
    except Exception as e:
        print(f"df_Prem lookup failed: {e}")

    # Test df_ZMECON lookup
    try:
        zmecon_lookup = df_ZMECON[df_ZMECON.iloc[:, 2] == int(test_contract_account)]
        print(f"df_ZMECON lookup result: {len(zmecon_lookup)} rows found")
        if not zmecon_lookup.empty:
            print(f"LOCATIONID from df_ZMECON (column 25): {zmecon_lookup.iloc[0, 25]}")
    except Exception as e:
        print(f"df_ZMECON lookup failed: {e}")

print("=== END DEBUG ===\n")

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

    location_id = 0  # Initialize with default value

    # Convert contract account to int for consistent comparison
    try:
        contract_account = int(row.iloc[0])
        
        # Try df_Prem first (convert float64 to int for comparison)
        Location_id_from_zdmprem = df_Prem[df_Prem.iloc[:, 9].fillna(0).astype(int) == contract_account]
        
        if not Location_id_from_zdmprem.empty:
            location_id = Location_id_from_zdmprem.iloc[0, 2]
        else:
            # Try df_ZMECON (already int64, so direct comparison works)
            location_id_from_zmecon = df_ZMECON[df_ZMECON.iloc[:, 2] == contract_account]
            if not location_id_from_zmecon.empty:
                location_id = location_id_from_zmecon.iloc[0, 25]
                
    except (ValueError, TypeError):
        # If conversion to int fails, keep location_id as 0
        location_id = 0
    # Create a new row in df_new
    balance_date = pd.to_datetime(row.iloc[11], errors='coerce').date()
    # Skip adding the row if BALANCEDATE is NaT (Not a Time)
    if not pd.isna(balance_date):
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


# SOLUTION: Fix missing LOCATIONID values by loading your reference file

print("\n" + "="*60)
print("FIXING MISSING LOCATIONID VALUES")
print("="*60)

# Step 1: Load your reference file with correct CUSTOMERID → LOCATIONID mappings
# ⚠️ CHANGE THIS PATH TO YOUR ACTUAL REFERENCE FILE ⚠️
# reference_file_path = r"C:\Users\us85360\Desktop\CONV 2 B - STAGE_AR_BALANCES\LocationID List.xlsx"

# print(f"Loading reference file: {reference_file_path}")

# try:
    # Load the reference file (adjust sheet name if needed)
#    df_reference = pd.read_excel(reference_file_path, sheet_name=0, engine='openpyxl')  # Uses first sheet#
 #   print(f"✅ Loaded reference file with {len(df_reference)} rows")
    
    # Show the column names to verify structure
   # print(f"Reference file columns: {list(df_reference.columns)}")
    
    # Create mapping from reference file (assuming columns are CUSTOMERID and LOCATIONID)
   # customerid_to_locationid = {}
  #  reference_mappings = 0
    
    # # Using column positions (A=0, B=1)
    # customer_col = 0  # Change if your column name is different
    # location_col = 1  # Change if your column name is different
    
    # If using column positions instead of names (e.g., A=0, B=1):
    # customer_col = 0
    # location_col = 1
    
    # for index, row in df_reference.iterrows():
        # try:
            # if isinstance(customer_col, str):
                # customer_id = int(row[customer_col]) if not pd.isna(row[customer_col]) else None
                # location_id = int(row[location_col]) if not pd.isna(row[location_col]) else None
            # else:
                # customer_id = int(row.iloc[customer_col]) if not pd.isna(row.iloc[customer_col]) else None
                # location_id = int(row.iloc[location_col]) if not pd.isna(row.iloc[location_col]) else None
            
            # if customer_id and location_id:
               # customerid_to_locationid[customer_id] = location_id
               # reference_mappings += 1
        # except (ValueError, TypeError):
            # continue
    
    # print(f"✅ Created {reference_mappings} mappings from reference file")
    
# except FileNotFoundError:
    # print(f"❌ Reference file not found: {reference_file_path}")
    # print("Please update the reference_file_path variable with the correct path to your file.")
    # customerid_to_locationid = {}
# except Exception as e:
    # print(f"❌ Error loading reference file: {e}")
   #  customerid_to_locationid = {}
customerid_to_locationid = {}
# Step 2: Supplement with data from existing sources (if needed)
print("Adding supplementary mappings from df_Prem and df_ZMECON...")

supplementary_count = 0

# Method 2: From df_Prem (only add if not in reference file)
for index, row in df_Prem.iterrows():
    try:
        customer_id = int(row.iloc[9]) if not pd.isna(row.iloc[9]) else None
        location_id = int(row.iloc[2]) if not pd.isna(row.iloc[2]) else None
        
        if customer_id and location_id and customer_id not in customerid_to_locationid:
            customerid_to_locationid[customer_id] = location_id
            supplementary_count += 1
    except (ValueError, TypeError):
        continue

# Method 3: From df_ZMECON (only add if not in reference file or df_Prem)
for index, row in df_ZMECON.iterrows():
    try:
        customer_id = int(row.iloc[2]) if not pd.isna(row.iloc[2]) else None
        location_id = int(row.iloc[25]) if not pd.isna(row.iloc[25]) else None
        
        if customer_id and location_id and customer_id not in customerid_to_locationid:
            customerid_to_locationid[customer_id] = location_id
            supplementary_count += 1
    except (ValueError, TypeError):
        continue

print(f"✅ Added {supplementary_count} supplementary mappings from existing sources")
print(f"✅ Total unique CUSTOMERID → LOCATIONID mappings: {len(customerid_to_locationid)}")

# Step 2: Identify and fix missing LOCATIONID values in your processed data
missing_before = 0
fixed_count = 0
still_missing = []

print("\nScanning for missing LOCATIONID values...")

# Go through df_new and fix missing LOCATIONID values
for index, row in df_new.iterrows():
    if row.get('CUSTOMERID') != 'TRAILER':  # Skip trailer row
        customer_id = row['CUSTOMERID']
        current_location_id = row['LOCATIONID']
        
        # If LOCATIONID is missing (0) or blank
        if current_location_id == 0 or pd.isna(current_location_id):
            missing_before += 1
            
            # Try to find the correct LOCATIONID
            if customer_id in customerid_to_locationid:
                correct_location_id = customerid_to_locationid[customer_id]
                df_new.at[index, 'LOCATIONID'] = correct_location_id
                fixed_count += 1
                print(f"  ✅ Fixed: CUSTOMERID {customer_id} → LOCATIONID {correct_location_id}")
            else:
                still_missing.append(customer_id)

print(f"\n📊 SUMMARY:")
print(f"   Missing LOCATIONID values found: {missing_before}")
print(f"   Successfully fixed: {fixed_count}")
print(f"   Still missing: {len(set(still_missing))}")

# Step 3: Report on remaining missing values
if still_missing:
    unique_still_missing = list(set(still_missing))
    print(f"\n❌ CUSTOMERIDs still missing LOCATIONID:")
    for customer_id in sorted(unique_still_missing[:20]):  # Show first 20
        print(f"   CUSTOMERID: {customer_id}")
    
    if len(unique_still_missing) > 20:
        print(f"   ... and {len(unique_still_missing) - 20} more")
    
#    # Save list of still missing to file
#    still_missing_df = pd.DataFrame({
#        'CUSTOMERID': sorted(unique_still_missing),
#        'LOCATIONID': 'STILL MISSING - CHECK DATA SOURCES'
#    })
#    still_missing_path = r"C:\Users\us85360\Desktop\CONV 2 B - STAGE_AR_BALANCES\STILL_MISSING_LOCATIONID.xlsx"
#    still_missing_df.to_excel(still_missing_path, index=False)
#    print(f"   📄 Saved remaining missing list to: {still_missing_path}")

else:
    print(f"\n🎉 All LOCATIONID values have been successfully populated!")

# Step 4: Save the complete mapping reference for future use
# mapping_df = pd.DataFrame([
#    {'CUSTOMERID': k, 'LOCATIONID': v} 
#    for k, v in sorted(customerid_to_locationid.items())
# ])

# mapping_path = r"C:\Users\us85360\Desktop\CONV 2 B - STAGE_AR_BALANCES\COMPLETE_CUSTOMERID_LOCATIONID_MAPPING.xlsx"
# mapping_df.to_excel(mapping_path, index=False)
# print(f"\n📄 Complete mapping reference saved to: {mapping_path}")
# print(f"   (Contains {len(mapping_df)} CUSTOMERID → LOCATIONID pairs)")

print("="*60 + "\n")




# Add a trailer row with default values
trailer_row = pd.DataFrame([['TRAILER'] + [''] * (len(df_new.columns) - 1)],
                           columns=df_new.columns)
 
# Append the trailer row to the DataFrame
df_new = pd.concat([df_new, trailer_row], ignore_index=True)
 
# Save to CSV with custom quoting and escape character
output_path = r"C:\Users\GTUSER1\Documents\CONV 3\output\Group B\STAGE_AR_BALANCES.csv"
 
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
