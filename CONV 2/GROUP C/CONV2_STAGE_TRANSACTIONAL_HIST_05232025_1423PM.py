# CONV2_STAGE_TRANSACTIONAL_HIST_05232025_1423PM.py

import pandas as pd
import os
import csv
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


# Define file paths - include all DFKKOP files
file_paths = {
    # DFKKOP files by year
    "DFKKOP1": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_TRANSACTIONAL_HIST\DFKKOP 01012015 to 12312015 (1).XLSX",
    "DFKKOP2": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_TRANSACTIONAL_HIST\DFKKOP 01012016 to 12312016.XLSX",
    "DFKKOP3": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_TRANSACTIONAL_HIST\DFKKOP 01012017 to 12312017.XLSX",
    "DFKKOP4": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_TRANSACTIONAL_HIST\DFKKOP 01012018 to 12312018.XLSX",
    "DFKKOP5": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_TRANSACTIONAL_HIST\DFKKOP 01012019 to 12312019.XLSX",
    "DFKKOP6": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_TRANSACTIONAL_HIST\DFKKOP 01012020 to 12312020.XLSX",
    "DFKKOP7": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_TRANSACTIONAL_HIST\DFKKOP 01012021 to 12312021.XLSX",
    "DFKKOP8": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_TRANSACTIONAL_HIST\DFKKOP 01012022 to 12312022.XLSX",
    "DFKKOP9": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_TRANSACTIONAL_HIST\DFKKOP 01012023 to 12312023.XLSX",
    "DFKKOP10": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_TRANSACTIONAL_HIST\DFKKOP 01012024 TO 03272025.XLSX",   
    # Other sources
    "EVER": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_TRANSACTIONAL_HIST\EVER.XLSX",
    "ZDM_PREMDETAILS": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_TRANSACTIONAL_HIST\ZDM_PREMDETAILS.XLSX",
}
 
# Initialize data_sources dictionary to hold our data
data_sources = {}
 
# Function to read an Excel file
def read_excel_file(name, path):
    try:
        df = pd.read_excel(path, sheet_name="Sheet1", engine="openpyxl")
        print(f"Successfully loaded {name}: {df.shape[0]} rows, {df.shape[1]} columns")
        return df
    except Exception as e:
        print(f"Error loading {name}: {e}")
        return None

# Load data sources
print("Loading data sources...")
for name, path in file_paths.items():
    data_sources[name] = read_excel_file(name, path)

# Combine all DFKKOP files into a single dataframe
dfkkop_keys = [key for key in data_sources.keys() if key.startswith("DFKKOP")]
if dfkkop_keys:
    dfkkop_dfs = [data_sources[key] for key in dfkkop_keys if data_sources[key] is not None]
    if dfkkop_dfs:
        data_sources["DFKKOP"] = pd.concat(dfkkop_dfs, ignore_index=True)
        print(f"Combined {len(dfkkop_dfs)} DFKKOP files into a single dataframe with {len(data_sources['DFKKOP'])} rows")
        
        # CRITICAL: Standardize MTrans and STrans formatting immediately after combining
        print("Standardizing MTrans and STrans formatting...")
        
        # Standardize STrans to 4-digit format with leading zeros
        data_sources["DFKKOP"]["STrans"] = data_sources["DFKKOP"]["STrans"].apply(
            lambda x: "{:04d}".format(int(x)) if pd.notna(x) and isinstance(x, (int, float)) else str(x) if pd.notna(x) else x
        )
        
        # Standardize MTrans to 4-digit format with leading zeros  
        data_sources["DFKKOP"]["MTrans"] = data_sources["DFKKOP"]["MTrans"].apply(
            lambda x: "{:04d}".format(int(x)) if pd.notna(x) and isinstance(x, (int, float)) else str(x) if pd.notna(x) else x
        )
        
        print("MTrans and STrans formatting standardized to 4-digit format")
        
    else:
        print("Warning: No valid DFKKOP dataframes found to combine")
else:
    print("Warning: No DFKKOP files found in data_sources")


# add this diagnostic codeE (line 85):
# Diagnostic: Check what MTrans/STrans values actually exist in the data
print("\nDiagnostic: Checking MTrans/STrans values in DFKKOP...")
print("Sample MTrans values:", data_sources["DFKKOP"]["MTrans"].head(20).tolist())
print("Sample STrans values:", data_sources["DFKKOP"]["STrans"].head(20).tolist())
print("Unique MTrans values (first 50):", sorted(data_sources["DFKKOP"]["MTrans"].astype(str).unique())[:50])
print("Unique STrans values (first 50):", sorted(data_sources["DFKKOP"]["STrans"].astype(str).unique())[:50])

# Check data types
print(f"\nMTrans dtype: {data_sources['DFKKOP']['MTrans'].dtype}")
print(f"STrans dtype: {data_sources['DFKKOP']['STrans'].dtype}")

# Check for the specific combinations we're looking for
test_combinations = [("0015", "0010"), ("0100", "0002"), ("0200", "0002")]
for mtrans, strans in test_combinations:
    count = len(data_sources["DFKKOP"][
        (data_sources["DFKKOP"]["MTrans"].astype(str).str.strip() == mtrans) & 
        (data_sources["DFKKOP"]["STrans"].astype(str).str.strip() == strans)
    ])
    print(f"Records with MTrans={mtrans}, STrans={strans}: {count}")

# Filter DFKKOP to only include valid MTrans/STrans combinations
print("\nFiltering DFKKOP for valid MTrans/STrans combinations...")


# ADD THIS DIAGNOSTIC CODE HERE (line 114):
# DIAGNOSTIC: Check year distribution BEFORE filtering
print("\n*** DIAGNOSTIC: Year distribution in DFKKOP BEFORE filtering ***")
year_dist_before_filter = pd.to_datetime(data_sources["DFKKOP"]["Doc. Date"], errors='coerce').dt.year.value_counts().sort_index()
print(year_dist_before_filter)

# Check which MTrans/STrans combinations exist by year
print("\nChecking which years have our valid combinations...")
df_temp = data_sources["DFKKOP"].copy()
df_temp['Year'] = pd.to_datetime(df_temp["Doc. Date"], errors='coerce').dt.year
df_temp['MTrans_STrans'] = df_temp['MTrans'].astype(str) + '_' + df_temp['STrans'].astype(str)

# First define the valid combinations for checking - Now as strings
valid_combinations_check = {
    ("0015", "0010"), ("0015", "0020"), ("0015", "0021"), ("0015", "0030"),
    ("0015", "0040"), ("0015", "0070"), ("0015", "0230"), ("0015", "0231"),
    ("0015", "0300"), ("0015", "0370"), ("0015", "0371"), ("0025", "0010"),
    ("0070", "0010"), ("0080", "0005"), ("0080", "0010"), ("0100", "0002"),
    ("0200", "0002"), ("0620", "0010"), ("0630", "0010")
}

# Check specific combinations by year
for year in range(2019, 2026):
    year_data = df_temp[df_temp['Year'] == year]
    if len(year_data) > 0:
        print(f"\nYear {year}: {len(year_data)} total records")
        # Check if any of our valid combinations exist
        valid_in_year = 0
        for mtrans, strans in valid_combinations_check:
            count = len(year_data[(year_data['MTrans'].astype(str).str.strip() == mtrans) & 
                                 (year_data['STrans'].astype(str).str.strip() == strans)])
            if count > 0:
                print(f"  Found {mtrans}/{strans}: {count} records")
                valid_in_year += count
        if valid_in_year == 0:
            print(f"  WARNING: No valid combinations found in {year}!")

# Define the 19 valid combinations - Now both MTrans and STrans as standardized strings
valid_combinations = {
    ("0015", "0010"), ("0015", "0020"), ("0015", "0021"), ("0015", "0030"),
    ("0015", "0040"), ("0015", "0070"), ("0015", "0230"), ("0015", "0231"),
    ("0015", "0300"), ("0015", "0370"), ("0015", "0371"), ("0025", "0010"),
    ("0070", "0010"), ("0080", "0005"), ("0080", "0010"), ("0100", "0002"),
    ("0200", "0002"), ("0620", "0010"), ("0630", "0010")
}

# Store original count
original_count = len(data_sources["DFKKOP"])

# Apply filter - Now much simpler since formats are standardized
def check_valid_combination(row):
    try:
        mtrans = str(row['MTrans']).strip()
        strans = str(row['STrans']).strip()
        
        # Direct string comparison since both are now standardized
        return (mtrans, strans) in valid_combinations
    except:
        return False

data_sources["DFKKOP"] = data_sources["DFKKOP"][
    data_sources["DFKKOP"].apply(check_valid_combination, axis=1)
]
filtered_count = len(data_sources["DFKKOP"])
print(f"Filtered DFKKOP from {original_count:,} to {filtered_count:,} records")
print(f"Reduction: {((original_count - filtered_count) / original_count * 100):.2f}%")

# Show distribution of combinations found
print("\nDistribution of valid combinations found:")
if filtered_count > 0:
    combo_counts = data_sources["DFKKOP"].groupby(['MTrans', 'STrans']).size().sort_values(ascending=False)
    for (mtrans, strans), count in combo_counts.items():
        print(f"  MTrans: {mtrans}, STrans: {strans} -> {count:,} records")
else:
    print("  No valid combinations found!")

# Check if we have any data left after filtering
if len(data_sources["DFKKOP"]) == 0:
    print("\nERROR: No valid MTrans/STrans combinations found in the data!")
    print("The filtering removed all records. Please check the MTrans/STrans values in your data.")
    print("Exiting to prevent further errors...")
    exit(1)



# Check if we have any data left after filtering
if len(data_sources["DFKKOP"]) == 0:
    print("\nERROR: No valid MTrans/STrans combinations found in the data!")
    print("The filtering removed all records. Please check the MTrans/STrans values in your data.")
    print("Exiting to prevent further errors...")
    exit(1)
# CRITICAL FIX: Create df_new from filtered DFKKOP to maintain row alignment
print("\n" + "="*60)
print("CREATING OUTPUT DATAFRAME WITH PROPER ROW ALIGNMENT")
print("="*60)

# Initialize output DataFrame (df_new) directly from filtered DFKKOP
df_new = data_sources["DFKKOP"].copy()
print(f"Created df_new with {len(df_new)} rows from filtered DFKKOP")

# --------------------------
# Extract CUSTOMERID from DFKKOP (BPartner column) - now aligned
# --------------------------
df_new["CUSTOMERID"] = df_new["BPartner"].apply(
    lambda x: str(int(x)) if pd.notna(x) and isinstance(x, (int, float)) else str(x)
).str.slice(0, 15)
print(f"Extracted {len(df_new)} CUSTOMERID values (aligned)")
# --------------------------
# Extract LOCATIONID through direct merge between DFKKOP, EVER and ZDM_PREMDETAILS
# --------------------------
if data_sources.get("ZDM_PREMDETAILS") is not None:
    print("\n" + "="*60)
    print("LOCATIONID EXTRACTION - ENHANCED DEBUGGING")
    print("="*60)
    
    # First, let's understand the data formats
    print("\nAnalyzing Contract Account formats...")
    
    # Get sample Contract Account values from DFKKOP
    dfkkop_ca_sample = df_new["Cont.Account"].dropna().head(10)
    print(f"DFKKOP Contract Account samples:")
    for ca in dfkkop_ca_sample:
        print(f"  '{ca}' (type: {type(ca).__name__}, len: {len(str(ca))})")
    
    # Get sample Contract Account values from ZDM_PREMDETAILS
    zdm_df = data_sources["ZDM_PREMDETAILS"].copy()
    zdm_ca_sample = zdm_df["Contract Account"].dropna().head(10)
    print(f"\nZDM_PREMDETAILS Contract Account samples:")
    for ca in zdm_ca_sample:
        print(f"  '{ca}' (type: {type(ca).__name__}, len: {len(str(ca))})")
    
    # Standardize both sides for matching
    print("\nStandardizing Contract Account formats...")
    
    # For DFKKOP - ensure consistent formatting
    df_new["CA_Clean"] = df_new["Cont.Account"].apply(
        lambda x: str(int(float(x))) if pd.notna(x) and str(x).replace('.', '').replace('0', '').strip() else str(x)
    )
    
    # For ZDM_PREMDETAILS - ensure consistent formatting
    zdm_df["CA_Clean"] = zdm_df["Contract Account"].apply(
        lambda x: str(int(float(x))) if pd.notna(x) and str(x).replace('.', '').replace('0', '').strip() else str(x)
    )
    
    # Check for matches
    dfkkop_ca_set = set(df_new["CA_Clean"].dropna().unique())
    zdm_ca_set = set(zdm_df["CA_Clean"].dropna().unique())
    overlap = dfkkop_ca_set.intersection(zdm_ca_set)
    
    print(f"\nMatching statistics:")
    print(f"Unique Contract Accounts in DFKKOP: {len(dfkkop_ca_set)}")
    print(f"Unique Contract Accounts in ZDM_PREMDETAILS: {len(zdm_ca_set)}")
    print(f"Overlapping Contract Accounts: {len(overlap)}")
    
    if len(overlap) == 0:
        print("\nWARNING: No direct matches found. Trying alternative matching...")
        
        # Try matching with leading zeros
        # Method 1: Pad DFKKOP values to 12 digits
        df_new["CA_Padded"] = df_new["Cont.Account"].apply(
            lambda x: str(x).strip().zfill(12) if pd.notna(x) else ""
        )
        
        # Method 2: Remove all leading zeros from both
        df_new["CA_NoZeros"] = df_new["Cont.Account"].apply(
            lambda x: str(x).lstrip('0') if pd.notna(x) and str(x).strip() else ""
        )
        zdm_df["CA_NoZeros"] = zdm_df["Contract Account"].apply(
            lambda x: str(x).lstrip('0') if pd.notna(x) and str(x).strip() else ""
        )
        
        # Check which method works
        overlap_padded = set(df_new["CA_Padded"].dropna().unique()).intersection(set(zdm_df["Contract Account"].unique()))
        overlap_nozeros = set(df_new["CA_NoZeros"].dropna().unique()).intersection(set(zdm_df["CA_NoZeros"].unique()))
        
        print(f"Overlap with padding: {len(overlap_padded)}")
        print(f"Overlap without zeros: {len(overlap_nozeros)}")
        
        # Use the method with better overlap
        if len(overlap_padded) > len(overlap_nozeros):
            ca_field_dfkkop = "CA_Padded"
            ca_field_zdm = "Contract Account"
            print("Using padded Contract Account for matching")
        else:
            ca_field_dfkkop = "CA_NoZeros"
            ca_field_zdm = "CA_NoZeros"
            print("Using no-zeros Contract Account for matching")
    else:
        ca_field_dfkkop = "CA_Clean"
        ca_field_zdm = "CA_Clean"
        print("Using cleaned Contract Account for matching")
    
    # Create the mapping
    print(f"\nCreating Contract Account to LOCATIONID mapping...")
    ca_to_locationid = {}
    for _, row in zdm_df.iterrows():
        ca = row[ca_field_zdm]
        premise = str(row["Premise"]).strip()
        if pd.notna(ca) and pd.notna(premise) and ca and premise:
            ca_to_locationid[ca] = premise
    
    print(f"Created mapping with {len(ca_to_locationid)} entries")
    
    # Apply the mapping
    df_new["LOCATIONID"] = df_new[ca_field_dfkkop].map(ca_to_locationid).fillna("")
    matched_count = (df_new["LOCATIONID"] != "").sum()
    unmatched_count = (df_new["LOCATIONID"] == "").sum()
    
    print(f"\nLOCATIONID Mapping Results:")
    print(f"Records with LOCATIONID: {matched_count:,} ({matched_count/len(df_new)*100:.1f}%)")
    print(f"Records without LOCATIONID: {unmatched_count:,} ({unmatched_count/len(df_new)*100:.1f}%)")
    
    # If we still have missing values, try EVER as a backup
    if unmatched_count > 0 and data_sources.get("EVER") is not None:
        print(f"\nTrying EVER fallback for {unmatched_count} missing LOCATIONIDs...")
        
        ever_df = data_sources["EVER"].copy()
        
        # First check EVER data format
        print("Sample EVER Cont.Account values:")
        ever_ca_sample = ever_df["Cont.Account"].dropna().head(5)
        for ca in ever_ca_sample:
            print(f"  '{ca}'")
        
        # Apply same cleaning to EVER Contract Accounts
        if ca_field_dfkkop == "CA_Padded":
            ever_df["CA_Match"] = ever_df["Cont.Account"].apply(
                lambda x: str(x).strip().zfill(12) if pd.notna(x) else ""
            )
        elif ca_field_dfkkop == "CA_NoZeros":
            ever_df["CA_Match"] = ever_df["Cont.Account"].apply(
                lambda x: str(x).lstrip('0') if pd.notna(x) and str(x).strip() else ""
            )
        else:
            ever_df["CA_Match"] = ever_df["Cont.Account"].apply(
                lambda x: str(int(float(x))) if pd.notna(x) and str(x).replace('.', '').replace('0', '').strip() else str(x)
            )
        
        # Create Contract Account to Installation mapping
        ca_to_install = {}
        for _, row in ever_df.iterrows():
            ca = row["CA_Match"]
            install = str(row["Installat."]).strip()
            if pd.notna(ca) and pd.notna(install) and ca and install:
                ca_to_install[ca] = install
        
        print(f"Created CA to Installation mapping with {len(ca_to_install)} entries")
        
        # Create Installation to Premise mapping from ZDM
        install_to_premise = {}
        for _, row in zdm_df.iterrows():
            install = str(row["Installation"]).strip()
            premise = str(row["Premise"]).strip()
            if pd.notna(install) and pd.notna(premise) and install and premise:
                install_to_premise[install] = premise
        
        print(f"Created Installation to Premise mapping with {len(install_to_premise)} entries")
        
        # Apply the two-step mapping for missing LOCATIONIDs
        found_count = 0
        for idx in df_new[df_new["LOCATIONID"] == ""].index:
            ca = df_new.at[idx, ca_field_dfkkop]
            if ca in ca_to_install:
                install = ca_to_install[ca]
                if install in install_to_premise:
                    df_new.at[idx, "LOCATIONID"] = install_to_premise[install]
                    found_count += 1
        
        print(f"Found {found_count} additional LOCATIONIDs through EVER")
    
    # Final diagnostic - show some unmatched Contract Accounts
    if unmatched_count > 0:
        print("\nSample unmatched Contract Accounts:")
        unmatched_sample = df_new[df_new["LOCATIONID"] == ""][ca_field_dfkkop].dropna().head(10)
        for ca in unmatched_sample:
            print(f"  '{ca}'")
    
    # Clean up temporary columns
    temp_cols = ["CA_Clean", "CA_Padded", "CA_NoZeros"]
    for col in temp_cols:
        if col in df_new.columns:
            df_new.drop(col, axis=1, inplace=True)
    
else:
    df_new["LOCATIONID"] = ""
    print("ZDM_PREMDETAILS not available, LOCATIONID set to empty")
# --------------------------
# Extract TAXYEAR (optional field)
# --------------------------
# TAXYEAR is optional and not directly mapped
df_new["TAXYEAR"] = ""  # Default to empty
print("Set TAXYEAR to default empty value (not provided in source data)")

# --------------------------
# Extract date fields: TRANSACTIONDATE, BILLINGDATE, DUEDATE - now aligned
# --------------------------
# Transaction Date (Doc. Date)
df_new["TRANSACTIONDATE"] = pd.to_datetime(
    df_new["Doc. Date"],
    errors='coerce'
).dt.strftime('%Y-%m-%d')

# Billing Date (Pstng Date)
df_new["BILLINGDATE"] = pd.to_datetime(
    df_new["Pstng Date"],
    errors='coerce'
).dt.strftime('%Y-%m-%d')

# Due Date
df_new["DUEDATE"] = pd.to_datetime(
    df_new["Due"],
    errors='coerce'
).dt.strftime('%Y-%m-%d')

print(f"Extracted date fields: TRANSACTIONDATE, BILLINGDATE, DUEDATE (aligned)")


# --------------------------
# Extract BILLORINVOICENUMBER - now aligned
# --------------------------
# Bill/Invoice Number (Doc. No.)
df_new["BILLORINVOICENUMBER"] = df_new["Doc. No."].apply(
    lambda x: str(int(x))[2:10] if pd.notna(x) and isinstance(x, (int, float)) else ""
)
print(f"Extracted BILLORINVOICENUMBER (aligned)")
# --------------------------
# OPTIMIZED: Extract TRANSACTIONTYPE, TRANSACTIONDESCRIPTION, BILLTYPE based on MTrans + STrans mapping
# Now properly aligned since df_new contains the same filtered data
# --------------------------
print("\nSetting up MTrans + STrans mapping for multiple fields (ALIGNED)...")

# Create a DataFrame to track progress
progress_start = datetime.now()

# Create a comprehensive mapping dictionary for MTrans + STrans combinations
# Format: (MTrans, STrans): {"TRANSACTIONTYPE": value, "TRANSACTIONDESCRIPTION": value, "BILLTYPE": value}
# Now using standardized string format for both MTrans and STrans
mtrans_strans_mapping = {
    # MTrans 0015 combinations
    ("0015", "0010"): {"TRANSACTIONTYPE": "5", "TRANSACTIONDESCRIPTION": "Reconnection fees", "BILLTYPE": "0"},
    ("0015", "0020"): {"TRANSACTIONTYPE": "5", "TRANSACTIONDESCRIPTION": "Returned checks fees", "BILLTYPE": "0"},
    ("0015", "0021"): {"TRANSACTIONTYPE": "5", "TRANSACTIONDESCRIPTION": "Returned chks fee Cr", "BILLTYPE": "0"},
    ("0015", "0030"): {"TRANSACTIONTYPE": "20", "TRANSACTIONDESCRIPTION": "Late Payment Charges", "BILLTYPE": "0"},
    ("0015", "0040"): {"TRANSACTIONTYPE": "20", "TRANSACTIONDESCRIPTION": "Late Pay Charges Cr", "BILLTYPE": "0"},
    ("0015", "0070"): {"TRANSACTIONTYPE": "5", "TRANSACTIONDESCRIPTION": "Field collection chg", "BILLTYPE": "0"},
    ("0015", "0230"): {"TRANSACTIONTYPE": "5", "TRANSACTIONDESCRIPTION": "Other Misc Charge", "BILLTYPE": "0"},
    ("0015", "0231"): {"TRANSACTIONTYPE": "5", "TRANSACTIONDESCRIPTION": "Other Misc Charge Cr", "BILLTYPE": "0"},
    ("0015", "0300"): {"TRANSACTIONTYPE": "5", "TRANSACTIONDESCRIPTION": "Telemetering", "BILLTYPE": "0"},
    ("0015", "0370"): {"TRANSACTIONTYPE": "4", "TRANSACTIONDESCRIPTION": "Reinstate write off", "BILLTYPE": "0"},
    ("0015", "0371"): {"TRANSACTIONTYPE": "4", "TRANSACTIONDESCRIPTION": "Reinstate write off", "BILLTYPE": "0"},
   
    # MTrans 0025 combinations
    ("0025", "0010"): {"TRANSACTIONTYPE": "14", "TRANSACTIONDESCRIPTION": "Int for Cash Deposit", "BILLTYPE": "0"},
   
    # MTrans 0070 combinations
    ("0070", "0010"): {"TRANSACTIONTYPE": "5", "TRANSACTIONDESCRIPTION": "Return charges", "BILLTYPE": "0"},
   
    # MTrans 0080 combinations
    ("0080", "0005"): {"TRANSACTIONTYPE": "4", "TRANSACTIONDESCRIPTION": "Wkly Installment Rec", "BILLTYPE": "0"},
    ("0080", "0010"): {"TRANSACTIONTYPE": "4", "TRANSACTIONDESCRIPTION": "Mthly Installment Rec", "BILLTYPE": "0"},
   
    # MTrans 0100 combinations
    ("0100", "0002"): {"TRANSACTIONTYPE": "99", "TRANSACTIONDESCRIPTION": "Consumption Billing", "BILLTYPE": "0"},
   
    # MTrans 0200 combinations
    ("0200", "0002"): {"TRANSACTIONTYPE": "99", "TRANSACTIONDESCRIPTION": "Final Billing", "BILLTYPE": "1"},
   
    # MTrans 0620 combinations  
    ("0620", "0010"): {"TRANSACTIONTYPE": "4", "TRANSACTIONDESCRIPTION": "Transfer", "BILLTYPE": "0"},
   
    # MTrans 0630 combinations
    ("0630", "0010"): {"TRANSACTIONTYPE": "4", "TRANSACTIONDESCRIPTION": "Write-Off", "BILLTYPE": "0"},
   
    # Add additional mappings as needed
}

# Add mappings for MTrans-only cases (backward compatibility)
mtrans_only_mapping = {
    "0015": {"TRANSACTIONTYPE": "5", "TRANSACTIONDESCRIPTION": "Misc Charges", "BILLTYPE": "0"},
    "0025": {"TRANSACTIONTYPE": "14", "TRANSACTIONDESCRIPTION": "Interest for Cash Sec.Deposit", "BILLTYPE": "0"},
    "0060": {"TRANSACTIONTYPE": "1", "TRANSACTIONDESCRIPTION": "On Account", "BILLTYPE": "0"},
    "0070": {"TRANSACTIONTYPE": "4", "TRANSACTIONDESCRIPTION": "Returns", "BILLTYPE": "0"},
    "0100": {"TRANSACTIONTYPE": "99", "TRANSACTIONDESCRIPTION": "Consumption Billing", "BILLTYPE": "0"},
    "0150": {"TRANSACTIONTYPE": "4", "TRANSACTIONDESCRIPTION": "Bal.For.Amount", "BILLTYPE": "0"},
    "0200": {"TRANSACTIONTYPE": "99", "TRANSACTIONDESCRIPTION": "Final Billing", "BILLTYPE": "1"},
    "0250": {"TRANSACTIONTYPE": "4", "TRANSACTIONDESCRIPTION": "Transfer Posting for Invoicing", "BILLTYPE": "0"},
    "0600": {"TRANSACTIONTYPE": "1", "TRANSACTIONDESCRIPTION": "Payment", "BILLTYPE": "0"},
    "0610": {"TRANSACTIONTYPE": "99", "TRANSACTIONDESCRIPTION": "Account Maintenance", "BILLTYPE": "0"},
    "0620": {"TRANSACTIONTYPE": "4", "TRANSACTIONDESCRIPTION": "Transfer", "BILLTYPE": "0"},
    "0625": {"TRANSACTIONTYPE": "99", "TRANSACTIONDESCRIPTION": "Resetting Cleared Items", "BILLTYPE": "0"},
    "0630": {"TRANSACTIONTYPE": "4", "TRANSACTIONDESCRIPTION": "Write off", "BILLTYPE": "0"},
    "CONV": {"TRANSACTIONTYPE": "99", "TRANSACTIONDESCRIPTION": "Conversion Record 2015-17", "BILLTYPE": "0"},
}

# Default values
default_mapping = {
    "TRANSACTIONTYPE": "99",
    "TRANSACTIONDESCRIPTION": "Other Transaction",
    "BILLTYPE": "0"
}

# OPTIMIZATION: Create a lookup table for all possible variations
print("Creating optimized lookup table...")
complete_mapping = {}

# Add all MTrans + STrans combinations
for (mtrans, strans), mapping in mtrans_strans_mapping.items():
    # Original format
    complete_mapping[(mtrans, strans)] = mapping
   
    # Try numeric formats if possible
    try:
        mtrans_num = str(int(mtrans))
        strans_num = str(int(strans))
       
        # Add variants without leading zeros
        complete_mapping[(mtrans_num, strans_num)] = mapping
        complete_mapping[(mtrans_num, strans)] = mapping
        complete_mapping[(mtrans, strans_num)] = mapping
    except ValueError:
        pass

# Add MTrans-only mappings
for mtrans, mapping in mtrans_only_mapping.items():
    for strans in ["", "nan", "None", None]:
        key = (mtrans, strans)
        if key not in complete_mapping:
            complete_mapping[key] = mapping
       
        # Try numeric format if possible
        try:
            mtrans_num = str(int(mtrans))
            key_num = (mtrans_num, strans)
            if key_num not in complete_mapping:
                complete_mapping[key_num] = mapping
        except ValueError:
            pass

print(f"Lookup table created with {len(complete_mapping)} entries")

# SIMPLIFIED: Direct mapping since df_new now contains the source data
print("Applying mapping directly to aligned data...")

# Create clean copies of MTrans and STrans - now already standardized
mtrans_series = df_new["MTrans"].astype(str).str.strip()
strans_series = df_new["STrans"].astype(str).str.strip()

# Initialize result columns with defaults  
df_new["TRANSACTIONTYPE"] = "99"
df_new["TRANSACTIONDESCRIPTION"] = "Other Transaction"
df_new["BILLTYPE"] = "0"

# Apply mappings directly
match_count = 0
for idx in df_new.index:
    mtrans = mtrans_series.loc[idx]
    strans = strans_series.loc[idx]
    key = (mtrans, strans)
    
    if key in complete_mapping:
        mapping = complete_mapping[key]
        df_new.at[idx, "TRANSACTIONTYPE"] = mapping["TRANSACTIONTYPE"]
        df_new.at[idx, "TRANSACTIONDESCRIPTION"] = mapping["TRANSACTIONDESCRIPTION"] 
        df_new.at[idx, "BILLTYPE"] = mapping["BILLTYPE"]
        match_count += 1
    elif (mtrans, "") in complete_mapping:
        # Fallback to MTrans-only mapping if available
        mapping = complete_mapping[(mtrans, "")]
        df_new.at[idx, "TRANSACTIONTYPE"] = mapping["TRANSACTIONTYPE"]
        df_new.at[idx, "TRANSACTIONDESCRIPTION"] = mapping["TRANSACTIONDESCRIPTION"]
        df_new.at[idx, "BILLTYPE"] = mapping["BILLTYPE"]
        match_count += 1

# Calculate and print elapsed time
elapsed = datetime.now() - progress_start
print(f"Mapping completed in {elapsed.total_seconds():.2f} seconds")
print(f"Matched {match_count} records out of {len(df_new)} ({match_count/len(df_new)*100:.1f}%)")

# Print mapping statistics
print(f"TRANSACTIONTYPE distribution: {df_new['TRANSACTIONTYPE'].value_counts().to_dict()}")
print(f"BILLTYPE distribution: {df_new['BILLTYPE'].value_counts().to_dict()}")
print(f"Sample TRANSACTIONDESCRIPTION values: {df_new['TRANSACTIONDESCRIPTION'].head(10).tolist()}")

# Filter out records marked for "don't convert" (empty TRANSACTIONTYPE)
records_before = len(df_new)
df_new = df_new[df_new["TRANSACTIONTYPE"] != ""]
records_after = len(df_new)
print(f"Filtered out {records_before - records_after} records marked 'don't convert'")
    
# --------------------------
# Extract TRANSACTIONAMOUNT - now aligned
# --------------------------
# Transaction Amount - preserve the sign from source data
df_new["TRANSACTIONAMOUNT"] = df_new["Amount"]

print(f"Extracted TRANSACTIONAMOUNT (aligned)")
print(f"Negative amount count: {(df_new['TRANSACTIONAMOUNT'] < 0).sum()}")
print(f"Positive amount count: {(df_new['TRANSACTIONAMOUNT'] > 0).sum()}")
print(f"Zero amount count: {(df_new['TRANSACTIONAMOUNT'] == 0).sum()}")

# --------------------------
# Determine APPLICATION (Commodity Type) - now aligned
# --------------------------
# Default to "5" (Gas) for all records as per the requirements
df_new["APPLICATION"] = "5"

# Check ZDM_PREMDETAILS for MRU=METRNP01 to set APPLICATION=2
if data_sources.get("ZDM_PREMDETAILS") is not None:
    print("\nSetting APPLICATION values based on MRU codes...")
    
    # Create a lookup from LOCATIONID to APPLICATION code
    location_to_application = {}
    
    # Find all premises that have MRU=METRNP01
    zdm_df = data_sources["ZDM_PREMDETAILS"].copy()
    # Filter for records with METRNP01
    metrnp01_records = zdm_df[zdm_df["MRU"] == "METRNP01"]
    metrnp01_count = len(metrnp01_records)
    print(f"Found {metrnp01_count} records with MRU=METRNP01")
    
    if metrnp01_count > 0:
        # Create mapping from Premise (LOCATIONID) to APPLICATION="2"
        for premise in metrnp01_records["Premise"].astype(str):
            location_to_application[premise] = "2"
        
        print(f"Created mapping for {len(location_to_application)} premises with APPLICATION=2")
        
        # Apply the mapping using vectorized operations  
        updated_count = 0
        for premise in location_to_application:
            mask = df_new["LOCATIONID"].astype(str) == premise
            df_new.loc[mask, "APPLICATION"] = "2"
            updated_count += mask.sum()
        
        print(f"Updated {updated_count} records to APPLICATION=2")
    
print(f"APPLICATION values - Gas(5): {(df_new['APPLICATION'] == '5').sum()}, Electric(2): {(df_new['APPLICATION'] == '2').sum()}")

# --------------------------
# Set TENDERTYPE to empty (not processing this field)
# --------------------------
df_new["TENDERTYPE"] = ""
print("TENDERTYPE set to empty (not processing this field)")

# --------------------------
# Set UPDATEDATE
# --------------------------
df_new['UPDATEDATE'] = " "

# --------------------------
# Format values with proper quoting
# --------------------------
print("\nFormatting field values...")
def custom_quote(val):
    if pd.isna(val) or val in ["", " "]:
        return ""
    return f'"{val}"'
    
def selective_custom_quote(val, column_name):
    # Numeric fields that should not be quoted
    numeric_columns = ['TAXYEAR', 'TRANSACTIONTYPE', 'TRANSACTIONAMOUNT', 'APPLICATION', 
                      'BILLTYPE', 'TENDERTYPE']
    
    if column_name in numeric_columns:
        return val
    return "" if val in [None, 'nan', 'NaN', 'NAN', ''] else custom_quote(val)
    
df_new = df_new.fillna("")
for col in df_new.columns:
    df_new[col] = df_new[col].apply(lambda x: selective_custom_quote(x, col))


# ADD THIS DIAGNOSTIC CODE HERE (after line 769):
# DIAGNOSTIC: Check years before filtering
print("\n*** DIAGNOSTIC: Year distribution BEFORE filtering for missing fields ***")
# First check the original DFKKOP data
year_dist_original = pd.to_datetime(data_sources["DFKKOP"]["Doc. Date"], errors='coerce').dt.year.value_counts().sort_index()
print("Year distribution in filtered DFKKOP data:")
print(year_dist_original)

# Check which years are getting filtered out due to missing LOCATIONID
df_temp = df_new.copy()
df_temp['Year'] = pd.to_datetime(df_temp['TRANSACTIONDATE'].str.replace('"', ''), errors='coerce').dt.year

# Check missing LOCATIONID by year
missing_locationid = df_temp[df_temp['LOCATIONID'] == ""]
if len(missing_locationid) > 0:
    print(f"\nRecords missing LOCATIONID by year (these will be filtered out):")
    print(missing_locationid['Year'].value_counts().sort_index())
    print(f"\nTotal records that will be lost due to missing LOCATIONID: {len(missing_locationid)}")

# Check missing other required fields
missing_customerid = df_temp[df_temp['CUSTOMERID'] == ""]
missing_transdate = df_temp[df_temp['TRANSACTIONDATE'] == ""]
print(f"\nRecords missing CUSTOMERID: {len(missing_customerid)}")
print(f"Records missing TRANSACTIONDATE: {len(missing_transdate)}")

# Remove any records missing ACCOUNTNUMBER and drop duplicates
df_new = df_new[(df_new['CUSTOMERID'] != "") & (df_new['LOCATIONID'] != "") & (df_new['TRANSACTIONDATE'] != "")]

# Remove any records missing ACCOUNTNUMBER and drop duplicates
df_new = df_new[(df_new['CUSTOMERID'] != "") & (df_new['LOCATIONID'] != "") & (df_new['TRANSACTIONDATE'] != "")]
# --------------------------
# Reorder columns based on target format and drop DFKKOP source columns
# --------------------------
column_order = [
    "TAXYEAR", "CUSTOMERID", "LOCATIONID", "TRANSACTIONDATE", "BILLINGDATE", 
    "DUEDATE", "BILLORINVOICENUMBER", "TRANSACTIONTYPE", "TRANSACTIONAMOUNT", 
    "TRANSACTIONDESCRIPTION", "APPLICATION", "BILLTYPE", "TENDERTYPE", "UPDATEDATE"
]

# Verify all required columns exist
missing_columns = [col for col in column_order if col not in df_new.columns]
if missing_columns:
    print(f"Warning: Missing required columns: {missing_columns}")
    for col in missing_columns:
        df_new[col] = ""

# Keep only the required columns (drop all DFKKOP source columns)
df_new = df_new[column_order]
print(f"Ordered columns according to target format. Final columns: {len(df_new.columns)}")
print(f"Final record count after alignment: {len(df_new)}")


# --------------------------
# Add trailer row
# --------------------------
trailer_row = pd.DataFrame([["TRAILER"] + [''] * (len(df_new.columns) - 1)], columns=df_new.columns)
df_new = pd.concat([df_new, trailer_row], ignore_index=True)
print(f"Added trailer row. Final row count: {len(df_new)}")

# --------------------------
# Save to CSV
# --------------------------
output_path = os.path.join(os.path.dirname(list(file_paths.values())[0]), 'STAGE_TRANSACTIONAL_HIST_05232025_1423PM.csv')
df_new.to_csv(output_path, index=False, header=True, quoting=csv.QUOTE_NONE, escapechar='\\')
print(f"CSV file saved at {output_path}")

# --------------------------
# Final validation summary
# --------------------------
print("\nFinal Output Validation:")
print(f"Total rows (excluding trailer): {len(df_new) - 1}")
print(f"All required columns present: {len(missing_columns) == 0}")
non_empty_cols = {col: (df_new[col] != "").sum() for col in column_order}
print("Non-empty values per column:")
for col, count in non_empty_cols.items():
    print(f"  {col}: {count} rows with values")