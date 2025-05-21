# CONV2_STAGE_TRANSACTIONAL_HIST_05202025_1920pm.py
# 1920pm
# OPTIMIZED VERSION
# FOR KYLE REMOVING BLANKS STAGE_TRANSACTIONAL_HIST.py
# Script to process transactional history data for conversion

import pandas as pd
import os
import csv
import multiprocessing
from datetime import datetime
import time

# Add this line for Windows multiprocessing support
if __name__ == "__main__":
    multiprocessing.freeze_support()

start_time = time.time()  # Track execution time

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
   
    # Adding DFKKZP source for TENDERTYPE mapping
    "DFKKZP": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_TRANSACTIONAL_HIST\dfkkzp v2.xlsx"
}

# OPTIMIZATION: Function to read an Excel file with optimized parameters
def read_excel_file(name, path):
    try:
        # Determine which columns to read based on file type
        usecols = None
        dtype_dict = {}
        
        if name.startswith("DFKKOP"):
            # Only load columns we need from DFKKOP files
            usecols = ["BPartner", "Cont.Account", "Doc. No.", "Doc. Date", "Pstng Date", 
                       "Due", "Amount", "MTrans", "STrans"]
            dtype_dict = {"BPartner": str, "Cont.Account": str, "MTrans": str, "STrans": str}
        elif name == "ZDM_PREMDETAILS":
            # First load a sample to check the structure
            temp_df = pd.read_excel(path, sheet_name="Sheet1", engine="openpyxl", nrows=5)
            column_count = len(temp_df.columns)
            print(f"ZDM_PREMDETAILS columns: {column_count}")
            # We need Contract Account, Premise, Installation and MRU
            if column_count >= 30:
                usecols = ["Contract Account", "Premise", "Installation", "MRU"]
            dtype_dict = {"Contract Account": str, "Premise": str, "Installation": str}
        elif name == "EVER":
            usecols = ["Cont.Account", "Installat."]
            dtype_dict = {"Cont.Account": str, "Installat.": str}
        elif name == "DFKKZP":
            # First check the structure to find the correct column names
            temp_df = pd.read_excel(path, sheet_name="Sheet1", engine="openpyxl", nrows=5)
            print(f"DFKKZP columns: {list(temp_df.columns)}")
            # As clarified, 'Selection Value 1' corresponds to 'Cont.Account'
            if 'Selection Value 1' in temp_df.columns and 'TenderCode' in temp_df.columns:
                usecols = ["Selection Value 1", "TenderCode", "Indicator"]
                dtype_dict = {"Selection Value 1": str, "TenderCode": str, "Indicator": str}
        
        # Now read the file with optimized parameters
        if usecols is not None:
            df = pd.read_excel(path, sheet_name="Sheet1", engine="openpyxl", 
                              usecols=usecols, dtype=dtype_dict)
        else:
            df = pd.read_excel(path, sheet_name="Sheet1", engine="openpyxl", dtype=dtype_dict)
        
        print(f"Successfully loaded {name}: {df.shape[0]} rows, {df.shape[1]} columns")
        return df
    except Exception as e:
        print(f"Error loading {name}: {e}")
        return None

# Function to process data in batches
def process_in_batches(dataframe, batch_size, process_func, *args):
    total_rows = len(dataframe)
    results = []
    
    for start_idx in range(0, total_rows, batch_size):
        end_idx = min(start_idx + batch_size, total_rows)
        if start_idx % 100000 == 0 and start_idx > 0:
            print(f"Processing batch {start_idx} to {end_idx} of {total_rows}...")
        
        batch_result = process_func(dataframe.iloc[start_idx:end_idx], *args)
        results.append(batch_result)
    
    return pd.concat(results) if results else pd.DataFrame()

# Load data sources more efficiently
print("Loading data sources...")
data_sources = {}

# Load each file
for name, path in file_paths.items():
    data_sources[name] = read_excel_file(name, path)

# Combine all DFKKOP files
dfkkop_keys = [key for key in data_sources.keys() if key.startswith("DFKKOP")]
if dfkkop_keys:
    dfkkop_dfs = [data_sources[key] for key in dfkkop_keys if data_sources[key] is not None]
    if dfkkop_dfs:
        data_sources["DFKKOP"] = pd.concat(dfkkop_dfs, ignore_index=True)
        print(f"Combined {len(dfkkop_dfs)} DFKKOP files into a single dataframe with {len(data_sources['DFKKOP'])} rows")
        
        # Ensure MTrans and STrans are treated as strings
        data_sources["DFKKOP"]["MTrans"] = data_sources["DFKKOP"]["MTrans"].astype(str)
        data_sources["DFKKOP"]["STrans"] = data_sources["DFKKOP"]["STrans"].astype(str)
        
        # Replace NaN values with empty strings
        data_sources["DFKKOP"]["MTrans"] = data_sources["DFKKOP"]["MTrans"].replace('nan', '')
        data_sources["DFKKOP"]["STrans"] = data_sources["DFKKOP"]["STrans"].replace('nan', '')
        
        # Check data types of key columns
        print("\nDFKKOP column data types:")
        for col in ['MTrans', 'STrans', 'Doc. No.', 'Doc. Date', 'Amount']:
            if col in data_sources["DFKKOP"].columns:
                print(f"  {col}: {data_sources['DFKKOP'][col].dtype}")
        
        # Check if MTrans values are being stored as expected
        if 'MTrans' in data_sources["DFKKOP"].columns:
            print("\nUnique MTrans values (first 20):")
            unique_mtrans = data_sources["DFKKOP"]['MTrans'].unique()
            print(unique_mtrans[:20])
            
            # Check if STrans values are being stored as expected
            if 'STrans' in data_sources["DFKKOP"].columns:
                print("\nUnique STrans values (first 20):")
                unique_strans = data_sources["DFKKOP"]['STrans'].unique()
                print(unique_strans[:20])
    else:
        print("Warning: No valid DFKKOP dataframes found to combine")
else:
    print("Warning: No DFKKOP files found in data_sources")

# Initialize output DataFrame only if we have DFKKOP data
if data_sources.get("DFKKOP") is not None:
    print("\nStarting field extraction and transformation...")
    df_new = pd.DataFrame()
    
    # Extract CUSTOMERID from DFKKOP (BPartner column)
    df_new["CUSTOMERID"] = data_sources["DFKKOP"]["BPartner"].apply(
        lambda x: str(int(x)) if pd.notna(x) and isinstance(x, (int, float)) else str(x)
    ).str.slice(0, 15)
    print(f"Extracted {len(df_new)} CUSTOMERID values")
    
    # --------------------------
    # Extract LOCATIONID through direct merge between DFKKOP, EVER and ZDM_PREMDETAILS
    # --------------------------
    if data_sources.get("DFKKOP") is not None and data_sources.get("ZDM_PREMDETAILS") is not None:
        print("\nExtracting LOCATIONID...")
        progress_start = datetime.now()
        
        # First, get Contract Account from DFKKOP
        df_new["Cont.Account"] = data_sources["DFKKOP"]["Cont.Account"].astype(str).str.strip()
        
        # Print samples to debug
        print("Sample DFKKOP Cont.Account values:", df_new["Cont.Account"].head(5).tolist())
        
        # Prepare ZDM_PREMDETAILS data - IMPORTANT: Keep original format with leading zeros!
        zdm_df = data_sources["ZDM_PREMDETAILS"].copy()
        zdm_df["Contract Account"] = zdm_df["Contract Account"].astype(str).str.strip()
        zdm_df["Premise"] = zdm_df["Premise"].astype(str).str.strip()
        
        # Print samples to debug
        print("Sample ZDM_PREMDETAILS Contract Account values:", zdm_df["Contract Account"].head(5).tolist())
        
        # Create lookup table
        location_lookup = zdm_df[["Contract Account", "Premise"]].copy()
        location_lookup = location_lookup.rename(columns={"Premise": "LOCATIONID"})
        
        # Check if there's any overlap between the two sets of Contract Account values
        dfkkop_ca_set = set(df_new["Cont.Account"].unique())
        zdm_ca_set = set(location_lookup["Contract Account"].unique())
        overlap = dfkkop_ca_set.intersection(zdm_ca_set)
        print(f"Contract Account overlap: {len(overlap)} values")
        
        # OPTIMIZATION: Create an efficient lookup dictionary instead of using merge
        print("Creating optimized LOCATIONID lookup...")
        ca_to_location = {}
        
        # Add all formats to the dictionary for robust matching
        for ca, location in zip(location_lookup["Contract Account"], location_lookup["LOCATIONID"]):
            if pd.notna(ca) and pd.notna(location):
                # Original format
                ca_to_location[ca] = location
                
                # Try formatting without leading zeros for numeric values
                try:
                    ca_numeric = str(int(float(ca)))
                    ca_to_location[ca_numeric] = location
                except (ValueError, TypeError):
                    pass
                
                # Try with padded zeros
                ca_to_location[str(ca).zfill(12)] = location
        
        print(f"Created location lookup with {len(ca_to_location)} entries")
        
        # Apply the lookup to df_new - process in batches for performance
        print("Applying LOCATIONID mapping...")
        batch_size = 50000
        total_rows = len(df_new)
        matches = 0
        
        for start_idx in range(0, total_rows, batch_size):
            end_idx = min(start_idx + batch_size, total_rows)
            if start_idx % 100000 == 0 and start_idx > 0:
                print(f"Processing LOCATIONID for records {start_idx} to {end_idx} of {total_rows}...")
            
            for i in range(start_idx, end_idx):
                ca = df_new.at[i, "Cont.Account"]
                
                # Try different formats for lookup
                if ca in ca_to_location:
                    df_new.at[i, "LOCATIONID"] = ca_to_location[ca]
                    matches += 1
                else:
                    # Try alternate formats
                    try:
                        ca_numeric = str(int(float(ca)))
                        if ca_numeric in ca_to_location:
                            df_new.at[i, "LOCATIONID"] = ca_to_location[ca_numeric]
                            matches += 1
                            continue
                    except (ValueError, TypeError):
                        pass
                    
                    # Try padded format
                    ca_padded = str(ca).zfill(12)
                    if ca_padded in ca_to_location:
                        df_new.at[i, "LOCATIONID"] = ca_to_location[ca_padded]
                        matches += 1
        
        print(f"Matched {matches} records with LOCATIONID values")
        
        # If we still have missing LOCATIONID values, try using EVER for a fallback
        missing_count = df_new["LOCATIONID"].isna().sum() if "LOCATIONID" in df_new.columns else total_rows
        if missing_count > 0 and data_sources.get("EVER") is not None:
            print(f"Still missing {missing_count} LOCATIONID values, trying EVER fallback")
            
            # Create a mapping from Cont.Account to Installation in EVER
            ever_df = data_sources["EVER"].copy()
            ever_df["Cont.Account"] = ever_df["Cont.Account"].astype(str).str.strip()
            ever_df["Installation"] = ever_df["Installat."].astype(str).str.strip()
            
            # Create a clean lookup dictionary
            ca_to_install = {}
            for ca, install in zip(ever_df["Cont.Account"], ever_df["Installation"]):
                if pd.notna(ca) and pd.notna(install) and ca and install:
                    ca_to_install[ca] = install
                    # Add numeric variant
                    try:
                        ca_numeric = str(int(float(ca)))
                        ca_to_install[ca_numeric] = install
                    except (ValueError, TypeError):
                        pass
            
            # Create a clean lookup from Installation to Premise
            install_to_premise = {}
            for install, premise in zip(zdm_df["Installation"].astype(str), zdm_df["Premise"].astype(str)):
                if pd.notna(install) and pd.notna(premise) and install and premise:
                    install_to_premise[install] = premise
            
            # Apply the mappings for rows with missing LOCATIONID
            found_count = 0
            
            for i in range(total_rows):
                if i % 100000 == 0 and i > 0:
                    print(f"Checking EVER fallback for record {i} of {total_rows}...")
                
                # Skip if already has LOCATIONID
                if "LOCATIONID" in df_new.columns and pd.notna(df_new.at[i, "LOCATIONID"]):
                    continue
                    
                ca = df_new.at[i, "Cont.Account"]
                if ca in ca_to_install:
                    install = ca_to_install[ca]
                    if install in install_to_premise:
                        if "LOCATIONID" not in df_new.columns:
                            df_new["LOCATIONID"] = ""
                        df_new.at[i, "LOCATIONID"] = install_to_premise[install]
                        found_count += 1
            
            print(f"Found {found_count} additional LOCATIONID values through EVER mapping")
        
        # Make sure LOCATIONID column exists
        if "LOCATIONID" not in df_new.columns:
            df_new["LOCATIONID"] = ""
        
        # Fill any remaining NaN values with empty string
        df_new["LOCATIONID"] = df_new["LOCATIONID"].fillna("")
        print(f"After all lookups: Rows with non-empty LOCATIONID: {(df_new['LOCATIONID'] != '').sum()}")
        
        # Drop the Cont.Account temporary column
        if "Cont.Account" in df_new.columns:
            df_new.drop(columns=["Cont.Account"], inplace=True)
        
        # Calculate and print elapsed time
        elapsed = datetime.now() - progress_start
        print(f"LOCATIONID extraction completed in {elapsed.total_seconds():.2f} seconds")
    
    # --------------------------
    # Extract TAXYEAR (optional field)
    # --------------------------
    # TAXYEAR is optional and not directly mapped
    df_new["TAXYEAR"] = ""  # Default to empty
    print("Set TAXYEAR to default empty value (not provided in source data)")
    
    # --------------------------
    # Extract date fields: TRANSACTIONDATE, BILLINGDATE, DUEDATE
    # --------------------------
    if data_sources.get("DFKKOP") is not None:
        # Transaction Date (Doc. Date)
        df_new["TRANSACTIONDATE"] = pd.to_datetime(
            data_sources["DFKKOP"]["Doc. Date"],
            errors='coerce'
        ).dt.strftime('%Y-%m-%d')
        
        # Billing Date (Pstng Date)
        df_new["BILLINGDATE"] = pd.to_datetime(
            data_sources["DFKKOP"]["Pstng Date"],
            errors='coerce'
        ).dt.strftime('%Y-%m-%d')
        
        # Due Date
        df_new["DUEDATE"] = pd.to_datetime(
            data_sources["DFKKOP"]["Due"],
            errors='coerce'
        ).dt.strftime('%Y-%m-%d')
        
        print(f"Extracted date fields: TRANSACTIONDATE, BILLINGDATE, DUEDATE")
    
    # --------------------------
    # Extract BILLORINVOICENUMBER
    # --------------------------
    if data_sources.get("DFKKOP") is not None:
        # Bill/Invoice Number (Doc. No.)
        df_new["BILLORINVOICENUMBER"] = data_sources["DFKKOP"]["Doc. No."].apply(
            lambda x: str(int(x))[2:10] if pd.notna(x) and isinstance(x, (int, float)) else ""
        )
        print(f"Extracted BILLORINVOICENUMBER")
    
    # --------------------------
    # OPTIMIZED: Extract TRANSACTIONTYPE, TRANSACTIONDESCRIPTION, BILLTYPE based on MTrans + STrans mapping
    # --------------------------
    if data_sources.get("DFKKOP") is not None:
        print("\nSetting up MTrans + STrans mapping for multiple fields...")
        
        # Create a DataFrame to track progress
        progress_start = datetime.now()
        
        # Create a comprehensive mapping dictionary for MTrans + STrans combinations
        # Format: (MTrans, STrans): {"TRANSACTIONTYPE": value, "TRANSACTIONDESCRIPTION": value, "BILLTYPE": value}
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
        
        # OPTIMIZATION: Use vectorized operations for mapping
        # Prepare dataframe for mapping
        print("Preparing data for vectorized mapping...")
        
        # Create clean copies of MTrans and STrans
        mtrans_series = data_sources["DFKKOP"]["MTrans"].astype(str).replace('nan', '')
        strans_series = data_sources["DFKKOP"]["STrans"].astype(str).replace('nan', '')
        
        # Create a mapping DataFrame for better performance
        map_df = pd.DataFrame({
            "MTrans": mtrans_series,
            "STrans": strans_series,
        })
        
        # Create a composite key column
        map_df["key"] = list(zip(map_df["MTrans"], map_df["STrans"]))
        
        # Create result columns with defaults
        map_df["TRANSACTIONTYPE"] = "99"
        map_df["TRANSACTIONDESCRIPTION"] = "Other Transaction"
        map_df["BILLTYPE"] = "0"
        
        # Use the lookup table to map values efficiently
        print("Applying mapping using vectorized operations...")
        match_count = 0
        total_records = len(map_df)
        batch_size = min(10000, total_records)  # Process in batches for large datasets
        
        for start_idx in range(0, total_records, batch_size):
            end_idx = min(start_idx + batch_size, total_records)
            if start_idx % 50000 == 0 and start_idx > 0:
                print(f"Processing records {start_idx} to {end_idx} of {total_records}...")
            
            batch = map_df.iloc[start_idx:end_idx]
            
            for idx, row in batch.iterrows():
                key = row["key"]
                if key in complete_mapping:
                    mapping = complete_mapping[key]
                    map_df.at[idx, "TRANSACTIONTYPE"] = mapping["TRANSACTIONTYPE"]
                    map_df.at[idx, "TRANSACTIONDESCRIPTION"] = mapping["TRANSACTIONDESCRIPTION"]
                    map_df.at[idx, "BILLTYPE"] = mapping["BILLTYPE"]
                    match_count += 1
                elif (row["MTrans"], "") in complete_mapping:
                    # Fallback to MTrans-only mapping if available
                    mapping = complete_mapping[(row["MTrans"], "")]
                    map_df.at[idx, "TRANSACTIONTYPE"] = mapping["TRANSACTIONTYPE"]
                    map_df.at[idx, "TRANSACTIONDESCRIPTION"] = mapping["TRANSACTIONDESCRIPTION"]
                    map_df.at[idx, "BILLTYPE"] = mapping["BILLTYPE"]
                    match_count += 1
        
        print("Creating direct field mappings to resolve index issues...")
        # Create dictionaries for direct mapping by index
        index_to_transactiontype = {}
        index_to_transactiondescription = {}
        index_to_billtype = {}
        
        # Fill the dictionaries
        for idx, row in enumerate(zip(map_df["TRANSACTIONTYPE"], map_df["TRANSACTIONDESCRIPTION"], map_df["BILLTYPE"])):
            trans_type, trans_desc, bill_type = row
            index_to_transactiontype[idx] = trans_type
            index_to_transactiondescription[idx] = trans_desc
            index_to_billtype[idx] = bill_type
        
        # Apply the mappings directly to df_new, using index as key for safe assignment
        for idx in range(len(df_new)):
            if idx < len(map_df):
                df_new.at[idx, "TRANSACTIONTYPE"] = index_to_transactiontype.get(idx, "99")
                df_new.at[idx, "TRANSACTIONDESCRIPTION"] = index_to_transactiondescription.get(idx, "Other Transaction")
                df_new.at[idx, "BILLTYPE"] = index_to_billtype.get(idx, "0")
            else:
                # For any extra rows in df_new, use default values
                df_new.at[idx, "TRANSACTIONTYPE"] = "99"
                df_new.at[idx, "TRANSACTIONDESCRIPTION"] = "Other Transaction"
                df_new.at[idx, "BILLTYPE"] = "0"
                print(f"Warning: Index {idx} out of range for mapping data (max: {len(map_df)-1})")
                # Only print a few warnings to avoid flooding the console
                if idx > len(map_df) + 10:
                    print(f"... skipping additional warnings ({len(df_new) - len(map_df)} total out-of-range indices)")
                    break
        
        # Calculate and print elapsed time
        elapsed = datetime.now() - progress_start
        print(f"Mapping completed in {elapsed.total_seconds():.2f} seconds")
        print(f"Matched {match_count} records out of {total_records} ({match_count/total_records*100:.1f}%)")
        
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
    # Extract TRANSACTIONAMOUNT
    # --------------------------
    if data_sources.get("DFKKOP") is not None:
        # Transaction Amount
        df_new["TRANSACTIONAMOUNT"] = data_sources["DFKKOP"]["Amount"]
        
        # Ensure payments (TRANSACTIONTYPE=1) and certain other types are negative
        # Based on the mapping:
        # - Type 1 (On Account, Payment): Should be negative
        # - Type 4 (Returns, Bal.For.Amount, Transfer, Write off): Should be negative if they're credits
        payment_types = ["1"]  # Always negative
        adjustment_types = ["4"]  # Negative if positive amount (credit)
        
        # Force payment types to be negative
        for payment_type in payment_types:
            mask = (df_new["TRANSACTIONTYPE"] == payment_type) & (df_new["TRANSACTIONAMOUNT"] > 0)
            df_new.loc[mask, "TRANSACTIONAMOUNT"] = -df_new.loc[mask, "TRANSACTIONAMOUNT"]
        
        # For adjustment types, negative adjustments should be negative (credits)
        # This logic is application-specific and may need adjustment based on your data
        for adj_type in adjustment_types:
            # Check for specific MTrans codes that should be negative when they're credits
            if "MTrans" in data_sources["DFKKOP"].columns:
                credit_codes = ["0070", "0150", "0620", "0630"]  # MTrans codes that are credits when positive
                for code in credit_codes:
                    mask = (data_sources["DFKKOP"]["MTrans"] == code) & (df_new["TRANSACTIONAMOUNT"] > 0)
                    df_new.loc[mask, "TRANSACTIONAMOUNT"] = -df_new.loc[mask, "TRANSACTIONAMOUNT"]
        
        print(f"Extracted and adjusted TRANSACTIONAMOUNT")
        print(f"Negative amount count: {(df_new['TRANSACTIONAMOUNT'] < 0).sum()}")
        print(f"Positive amount count: {(df_new['TRANSACTIONAMOUNT'] > 0).sum()}")
    
    # --------------------------
    # Determine APPLICATION (Commodity Type)
    # --------------------------
    # Default to "5" (Gas) for all records as per the requirements
    df_new["APPLICATION"] = "5"
    
    # Check ZDM_PREMDETAILS for MRU=METRNP01 to set APPLICATION=2
    if data_sources.get("ZDM_PREMDETAILS") is not None:
        # Check if any records have MRU=METRNP01
        metrnp01_count = (data_sources["ZDM_PREMDETAILS"]["MRU"] == "METRNP01").sum()
        print(f"Found {metrnp01_count} records with MRU=METRNP01")
        
        if metrnp01_count > 0:
            # We would need to link these to the corresponding transactions
            # This would require a more complex lookup process
            # For now, we'll leave APPLICATION as "5" for all records
            pass
    
    print(f"Set APPLICATION to '5' (Gas) for all records")
    
    # --------------------------
    # OPTIMIZED: Determine TENDERTYPE based on DFKKZP mapping
    # --------------------------
    print("\nDetermining TENDERTYPE based on DFKKZP mapping...")
    progress_start = datetime.now()
    
    # First check if DFKKZP data source is available
    if data_sources.get("DFKKZP") is not None:
        print("Using DFKKZP data for TENDERTYPE mapping")
        
        # Define mapping dictionary for TenderCode -> TENDERTYPE
        tender_map = {
            "CA": "70", "CK": "71", "CR": "81", "WD": "84",
            "UB": "81", "IB": "79", "UK": "80", "IK": "77", "CP": "7"
        }
        
        # Ensure df_dfkkzp is properly formatted
        df_dfkkzp = data_sources["DFKKZP"].copy()
        
        # Use Selection Value 1 as Cont.Account per your clarification
        if "Selection Value 1" in df_dfkkzp.columns:
            df_dfkkzp["Cont.Account"] = df_dfkkzp["Selection Value 1"].astype(str).str.strip()
            
            # Print sample values for debugging
            print("Sample df_dfkkzp Cont.Account values:", df_dfkkzp["Cont.Account"].head(5).tolist())
            
            # Create a dictionary for efficient lookup
            tender_lookup = {}
            for idx, row in df_dfkkzp.iterrows():
                ca = row["Cont.Account"]
                tender_code = row.get("TenderCode", "")
                indicator = row.get("Indicator", "")
                
                if pd.notna(ca) and ca:
                    # Determine TENDERTYPE based on Indicator and TenderCode
                    if indicator in ("1", "2"):
                        tender_lookup[ca] = "69"
                    elif tender_code in tender_map:
                        tender_lookup[ca] = tender_map[tender_code]
                    else:
                        tender_lookup[ca] = ""  # Default value
            
            print(f"Created tender lookup with {len(tender_lookup)} entries")
            
            # Recreate Cont.Account column in df_new for lookup
            if "Cont.Account" not in df_new.columns and data_sources.get("DFKKOP") is not None:
                df_new["Cont.Account"] = data_sources["DFKKOP"]["Cont.Account"].astype(str).str.strip()
            
            # Apply the lookup to df_new
            df_new["TENDERTYPE"] = ""
            
            # Process in batches for performance
            batch_size = 50000
            total_rows = len(df_new)
            matches = 0
            
            for start_idx in range(0, total_rows, batch_size):
                end_idx = min(start_idx + batch_size, total_rows)
                if start_idx % 100000 == 0 and start_idx > 0:
                    print(f"Processing TENDERTYPE for records {start_idx} to {end_idx} of {total_rows}...")
                
                for i in range(start_idx, end_idx):
                    if "Cont.Account" in df_new.columns:
                        ca = df_new.at[i, "Cont.Account"]
                        if ca in tender_lookup:
                            df_new.at[i, "TENDERTYPE"] = tender_lookup[ca]
                            matches += 1
            
            print(f"Matched {matches} records with TENDERTYPE values")
            
            # Fill remaining records with default value
            df_new["TENDERTYPE"] = df_new["TENDERTYPE"].fillna("0")
            
            # Drop the temporary Cont.Account column if it was added
            if "Cont.Account" in df_new.columns:
                df_new.drop(columns=["Cont.Account"], inplace=True)
        else:
            print("Warning: 'Selection Value 1' column not found in DFKKZP, setting default TENDERTYPE")
            df_new["TENDERTYPE"] = "0"
    else:
        # Default to "0" if DFKKZP data is not available
        print("DFKKZP data not available. Setting default TENDERTYPE value.")
        df_new["TENDERTYPE"] = "0"
    
    # Calculate and print elapsed time
    elapsed = datetime.now() - progress_start
    print(f"TENDERTYPE mapping completed in {elapsed.total_seconds():.2f} seconds")
    
    # --------------------------
    # Set UPDATEDATE to current date (conversion date)
    # --------------------------
    current_date = datetime.now().strftime('%Y-%m-%d')
    df_new["UPDATEDATE"] = current_date
    print(f"Set UPDATEDATE to current date: {current_date}")
    
    # --------------------------
    # Format numeric values (remove decimal places)
    # --------------------------
    # Convert numeric fields to integers to remove decimals
    numeric_fields = ["TRANSACTIONAMOUNT"]
    
    for field in numeric_fields:
        if field in df_new.columns:
            try:
                # Special handling for TRANSACTIONAMOUNT to preserve negative signs
                if field == "TRANSACTIONAMOUNT":
                    df_new[field] = df_new[field].apply(
                        lambda x: str(int(float(x))) if pd.notna(x) else "0"
                    )
                else:
                    df_new[field] = df_new[field].apply(
                        lambda x: str(int(float(x))) if pd.notna(x) else "0"
                    )
            except Exception as e:
                print(f"Warning: Error converting {field} to integer: {e}")
    
    print("Formatted numeric fields")
    
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
    
    
    # Remove any records missing ACCOUNTNUMBER and drop duplicates
    df_new = df_new[(df_new['CUSTOMERID'] != "") & (df_new['LOCATIONID'] != "") & (df_new['TRANSACTIONDATE'] != "")]
    
    # --------------------------
    # Reorder columns based on target format
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
    output_path = os.path.join(os.path.dirname(list(file_paths.values())[0]), 'STAGE_TRANSACTIONAL_HIST.csv')
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
        
    # Print execution time
    end_time = time.time()
    total_time = end_time - start_time
    print(f"\nTotal script execution time: {total_time:.2f} seconds")
else:
    print("Error: DFKKOP data is required but not available.")