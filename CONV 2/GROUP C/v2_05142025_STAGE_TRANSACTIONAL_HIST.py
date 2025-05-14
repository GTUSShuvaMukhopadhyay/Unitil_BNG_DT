# v2_05142025_STAGE_TRANSACTIONAL_HIST.py
# Script to process transactional history data for conversion

# ISSUES : TRANSACTIONTYPE, TRANSACTIONDESCRIPTION: 04/21/2025
# Issues: TRANSACTIONTYPE, TRANSACTIONDESCRIPTION, BILLTYPE, TENDERTYPE - 05/14/2025
# Updated: 05/14/2025
# Resolved issues with TRANSACTIONTYPE, TRANSACTIONDESCRIPTION, BILLTYPE, TENDERTYPE using updated mapping and new data sources

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
    "DFKKOP_2019": r"C:\Users\us85360\Desktop\STAGE_TRANSACTIONAL_HIST\DFKKOP 01012019 to 12312019.XLSX",
    "DFKKOP_2020": r"C:\Users\us85360\Desktop\STAGE_TRANSACTIONAL_HIST\DFKKOP 01012020 to 12312020.XLSX",
    "DFKKOP_2021": r"C:\Users\us85360\Desktop\STAGE_TRANSACTIONAL_HIST\DFKKOP 01012021 to 12312021.XLSX",
    "DFKKOP_2022": r"C:\Users\us85360\Desktop\STAGE_TRANSACTIONAL_HIST\Dfkkop 01012022 to 12312022.XLSX",
    "DFKKOP_2023": r"C:\Users\us85360\Desktop\STAGE_TRANSACTIONAL_HIST\Dfkkop 01012023 to 12312023.XLSX",
    "DFKKOP_2024": r"C:\Users\us85360\Desktop\STAGE_TRANSACTIONAL_HIST\DFKKOP 01012024 to 02132025.XLSX",
    
    # Other sources
    "EVER": r"C:\Users\us85360\Desktop\STAGE_TRANSACTIONAL_HIST\EVER.XLSX",
    "ZDM_PREMDETAILS": r"C:\Users\us85360\Desktop\STAGE_TRANSACTIONAL_HIST\ZDM_PREMDETAILS.XLSX",
    
    # Adding DFKKZP source for TENDERTYPE mapping
    "DFKKZP": r"C:\Users\us85360\Desktop\STAGE_TRANSACTIONAL_HIST\dfkkzp v2.xlsx"
}

# Initialize data_sources dictionary to hold our data
data_sources = {}

# Function to read an Excel file
def read_excel_file(name, path):
    try:
        # Specify dtype to keep MTrans as string
        dtype_dict = {'MTrans': str, 'STrans': str}
        df = pd.read_excel(path, sheet_name="Sheet1", engine="openpyxl", dtype=dtype_dict)
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
dfkkop_keys = [key for key in data_sources.keys() if key.startswith("DFKKOP_")]
if dfkkop_keys:
    dfkkop_dfs = [data_sources[key] for key in dfkkop_keys if data_sources[key] is not None]
    if dfkkop_dfs:
        data_sources["DFKKOP"] = pd.concat(dfkkop_dfs, ignore_index=True)
        print(f"Combined {len(dfkkop_dfs)} DFKKOP files into a single dataframe with {len(data_sources['DFKKOP'])} rows")
    else:
        print("Warning: No valid DFKKOP dataframes found to combine")
else:
    print("Warning: No DFKKOP files found in data_sources")

# Add this debug code to check data types of key columns
if data_sources.get("DFKKOP") is not None:
    # Ensure MTrans and STrans are treated as strings
    data_sources["DFKKOP"]["MTrans"] = data_sources["DFKKOP"]["MTrans"].astype(str)
    data_sources["DFKKOP"]["STrans"] = data_sources["DFKKOP"]["STrans"].astype(str)
    
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

# Initialize output DataFrame (df_new)
df_new = pd.DataFrame()

print("\nStarting field extraction and transformation...")

# --------------------------
# Extract CUSTOMERID from DFKKOP (BPartner column)
# --------------------------
if data_sources.get("DFKKOP") is not None:
    df_new["CUSTOMERID"] = data_sources["DFKKOP"]["BPartner"].apply(
        lambda x: str(int(x)) if pd.notna(x) and isinstance(x, (int, float)) else str(x)
    ).str.slice(0, 15)
    print(f"Extracted {len(df_new)} CUSTOMERID values")

# --------------------------
# Extract LOCATIONID through direct merge between DFKKOP, EVER and ZDM_PREMDETAILS
# --------------------------
if data_sources.get("DFKKOP") is not None and data_sources.get("ZDM_PREMDETAILS") is not None:
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
    
    # Try different formats for matching
    if len(overlap) == 0:
        print("No direct match found. Trying with formatted Contract Account...")
        # Try formatting with different approaches
        
        # Approach 1: Format ZDM with no leading zeros
        location_lookup["Contract Account Formatted"] = location_lookup["Contract Account"].apply(
            lambda x: str(int(x)) if pd.notna(x) and x.replace('0', '').strip() else x
        )
        
        # Approach 2: Format DFKKOP with leading zeros
        df_new["Cont.Account Formatted"] = df_new["Cont.Account"].apply(
            lambda x: x.zfill(12) if pd.notna(x) else x
        )
        
        # Check overlap with formatted values
        formatted_dfkkop_ca_set = set(df_new["Cont.Account Formatted"].unique())
        formatted_zdm_ca_set = set(location_lookup["Contract Account Formatted"].unique())
        formatted_overlap = formatted_dfkkop_ca_set.intersection(formatted_zdm_ca_set)
        print(f"Formatted Contract Account overlap: {len(formatted_overlap)} values")
        
        # Try both merges
        if len(formatted_overlap) > 0:
            # First try merge with formatted keys
            df_new = df_new.merge(
                location_lookup[["Contract Account Formatted", "LOCATIONID"]],
                how="left",
                left_on="Cont.Account Formatted",
                right_on="Contract Account Formatted"
            )
            print(f"Merged with formatted keys. LOCATIONID matches: {df_new['LOCATIONID'].notna().sum()}")
        else:
            # Fall back to original fields
            df_new = df_new.merge(
                location_lookup[["Contract Account", "LOCATIONID"]],
                how="left",
                left_on="Cont.Account",
                right_on="Contract Account"
            )
            print(f"Merged with original keys. LOCATIONID matches: {df_new['LOCATIONID'].notna().sum()}")
    else:
        # Use original merge if overlap exists
        df_new = df_new.merge(
            location_lookup,
            how="left",
            left_on="Cont.Account",
            right_on="Contract Account"
        )
        print(f"Merged with original keys. LOCATIONID matches: {df_new['LOCATIONID'].notna().sum()}")
    
    # Drop helper columns
    drop_cols = [col for col in ["Contract Account", "Contract Account Formatted", "Cont.Account Formatted"] 
                if col in df_new.columns]
    if drop_cols:
        df_new.drop(columns=drop_cols, inplace=True)
    
    # If we still have missing LOCATIONID values, try using EVER for a fallback
    if df_new["LOCATIONID"].isna().sum() > 0 and data_sources.get("EVER") is not None:
        print(f"Still missing {df_new['LOCATIONID'].isna().sum()} LOCATIONID values, trying EVER fallback")
        
        # Create a mapping from Cont.Account to Installation in EVER
        ever_df = data_sources["EVER"].copy()
        ever_df["Cont.Account"] = ever_df["Cont.Account"].astype(str).str.strip()
        ever_df["Installation"] = ever_df["Installat."].astype(str).str.strip()
        
        # Create a clean lookup dictionary
        ca_to_install = {}
        for ca, install in zip(ever_df["Cont.Account"], ever_df["Installation"]):
            if pd.notna(ca) and pd.notna(install) and ca and install:
                ca_to_install[ca] = install
        
        # Create a clean lookup from Installation to Premise
        install_to_premise = {}
        for install, premise in zip(zdm_df["Installation"].astype(str), zdm_df["Premise"].astype(str)):
            if pd.notna(install) and pd.notna(premise) and install and premise:
                install_to_premise[install] = premise
        
        # Apply the mappings for rows with missing LOCATIONID
        missing_mask = df_new["LOCATIONID"].isna()
        missing_count = missing_mask.sum()
        found_count = 0
        
        for i, row in df_new[missing_mask].iterrows():
            ca = row["Cont.Account"]
            if ca in ca_to_install:
                install = ca_to_install[ca]
                if install in install_to_premise:
                    df_new.at[i, "LOCATIONID"] = install_to_premise[install]
                    found_count += 1
        
        print(f"Found {found_count} additional LOCATIONID values through EVER mapping")
        print(f"After fallback: Rows with non-empty LOCATIONID: {df_new['LOCATIONID'].notna().sum()}")
    
    # Drop the Cont.Account temporary column
    if "Cont.Account" in df_new.columns:
        df_new.drop(columns=["Cont.Account"], inplace=True)
        
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
# UPDATED: Extract TRANSACTIONTYPE, TRANSACTIONDESCRIPTION, BILLTYPE based on MTrans + STrans mapping
# --------------------------
if data_sources.get("DFKKOP") is not None:
    # Debug info before mapping
    print("\nSetting up MTrans + STrans mapping for multiple fields...")
    
    # Ensure MTrans and STrans are treated as strings
    data_sources["DFKKOP"]["MTrans"] = data_sources["DFKKOP"]["MTrans"].astype(str)
    data_sources["DFKKOP"]["STrans"] = data_sources["DFKKOP"]["STrans"].astype(str)
    
    # Normalize format by padding with leading zeros (4 digits for consistency)
    data_sources["DFKKOP"]["MTrans_normalized"] = data_sources["DFKKOP"]["MTrans"].apply(
        lambda x: str(x).zfill(4) if x.replace('.', '', 1).isdigit() else x
    )
    data_sources["DFKKOP"]["STrans_normalized"] = data_sources["DFKKOP"]["STrans"].apply(
        lambda x: str(x).zfill(4) if x.replace('.', '', 1).isdigit() else x
    )
    
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
    
    # Add aliases for without leading zeros for common cases (for robustness)
    # This creates additional entries like ("15", "10") that point to the same mapping as ("0015", "0010")
    aliases = []
    for (mtrans, strans), mapping in mtrans_strans_mapping.items():
        try:
            # Create variant with no leading zeros
            mtrans_no_zeros = str(int(mtrans))
            strans_no_zeros = str(int(strans))
            aliases.append(((mtrans_no_zeros, strans_no_zeros), mapping))
            
            # Create mixed variants
            aliases.append(((mtrans, strans_no_zeros), mapping))
            aliases.append(((mtrans_no_zeros, strans), mapping))
        except ValueError:
            # Skip if not convertible to int (e.g., non-numeric codes)
            pass
    
    # Add aliases to the main mapping
    for alias, mapping in aliases:
        if alias not in mtrans_strans_mapping:
            mtrans_strans_mapping[alias] = mapping
    
    # Default values for combinations not in the mapping
    default_mapping = {
        "TRANSACTIONTYPE": "99",
        "TRANSACTIONDESCRIPTION": "Other Transaction",
        "BILLTYPE": "0"
    }
    
    # Function to apply mapping based on MTrans and STrans
    def apply_mtrans_strans_mapping(mtrans, strans):
        # Try different combinations
        key = (mtrans, strans)
        if key in mtrans_strans_mapping:
            return mtrans_strans_mapping[key]
        
        # Try normalized values
        mtrans_norm = mtrans.zfill(4) if mtrans.replace('.', '', 1).isdigit() else mtrans
        strans_norm = strans.zfill(4) if strans.replace('.', '', 1).isdigit() else strans
        key_norm = (mtrans_norm, strans_norm)
        if key_norm in mtrans_strans_mapping:
            return mtrans_strans_mapping[key_norm]
        
        # Try with MTrans only (for backward compatibility)
        mtrans_mapping = {
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
        # Add number without leading zeros
        for k, v in list(mtrans_mapping.items()):
            try:
                # Add variant with no leading zeros
                k_no_zeros = str(int(k))
                mtrans_mapping[k_no_zeros] = v
            except ValueError:
                # Skip non-numeric keys
                pass
        
        if mtrans in mtrans_mapping:
            return mtrans_mapping[mtrans]
        if mtrans_norm in mtrans_mapping:
            return mtrans_mapping[mtrans_norm]
        
        # Return default if no match
        return default_mapping
    
    # Extract source data (copy to avoid modifying original)
    mtrans_column = data_sources["DFKKOP"]["MTrans"].copy()
    strans_column = data_sources["DFKKOP"]["STrans"].copy()
    
    # Apply the mapping function to each row
    for idx, (mtrans, strans) in enumerate(zip(mtrans_column, strans_column)):
        mapping_result = apply_mtrans_strans_mapping(str(mtrans), str(strans))
        
        # Assign values to corresponding columns
        df_new.at[idx, "TRANSACTIONTYPE"] = mapping_result["TRANSACTIONTYPE"]
        df_new.at[idx, "TRANSACTIONDESCRIPTION"] = mapping_result["TRANSACTIONDESCRIPTION"]
        df_new.at[idx, "BILLTYPE"] = mapping_result["BILLTYPE"]
    
    # Print mapping statistics
    print(f"Applied MTrans + STrans mapping")
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
# UPDATED: Determine TENDERTYPE based on DFKKZP mapping
# --------------------------
# First set default value
df_new["TENDERTYPE"] = ""

# For payments (TRANSACTIONTYPE=1), set to "98" as default
# This will be overridden with more specific values if found in DFKKZP
df_new.loc[df_new["TRANSACTIONTYPE"] == "1", "TENDERTYPE"] = "98"

# If DFKKZP is available, use it to map tender types
if data_sources.get("DFKKZP") is not None and "DFKKOP" in data_sources:
    print("\nMapping TENDERTYPE from DFKKZP...")
    
    # Define mapping from DFKKZP DT (Document Type) to TENDERTYPE
    dt_to_tendertype = {
        "CA": "70",  # Cash-others → CASH.BNG
        "CK": "71",  # Check-others → CHECK.BNG
        "CR": "81",  # Credit Card (CC) Pmt → WEB CC.BNG
        "WD": "84",  # Wired payments → EFTPYMT.BNG
        "UB": "81",  # 1 Time CC Pmt Utill → WEB CC.BNG
        "IB": "79",  # 1 Time CC IVR Utill → IVR CC.BNG
        "UK": "80",  # 1 Time CK WEB Utili → WEB ACH.BNG
        "IK": "77",  # 1 Time CK IVR Utill → IVR ACH.BNG
        "CP": "7",   # Customer Payment → ACH-ALT
    }
    
    # Prepare DFKKZP data
    dfkkzp_df = data_sources["DFKKZP"].copy()
    
    # Make sure relevant columns are strings
    if "Selection Value 1" in dfkkzp_df.columns:
        dfkkzp_df["Selection Value 1"] = dfkkzp_df["Selection Value 1"].astype(str).str.strip()
    if "DT" in dfkkzp_df.columns:
        dfkkzp_df["DT"] = dfkkzp_df["DT"].astype(str).str.strip()
    
    # Add column for mapping Contract Account in DFKKOP
    if "Cont.Account" in data_sources["DFKKOP"].columns:
        # Ensure it's a string and properly formatted
        ca_series = data_sources["DFKKOP"]["Cont.Account"].astype(str).str.strip()
        df_new["Cont.Account"] = ca_series
        
        # Create a mapping of Doc. No. to DFKKOP index
        doc_to_idx = {}
        if "Doc. No." in data_sources["DFKKOP"].columns:
            for idx, doc_no in enumerate(data_sources["DFKKOP"]["Doc. No."]):
                if pd.notna(doc_no):
                    doc_to_idx[str(doc_no).strip()] = idx
        
        # Check for returned payments column (R flag)
        r_flag_col = None
        for col in dfkkzp_df.columns:
            if col == "R" or "Column H" in str(col):
                r_flag_col = col
                print(f"Found returned payments flag column: {r_flag_col}")
                break
        
        # Create a mapping from Contract Account and Doc. No. to Tender Type
        ca_doc_to_tendertype = {}
        returned_payments = set()
        
        for _, row in dfkkzp_df.iterrows():
            if "Selection Value 1" in row and "Doc. No." in row and "DT" in row:
                ca = str(row["Selection Value 1"]).strip()
                doc_no = str(row["Doc. No."]).strip()
                dt = str(row["DT"]).strip()
                
                # Check for returned payment flag
                is_returned = False
                if r_flag_col and r_flag_col in row:
                    # Check for any non-null, non-zero value in the R column
                    r_val = row[r_flag_col]
                    if pd.notna(r_val) and str(r_val).strip() not in ["", "0"]:
                        is_returned = True
                        returned_payments.add((ca, doc_no))
                
                # Determine TENDERTYPE
                if is_returned:
                    tendertype = "94"  # Customer Returned Payment
                elif dt in dt_to_tendertype:
                    tendertype = dt_to_tendertype[dt]
                else:
                    tendertype = "98"  # Default for payments not specifically mapped
                
                # Store in our mapping
                ca_doc_to_tendertype[(ca, doc_no)] = tendertype
        
        print(f"Found {len(ca_doc_to_tendertype)} payment records in DFKKZP")
        print(f"Found {len(returned_payments)} returned payments")
        
        # Apply the mapping to df_new
        payment_matches = 0
        
        # Method 1: Try to match by Contract Account and Doc. No.
        for idx, row in df_new.iterrows():
            if row["TRANSACTIONTYPE"] == "1":  # Only apply to payments
                ca = str(row["Cont.Account"]).strip() if "Cont.Account" in row else ""
                doc_no = str(row["BILLORINVOICENUMBER"]).strip() if "BILLORINVOICENUMBER" in row else ""
                
                # Try to find in our mapping
                if (ca, doc_no) in ca_doc_to_tendertype:
                    df_new.at[idx, "TENDERTYPE"] = ca_doc_to_tendertype[(ca, doc_no)]
                    payment_matches += 1
                    
                    # If this is a returned payment, update the description
                    if (ca, doc_no) in returned_payments:
                        df_new.at[idx, "TRANSACTIONDESCRIPTION"] = "Customer Returned Payment"
        
        print(f"Matched {payment_matches} payments with specific TENDERTYPE values")
        
        # Method 2: For any remaining payments, try to match by Doc. No. only
        if payment_matches < (df_new["TRANSACTIONTYPE"] == "1").sum():
            remaining_payments = 0
            for idx, row in df_new.iterrows():
                if row["TRANSACTIONTYPE"] == "1" and row["TENDERTYPE"] == "98":
                    doc_no = str(row["BILLORINVOICENUMBER"]).strip() if "BILLORINVOICENUMBER" in row else ""
                    
                    # Look for any match with this Doc. No.
                    for (ca, doc_no_key), tendertype in ca_doc_to_tendertype.items():
                        if doc_no == doc_no_key:
                            df_new.at[idx, "TENDERTYPE"] = tendertype
                            remaining_payments += 1
                            
                            # Check if it's a returned payment
                            if (ca, doc_no) in returned_payments:
                                df_new.at[idx, "TRANSACTIONDESCRIPTION"] = "Customer Returned Payment"
                            
                            break
            
            print(f"Matched {remaining_payments} additional payments by Doc. No. only")
    
    # Drop temporary columns
    if "Cont.Account" in df_new.columns:
        df_new.drop(columns=["Cont.Account"], inplace=True)
    
    # Print TENDERTYPE statistics
    print(f"TENDERTYPE distribution: {df_new['TENDERTYPE'].value_counts().to_dict()}")

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
output_path = os.path.join(os.path.dirname(list(file_paths.values())[0]), 'v2_05142025_STAGE_TRANSACTIONAL_HIST.csv')
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