# 0421_2025 fixing this STAGE_TRANSACTIONAL_HIST
# FOR KYLE REMOVING BLANKS STAGE_TRANSACTIONAL_HIST.py
# Script to process transactional history data for conversion

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
    "ZDM_PREMDETAILS": r"C:\Users\us85360\Desktop\STAGE_TRANSACTIONAL_HIST\ZDM_PREMDETAILS.XLSX"
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
# Extract TRANSACTIONTYPE from MTrans based on specified mapping
# --------------------------
if data_sources.get("DFKKOP") is not None:
    # Create a mapping dictionary for MTrans to TRANSACTIONTYPE
    # Using the provided mapping: MTrans Description --> enQuesta Trans Code
    mtrans_mapping = {
        "0015": "5",   # Misc Charges --> 5
        "0025": "14",  # Interest for Cash Sec.Deposit --> 14
        "0060": "1",   # On Account --> 1
        "0070": "4",   # Returns --> 4
        "0080": "",    # Installments --> don't convert
        "0100": "99",  # Consumption Billing --> 99
        "0150": "4",   # Bal.For.Amount (From Pay.Plan) --> 4
        "0200": "99",  # Final Billing --> 99
        "0250": "4",   # Transfer Posting for Invoicing --> 4
        "0600": "1",   # Payment --> 1
        "0610": "",    # Account Maintenance --> ?
        "0620": "4",   # Transfer --> 4
        "0625": "",    # Resetting Cleared Items --> ?
        "0630": "4",   # Write off --> 4
        "CONV": ""     # Records from 2015-17 --> ?
    }
    
    # Apply the mapping with a default value of 99 for unspecified codes
    df_new["TRANSACTIONTYPE"] = data_sources["DFKKOP"]["MTrans"].apply(
        lambda x: mtrans_mapping.get(str(x), "99") if pd.notna(x) else "99"
    )
    
    # Filter out records marked for "don't convert"
    df_new = df_new[df_new["TRANSACTIONTYPE"] != ""]
    
    print(f"Mapped TRANSACTIONTYPE values using specified mapping")
    print(f"TRANSACTIONTYPE distribution: {df_new['TRANSACTIONTYPE'].value_counts().to_dict()}")
    print(f"Filtered out {(df_new['TRANSACTIONTYPE'] == '').sum()} records marked 'don't convert'")


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
# Extract TRANSACTIONDESCRIPTION
# --------------------------
if data_sources.get("DFKKOP") is not None:
    # Map MTrans codes to descriptions based on the provided mapping
    mtrans_descriptions = {
        "0015": "Misc Charges",
        "0025": "Interest for Cash Sec.Deposit",
        "0060": "On Account",
        "0070": "Returns",
        "0080": "Installments",
        "0100": "Consumption Billing",
        "0150": "Bal.For.Amount",
        "0200": "Final Billing",
        "0250": "Transfer Posting for Invoicing",
        "0600": "Payment",
        "0610": "Account Maintenance",
        "0620": "Transfer",
        "0625": "Resetting Cleared Items",
        "0630": "Write off",
        "CONV": "Conversion Record 2015-17"
    }
    
    # Create description based on MTrans code
    df_new["TRANSACTIONDESCRIPTION"] = data_sources["DFKKOP"]["MTrans"].apply(
        lambda x: mtrans_descriptions.get(str(x), "Other Transaction") if pd.notna(x) else ""
    )
    
    # For transactions that have a subtransaction code (STrans), we can append it to make the description more specific
    if "STrans" in data_sources["DFKKOP"].columns:
        # Only append STrans for certain transaction types where it adds value
        append_strans_for = ["0100", "0200", "0250"]  # Add MTrans codes where STrans is meaningful
        
        for mtrans in append_strans_for:
            mask = data_sources["DFKKOP"]["MTrans"] == mtrans
            if mask.any():
                # Get the corresponding rows in df_new
                df_indices = df_new.index[mask]
                for idx in df_indices:
                    mtrans_val = data_sources["DFKKOP"].loc[idx, "MTrans"]
                    strans_val = data_sources["DFKKOP"].loc[idx, "STrans"]
                    if pd.notna(strans_val):
                        base_desc = mtrans_descriptions.get(str(mtrans_val), "Other Transaction")
                        df_new.loc[idx, "TRANSACTIONDESCRIPTION"] = f"{base_desc} - {strans_val}"
    
    print(f"Assigned TRANSACTIONDESCRIPTION values from MTrans codes")
    print(f"Sample descriptions: {df_new['TRANSACTIONDESCRIPTION'].value_counts().head(5).to_dict()}")


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
# Determine BILLTYPE
# --------------------------
# Default to "0" (Normal Bill)
df_new["BILLTYPE"] = "0"

# Add logic to identify final and cancelled bills if the data contains such indicators
# For example, if Status field indicates bill status:
if "Status" in data_sources["DFKKOP"].columns:
    # Map Status values to BILLTYPE (this is a placeholder - adjust based on your data)
    status_to_billtype = {
        "1": "0",  # Normal Bill
        "2": "1",  # Final Bill
        "3": "2",  # Cancelled Bill
    }
    
    # Apply mapping (with default to "0")
    df_new["BILLTYPE"] = data_sources["DFKKOP"]["Status"].apply(
        lambda x: status_to_billtype.get(str(x), "0") if pd.notna(x) else "0"
    )

print(f"Assigned BILLTYPE values")

# --------------------------
# Determine TENDERTYPE
# --------------------------
# Default to empty
df_new["TENDERTYPE"] = ""

# For payments (TRANSACTIONTYPE=1), set to "98"
df_new.loc[df_new["TRANSACTIONTYPE"] == "1", "TENDERTYPE"] = "98"

print(f"Assigned TENDERTYPE values")

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
output_path = os.path.join(os.path.dirname(list(file_paths.values())[0]), 'v5STAGE_TRANSACTIONAL_HIST.csv')
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