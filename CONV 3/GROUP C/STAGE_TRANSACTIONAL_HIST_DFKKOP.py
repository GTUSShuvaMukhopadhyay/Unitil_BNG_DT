# STAGE_TRANSACTIONAL_HIST_DFKKOP.py
# Performance improvements without changing field logic


# excluded all reocrds with mtrans 0100 and strans of 0002
# cutoff at 6 years


import pandas as pd
import os
import csv
import concurrent.futures
from datetime import datetime, timedelta
import pickle
import numpy as np

# Define specific date range for filtering (replaces the 6-year cutoff)
START_DATE = pd.to_datetime("2019-06-01")
END_DATE = pd.to_datetime("2025-06-14")
print(f"Applying date range filter: {START_DATE.date()} to {END_DATE.date()}")

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
    "DFKKOP5": r"c:\Users\GTUSER1\Documents\CONV 3\DFKKOP\DFKKOP 01012019 to 12312019.XLSX",
    "DFKKOP6": r"c:\Users\GTUSER1\Documents\CONV 3\DFKKOP\DFKKOP 01012020 to 12312020.XLSX",
    "DFKKOP7": r"c:\Users\GTUSER1\Documents\CONV 3\DFKKOP\DFKKOP 01012021 to 12312021.XLSX",
    "DFKKOP8": r"c:\Users\GTUSER1\Documents\CONV 3\DFKKOP\DFKKOP 01012022 to 12312022.XLSX",
    "DFKKOP9": r"c:\Users\GTUSER1\Documents\CONV 3\DFKKOP\DFKKOP 01012023 to 12312023.XLSX",
    "DFKKOP10": r"c:\Users\GTUSER1\Documents\CONV 3\DFKKOP\DFKKOP 01012024 to 12312024.XLSX",
    "DFKKOP11": r"c:\Users\GTUSER1\Documents\CONV 3\DFKKOP\DFKKOP 01012025 to 12312025.XLSX",
    # Other sources
    "EVER": r"c:\Users\GTUSER1\Documents\CONV 3\EVER - 0802.XLSX",
    "ZDM_PREMDETAILS": r"c:\Users\GTUSER1\Documents\CONV 3\ZDM_PREMDETAILS.XLSX",
}

# OPTIMIZATION 1: Check for cached parquet files and use them if available
cache_dir = os.path.join(os.path.dirname(list(file_paths.values())[0]), "cache")
os.makedirs(cache_dir, exist_ok=True)


def get_cache_path(name):
    return os.path.join(cache_dir, f"{name}.parquet")


def get_file_mtime(path):
    """Get file modification time"""
    try:
        return os.path.getmtime(path)
    except:
        return 0


def should_use_cache(name, path):
    """Check if cached version is newer than source file"""
    cache_path = get_cache_path(name)
    if not os.path.exists(cache_path):
        return False

    cache_mtime = get_file_mtime(cache_path)
    source_mtime = get_file_mtime(path)

    return cache_mtime > source_mtime


# OPTIMIZATION 2: Improved file reading with caching and filtering
def read_excel_file_with_filter(name, path):
    try:
        # Check if we can use cached version
        if should_use_cache(name, path):
            print(f"Using cached data for {name}")
            df = pd.read_parquet(get_cache_path(name))
        else:
            print(f"Loading and caching {name}...")
            df = pd.read_excel(path, sheet_name="Sheet1", engine="openpyxl")

            # Apply date filtering for DFKKOP files (same as consumption script)
            if name.startswith("DFKKOP"):
                # Filter by Doc. Date (same logic as ZMECON filtering)
                if "Doc. Date" in df.columns:
                    date_series = pd.to_datetime(df["Doc. Date"], errors='coerce')
                    original_count = len(df)
                    start_date = pd.to_datetime("2019-06-01")
                    end_date = pd.to_datetime("2025-06-14")
                    mask = (date_series >= start_date) & (date_series <= end_date)
                    df = df[mask]
                    print(f"Date filtered {name}: {original_count} → {len(df)} rows")

            # Cache the filtered data
            df.to_parquet(get_cache_path(name), index=False)

        print(f"Successfully loaded {name}: {df.shape[0]} rows, {df.shape[1]} columns")
        return name, df
    except Exception as e:
        print(f"Error loading {name}: {e}")
        return name, None


# Initialize data_sources dictionary to hold our data
data_sources = {}

# OPTIMIZATION 3: Use parallel loading with more threads
print("Loading data sources in parallel...")
max_workers = min(8, len(file_paths))  # Use up to 8 threads
with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = {executor.submit(read_excel_file_with_filter, name, path): name for name, path in file_paths.items()}
    for future in concurrent.futures.as_completed(futures):
        name, df = future.result()
        data_sources[name] = df

# Combine all DFKKOP files into a single dataframe
dfkkop_keys = [key for key in data_sources.keys() if key.startswith("DFKKOP")]
if dfkkop_keys:
    dfkkop_dfs = [data_sources[key] for key in dfkkop_keys if data_sources[key] is not None]
    if dfkkop_dfs:
        data_sources["DFKKOP"] = pd.concat(dfkkop_dfs, ignore_index=True)
        print(
            f"Combined {len(dfkkop_dfs)} DFKKOP files into a single dataframe with {len(data_sources['DFKKOP'])} rows")

        # OPTIMIZATION 4: Vectorized MTrans and STrans formatting
        print("Standardizing MTrans and STrans formatting (vectorized)...")

        # Vectorized formatting - much faster than apply()
        mtrans_mask = data_sources["DFKKOP"]["MTrans"].notna() & data_sources["DFKKOP"]["MTrans"].apply(
            lambda x: isinstance(x, (int, float)))
        strans_mask = data_sources["DFKKOP"]["STrans"].notna() & data_sources["DFKKOP"]["STrans"].apply(
            lambda x: isinstance(x, (int, float)))

        data_sources["DFKKOP"]["MTrans"] = data_sources["DFKKOP"]["MTrans"].astype(str)
        data_sources["DFKKOP"]["STrans"] = data_sources["DFKKOP"]["STrans"].astype(str)

        # Apply formatting only where needed
        data_sources["DFKKOP"].loc[mtrans_mask, "MTrans"] = data_sources["DFKKOP"].loc[mtrans_mask, "MTrans"].apply(
            lambda x: "{:04d}".format(int(float(x)))
        )
        data_sources["DFKKOP"].loc[strans_mask, "STrans"] = data_sources["DFKKOP"].loc[strans_mask, "STrans"].apply(
            lambda x: "{:04d}".format(int(float(x)))
        )

        print("MTrans and STrans formatting standardized")

    else:
        print("Warning: No valid DFKKOP dataframes found to combine")
else:
    print("Warning: No DFKKOP files found in data_sources")

# Diagnostic: Check what MTrans/STrans values actually exist in the data
print("\nDiagnostic: Checking MTrans/STrans values in DFKKOP...")
print("Sample MTrans values:", data_sources["DFKKOP"]["MTrans"].head(20).tolist())
print("Sample STrans values:", data_sources["DFKKOP"]["STrans"].head(20).tolist())

# OPTIMIZATION 5: Use numpy for faster filtering
print("\nFiltering DFKKOP for valid MTrans/STrans combinations...")

# Define the 19 valid combinations as a set for O(1) lookup
valid_combinations = {
    ("0015", "0010"), ("0015", "0020"), ("0015", "0021"), ("0015", "0030"),
    ("0015", "0040"), ("0015", "0070"), ("0015", "0230"), ("0015", "0231"),
    ("0015", "0300"), ("0015", "0370"), ("0015", "0371"), ("0025", "0010"),
    ("0070", "0010"), ("0080", "0005"), ("0080", "0010"), ("0100", "0002"),
    ("0200", "0002"), ("0620", "0010"), ("0630", "0010")
}

# Store original count
original_count = len(data_sources["DFKKOP"])

# OPTIMIZATION 6: Vectorized filtering using numpy
mtrans_array = data_sources["DFKKOP"]["MTrans"].astype(str).str.strip().values
strans_array = data_sources["DFKKOP"]["STrans"].astype(str).str.strip().values

# Create boolean mask for valid combinations
valid_mask = np.array([
    (mtrans, strans) in valid_combinations
    for mtrans, strans in zip(mtrans_array, strans_array)
])

data_sources["DFKKOP"] = data_sources["DFKKOP"][valid_mask]
filtered_count = len(data_sources["DFKKOP"])
print(f"Filtered DFKKOP from {original_count:,} to {filtered_count:,} records")
print(f"Reduction: {((original_count - filtered_count) / original_count * 100):.2f}%")

# Check if we have any data left after filtering
if len(data_sources["DFKKOP"]) == 0:
    print("\nERROR: No valid MTrans/STrans combinations found in the data!")
    print("The filtering removed all records. Please check the MTrans/STrans values in your data.")
    print("Exiting to prevent further errors...")
    exit(1)

# Initialize output DataFrame (df_new) directly from filtered DFKKOP
df_new = data_sources["DFKKOP"].copy()
print(f"Created df_new with {len(df_new)} rows from filtered DFKKOP")

# OPTIMIZATION 7: Extract all basic fields at once using vectorized operations
print("Extracting basic fields (vectorized)...")


# FIXED: Define the robust customer ID cleaning function (was missing in original)
def clean_customerid(value):
    """Robust customer ID cleaning"""
    if pd.isna(value):
        return ""

    str_value = str(value).strip().strip('"\'')

    if not str_value or str_value.lower() in ['nan', 'none', 'null']:
        return ""

    # Handle numeric values safely
    if str_value.replace('.', '').replace('-', '').isdigit():
        try:
            if '.' in str_value:
                float_val = float(str_value)
                if float_val.is_integer():
                    str_value = str(int(float_val))
            else:
                str_value = str(int(str_value))
        except (ValueError, OverflowError):
            pass

    # Remove leading zeros unless it's all zeros
    if str_value.isdigit() and len(str_value) > 1:
        str_value = str_value.lstrip('0') or '0'

    return str_value[:15]


# Apply the robust cleaning to BPartner field
print("Extracting CUSTOMERID from BPartner with error handling...")
raw_bpartner = df_new["BPartner"]
print(f"Raw BPartner: {len(raw_bpartner)} records, type: {raw_bpartner.dtype}")
print(f"Sample values: {raw_bpartner.head(10).tolist()}")

df_new["CUSTOMERID"] = raw_bpartner.apply(clean_customerid)

# Validate results
valid_count = (df_new["CUSTOMERID"] != "").sum()
print(f"Valid CUSTOMERID extracted: {valid_count}/{len(df_new)} ({valid_count / len(df_new) * 100:.1f}%)")

# Diagnostic check for CUSTOMERID issues
print("\nCUSTOMERID DIAGNOSTIC CHECK:")
print(f"Empty CUSTOMERID count: {(df_new['CUSTOMERID'] == '').sum()}")
print(f"Unique customers: {df_new['CUSTOMERID'].nunique()}")
print(f"Length distribution: {df_new['CUSTOMERID'].str.len().value_counts().sort_index().to_dict()}")

# Check for potential issues
non_numeric = df_new['CUSTOMERID'][~df_new['CUSTOMERID'].str.isdigit() & (df_new['CUSTOMERID'] != '')]
if len(non_numeric) > 0:
    print(f"Non-numeric CUSTOMERID found: {len(non_numeric)}")
    print(f"Samples: {non_numeric.head(10).tolist()}")

# Extract date fields using vectorized operations
df_new["TRANSACTIONDATE"] = pd.to_datetime(df_new["Doc. Date"], errors='coerce').dt.strftime('%Y-%m-%d')
df_new["BILLINGDATE"] = pd.to_datetime(df_new["Pstng Date"], errors='coerce').dt.strftime('%Y-%m-%d')
df_new["DUEDATE"] = pd.to_datetime(df_new["Due"], errors='coerce').dt.strftime('%Y-%m-%d')


# FIXED: Define robust function for BILLORINVOICENUMBER extraction
def clean_billorinvoice(value):
    """Robust bill/invoice number cleaning"""
    if pd.isna(value):
        return ""

    str_value = str(value).strip()

    if not str_value or str_value.lower() in ['nan', 'none', 'null']:
        return ""

    # Handle numeric values safely
    if str_value.replace('.', '').replace('-', '').isdigit():
        try:
            if '.' in str_value:
                float_val = float(str_value)
                if float_val.is_integer():
                    int_val = int(float_val)
                else:
                    return ""  # Don't process non-integer floats
            else:
                int_val = int(str_value)

            # Apply the [2:10] slicing logic (remove first 2 chars, take next 8)
            str_result = str(int_val)
            if len(str_result) > 2:
                return str_result[2:10]
            else:
                return ""  # Too short to slice

        except (ValueError, OverflowError):
            return ""

    return ""  # Non-numeric values return empty


# Extract BILLORINVOICENUMBER with robust error handling
df_new["BILLORINVOICENUMBER"] = df_new["Reference"].apply(clean_billorinvoice)

print(f"Extracted basic fields for {len(df_new)} records")

# OPTIMIZATION 8: Optimized LOCATIONID extraction with pre-built lookup
if data_sources.get("ZDM_PREMDETAILS") is not None:
    print("\nExtracting LOCATIONID with optimized lookup...")

    zdm_df = data_sources["ZDM_PREMDETAILS"].copy()


    # FIXED: Define robust contract account cleaning function
    def clean_contract_account(value):
        """Robust contract account cleaning"""
        if pd.isna(value):
            return ""

        str_value = str(value).strip()

        if not str_value or str_value.lower() in ['nan', 'none', 'null']:
            return ""

        # Handle numeric values safely
        if str_value.replace('.', '').replace('-', '').isdigit():
            try:
                if '.' in str_value:
                    float_val = float(str_value)
                    if float_val.is_integer():
                        str_value = str(int(float_val))
                else:
                    str_value = str(int(str_value))
            except (ValueError, OverflowError):
                pass

        return str_value


    # Pre-process ZDM data once with robust cleaning
    zdm_ca_clean = zdm_df["Contract Account"].apply(clean_contract_account)
    zdm_premise = zdm_df["Premise"].astype(str).str.strip()

    # Create lookup dictionary once
    ca_to_locationid = dict(zip(zdm_ca_clean, zdm_premise))

    # Clean DFKKOP Contract Accounts with robust cleaning
    df_new_ca_clean = df_new["Cont.Account"].apply(clean_contract_account)

    # Apply vectorized mapping
    df_new["LOCATIONID"] = df_new_ca_clean.map(ca_to_locationid).fillna("")

    matched_count = (df_new["LOCATIONID"] != "").sum()
    print(f"LOCATIONID mapping: {matched_count:,} matched ({matched_count / len(df_new) * 100:.1f}%)")

    # EVER fallback if needed
    if matched_count < len(df_new) and data_sources.get("EVER") is not None:
        print("Applying EVER fallback...")
        ever_df = data_sources["EVER"].copy()

        # Create EVER mappings with robust cleaning
        ever_ca_clean = ever_df["Cont.Account"].apply(clean_contract_account)
        ever_install = ever_df["Installat."].astype(str).str.strip()
        ca_to_install = dict(zip(ever_ca_clean, ever_install))

        zdm_install = zdm_df["Installation"].astype(str).str.strip()
        install_to_premise = dict(zip(zdm_install, zdm_premise))

        # Apply two-step fallback mapping
        missing_mask = df_new["LOCATIONID"] == ""
        missing_ca = df_new_ca_clean[missing_mask]

        fallback_installs = missing_ca.map(ca_to_install)
        fallback_premises = fallback_installs.map(install_to_premise).fillna("")

        df_new.loc[missing_mask, "LOCATIONID"] = fallback_premises

        final_matched = (df_new["LOCATIONID"] != "").sum()
        print(f"After EVER fallback: {final_matched:,} matched ({final_matched / len(df_new) * 100:.1f}%)")
else:
    df_new["LOCATIONID"] = ""
    print("ZDM_PREMDETAILS not available, LOCATIONID set to empty")

# Set other required fields
df_new["TAXYEAR"] = ""
df_new["TRANSACTIONAMOUNT"] = df_new["Amount"]
df_new["APPLICATION"] = "5"
df_new["TENDERTYPE"] = ""
df_new['UPDATEDATE'] = " "

# OPTIMIZATION 9: Pre-compiled mapping dictionaries for transaction types
print("Setting up optimized transaction type mapping...")

# Pre-compile all mappings into a single dictionary for O(1) lookup
transaction_mappings = {
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
    ("0080", "0010"): {"TRANSACTIONTYPE": "4", "TRANSACTIONDESCRIPTION": "Mthy Installment Rec", "BILLTYPE": "0"},
    # MTrans 0100 combinations
    ("0100", "0002"): {"TRANSACTIONTYPE": "2", "TRANSACTIONDESCRIPTION": "Consumption Billing", "BILLTYPE": "0"},
    # MTrans 0200 combinations
    ("0200", "0002"): {"TRANSACTIONTYPE": "99", "TRANSACTIONDESCRIPTION": "Final Billing", "BILLTYPE": "1"},
    # MTrans 0620 combinations
    ("0620", "0010"): {"TRANSACTIONTYPE": "4", "TRANSACTIONDESCRIPTION": "Transfer", "BILLTYPE": "0"},
    # MTrans 0630 combinations
    ("0630", "0010"): {"TRANSACTIONTYPE": "4", "TRANSACTIONDESCRIPTION": "Write-Off", "BILLTYPE": "0"},
}

# OPTIMIZATION 10: Vectorized transaction type mapping
mtrans_series = df_new["MTrans"].astype(str).str.strip()
strans_series = df_new["STrans"].astype(str).str.strip()

# Create combination keys
combo_keys = list(zip(mtrans_series, strans_series))

# Apply mappings using vectorized operations
df_new["TRANSACTIONTYPE"] = "99"  # Default
df_new["TRANSACTIONDESCRIPTION"] = "Other Transaction"  # Default
df_new["BILLTYPE"] = "0"  # Default

# Vectorized mapping
for combo, mapping in transaction_mappings.items():
    mask = (mtrans_series == combo[0]) & (strans_series == combo[1])
    df_new.loc[mask, "TRANSACTIONTYPE"] = mapping["TRANSACTIONTYPE"]
    df_new.loc[mask, "TRANSACTIONDESCRIPTION"] = mapping["TRANSACTIONDESCRIPTION"]
    df_new.loc[mask, "BILLTYPE"] = mapping["BILLTYPE"]

print("Applied transaction type mappings")

# new code try here
# exclude all reocrds with mtrans 0100 and strans of 0002
df_new = df_new[~((df_new["MTrans"].astype(str).str.strip() == "0100") &
                  (df_new["STrans"].astype(str).str.strip() == "0002"))]

# DEBUG CODE (replace the existing debug section with this fixed version)
print("\n" + "=" * 80)
print("DEBUG: TRANSACTIONAL HIST - BILLORINVOICENUMBER TRACKING")
print("=" * 80)

# Before extraction - examine raw Reference values
print("\n1. RAW REFERENCE VALUES (before processing):")
raw_references = df_new["Reference"].dropna()
print(f"   Total non-null Reference values: {len(raw_references):,}")
print(f"   Sample raw values: {raw_references.head(10).tolist()}")
print(f"   Data types: {raw_references.dtype}")

# FIXED: Handle mixed data types safely
try:
    # Convert to numeric and find range of numeric values
    numeric_refs = pd.to_numeric(raw_references, errors='coerce')
    valid_numeric = numeric_refs.dropna()
    if len(valid_numeric) > 0:
        print(f"   Numeric value range: {valid_numeric.min():.0f} to {valid_numeric.max():.0f}")
        print(f"   Numeric values: {len(valid_numeric):,} ({len(valid_numeric) / len(raw_references) * 100:.1f}%)")

    # Show non-numeric values if any
    non_numeric_mask = pd.to_numeric(raw_references, errors='coerce').isna()
    non_numeric = raw_references[non_numeric_mask]
    if len(non_numeric) > 0:
        print(f"   Non-numeric values: {len(non_numeric):,} samples: {non_numeric.head(5).tolist()}")

except Exception as e:
    print(f"   Could not analyze value range: {e}")

# After extraction - examine processed BILLORINVOICENUMBER
print(f"\n2. PROCESSED BILLORINVOICENUMBER VALUES:")
print(f"   Total processed values: {len(df_new['BILLORINVOICENUMBER']):,}")
print(f"   Non-empty processed values: {(df_new['BILLORINVOICENUMBER'] != '').sum():,}")
non_empty_invoice = df_new[df_new['BILLORINVOICENUMBER'] != '']['BILLORINVOICENUMBER']
if len(non_empty_invoice) > 0:
    print(f"   Sample processed values: {non_empty_invoice.head(10).tolist()}")
    print(f"   Processed value lengths: {non_empty_invoice.str.len().value_counts().to_dict()}")

# Create tracking DataFrame for later comparison
debug_transactional = pd.DataFrame({
    'CUSTOMERID': df_new["CUSTOMERID"],
    'DOC_DATE': df_new["Doc. Date"],
    'RAW_REFERENCE': df_new["Reference"],
    'PROCESSED_BILLORINVOICE': df_new["BILLORINVOICENUMBER"],
    'AMOUNT': df_new["Amount"],
    'MTRANS': df_new["MTrans"],
    'STRANS': df_new["STrans"],
    'ROW_INDEX': range(len(df_new))
})

# Save debug file
debug_path = os.path.join(os.path.dirname(list(file_paths.values())[0]), 'DEBUG_TRANSACTIONAL_TRACKING.csv')
debug_transactional.to_csv(debug_path, index=False)
print(f"\n3. DEBUG FILE SAVED: {debug_path}")

# Show sample customer records
if len(debug_transactional) > 0:
    sample_customer = debug_transactional.iloc[0]['CUSTOMERID']
    sample_records = debug_transactional[debug_transactional['CUSTOMERID'] == sample_customer].head(5)
    print(f"\n4. SAMPLE RECORDS FOR CUSTOMER {sample_customer}:")
    for idx, row in sample_records.iterrows():
        print(
            f"   Row {row['ROW_INDEX']}: Date: {row['DOC_DATE']}, Raw: {row['RAW_REFERENCE']}, Processed: {row['PROCESSED_BILLORINVOICE']}, Amount: {row['AMOUNT']}")

# Check for potential issues
print(f"\n5. VALIDATION CHECKS:")
print(f"   Records with empty BILLORINVOICENUMBER: {(df_new['BILLORINVOICENUMBER'] == '').sum():,}")

# FIXED: Safe check for non-numeric references
try:
    numeric_check = pd.to_numeric(df_new["Reference"], errors='coerce').isna()
    non_numeric_count = numeric_check.sum()
    print(f"   Records with non-numeric Reference: {non_numeric_count:,}")
except:
    print(f"   Could not check non-numeric Reference values")

# Show unique processed values (first 20)
unique_processed = df_new[df_new['BILLORINVOICENUMBER'] != '']['BILLORINVOICENUMBER'].unique()
if len(unique_processed) > 0:
    print(f"   Unique BILLORINVOICENUMBER values (first 20): {unique_processed[:20].tolist()}")
    print(f"   Total unique BILLORINVOICENUMBER values: {len(unique_processed):,}")

# Show date range of records
try:
    date_series = pd.to_datetime(df_new["Doc. Date"], errors='coerce')
    valid_dates = date_series.dropna()
    if len(valid_dates) > 0:
        print(f"\n6. DATE ANALYSIS:")
        print(f"   Date range: {valid_dates.min()} to {valid_dates.max()}")
        print(f"   Records by year: {valid_dates.dt.year.value_counts().sort_index().to_dict()}")
except Exception as e:
    print(f"\n6. DATE ANALYSIS: Could not analyze dates: {e}")

print("=" * 80)

# OPTIMIZATION 11: Vectorized data filtering and formatting
print("\nFiltering and formatting data...")

# Remove records missing required fields
required_mask = (
        (df_new['CUSTOMERID'] != "") &
        (df_new['LOCATIONID'] != "") &
        (df_new['TRANSACTIONDATE'] != "")
)
df_new = df_new[required_mask]
print(f"After filtering: {len(df_new)} records remain")

# OPTIMIZATION 12: Faster column formatting
print("Formatting field values (optimized)...")

# Define numeric columns that should not be quoted
numeric_columns = {'TAXYEAR', 'TRANSACTIONTYPE', 'TRANSACTIONAMOUNT', 'APPLICATION', 'BILLTYPE', 'TENDERTYPE'}


# Vectorized formatting function
def format_column_vectorized(series, column_name):
    if column_name in numeric_columns:
        return series.fillna("")
    else:
        # Quote non-numeric fields
        mask = series.notna() & (series != "") & (series != " ")
        result = series.astype(str).copy()
        result[mask] = '"' + result[mask] + '"'
        result[~mask] = ""
        return result


# Apply formatting to all columns
df_new = df_new.fillna("")
for col in df_new.columns:
    if col not in ['Doc. Date', 'Pstng Date', 'Due', 'Reference', 'BPartner', 'Cont.Account', 'Amount', 'MTrans',
                   'STrans']:
        df_new[col] = format_column_vectorized(df_new[col], col)

# Reorder columns based on target format
column_order = [
    "TAXYEAR", "CUSTOMERID", "LOCATIONID", "TRANSACTIONDATE", "BILLINGDATE",
    "DUEDATE", "BILLORINVOICENUMBER", "TRANSACTIONTYPE", "TRANSACTIONAMOUNT",
    "TRANSACTIONDESCRIPTION", "APPLICATION", "BILLTYPE", "TENDERTYPE", "UPDATEDATE"
]

# Keep only the required columns
df_new = df_new[column_order]
print(f"Final record count: {len(df_new)}")

# Add trailer row
trailer_row = pd.DataFrame([["TRAILER"] + [''] * (len(df_new.columns) - 1)], columns=df_new.columns)
df_new = pd.concat([df_new, trailer_row], ignore_index=True)

# Save to CSV
# output_path = os.path.join(os.path.dirname(list(file_paths.values())[0]), 'OPTIMIZED_STAGE_TRANSACTIONAL_HIST.csv')
output_path = r"C:\Users\GTUSER1\Documents\CONV 3\output\Group C\STAGE_TRANSACTIONAL_HIST.csv"

df_new.to_csv(output_path, index=False, header=True, quoting=csv.QUOTE_NONE, escapechar='\\')
print(f"CSV file saved at {output_path}")

print("\n🚀 OPTIMIZATION COMPLETE! The processing should now be significantly faster.")
