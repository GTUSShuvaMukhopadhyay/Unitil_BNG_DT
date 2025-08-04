# CONV 2 B - STAGE_TRANSACTIONAL_HIST_ZMECON.py

import pandas as pd
import os
import csv
from datetime import datetime

# Define specific date range for filtering
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

# Define file paths
file_paths = {
    "ZMECON1": r"C:\Users\us85360\Desktop\CONV 2 B - STAGE_TRANSACTIONAL_HIST_ZMECON\ZMECON 010125 TO 07142025.XLSX",
    "ZMECON2": r"C:\Users\us85360\Desktop\CONV 2 B - STAGE_TRANSACTIONAL_HIST_ZMECON\ZMECON 01012022 TO 12312024 v1.XLSX",
    "ZMECON3": r"C:\Users\us85360\Desktop\CONV 2 B - STAGE_TRANSACTIONAL_HIST_ZMECON\ZMECON 01012017 TO 12312019.XLSX",
    "ZMECON4": r"C:\Users\us85360\Desktop\CONV 2 B - STAGE_TRANSACTIONAL_HIST_ZMECON\ZMECON 01012020 TO 12312021.XLSX",
    "ZMECON5": r"C:\Users\us85360\Desktop\CONV 2 B - STAGE_TRANSACTIONAL_HIST_ZMECON\ZMECON 010115 TO 123116.XLSX",
}
 
# Initialize data_sources dictionary to hold our data
data_sources = {}

# Function to read an Excel file with date filtering
def read_excel_file(name, path):
    try:
        # For ZMECON files, try to read the first sheet regardless of name
        if name.startswith("ZMECON"):
            df = pd.read_excel(path, sheet_name=0, engine="openpyxl")  # 0 means first sheet
            
            # Apply specific date filtering to ZMECON files using column 23
            if "ZMECON" in name:
                # Convert column 23 (index 23) to datetime safely
                date_col = pd.to_datetime(df.iloc[:, 23], errors='coerce')
                start_date = pd.to_datetime("2019-06-01")
                end_date = pd.to_datetime("2025-06-14")
                mask = (date_col >= start_date) & (date_col <= end_date)
                original_rows = df.shape[0]
                df = df[mask]
                print(f"Filtered {name}: {original_rows} → {df.shape[0]} rows in date range {start_date.date()} to {end_date.date()}")
                
        else:
            df = pd.read_excel(path, sheet_name="Sheet1", engine="openpyxl")
            
        print(f"Successfully loaded {name}: {df.shape[0]} rows, {df.shape[1]} columns")
        return df
    except Exception as e:
        print(f"Error loading {name}: {e}")
        return None

# Load data sources
print("\nLoading data sources...")
for name, path in file_paths.items():
    data_sources[name] = read_excel_file(name, path)

# Combine all ZMECON files into a single dataframe
zmecon_keys = [key for key in data_sources.keys() if key.startswith("ZMECON")]
if zmecon_keys:
    zmecon_dfs = [data_sources[key] for key in zmecon_keys if data_sources[key] is not None]
    if zmecon_dfs:
        data_sources["ZMECON"] = pd.concat(zmecon_dfs, ignore_index=True)
        print(f"Combined {len(zmecon_dfs)} ZMECON files into a single dataframe with {len(data_sources['ZMECON'])} rows")
    else:
        print("Warning: No valid ZMECON dataframes found to combine")
        exit(1)
else:
    print("Warning: No ZMECON files found in data_sources")
    exit(1)

# Verify all data sources loaded successfully
failed_sources = [name for name, df in data_sources.items() if df is None]
if failed_sources:
    print(f"Error: Failed to load data sources: {', '.join(failed_sources)}")
    exit(1)


# --------------------------
# Start with ZMECON as base
# --------------------------
print("\nStarting transformation with ZMECON as base...")
df_new = data_sources["ZMECON"].copy()
print(f"Base ZMECON records: {len(df_new)}")

# Print column names to verify
print("\nZMECON columns:", df_new.columns.tolist())

# --------------------------
# Extract CUSTOMERID from ZMECON (Column A = iloc[:, 0])
# --------------------------
if data_sources.get("ZMECON") is not None:
    df_new["CUSTOMERID"] = data_sources["ZMECON"].iloc[:, 0].apply(
        lambda x: str(int(x)) if pd.notna(x) and isinstance(x, (int, float)) else str(x)
    ).str.slice(0, 15)
    print(f"Extracted {len(df_new)} CUSTOMERID values from filtered data")

# --------------------------
# Extract LOCATIONID directly from ZMECON (Premise column, index 25)
# --------------------------
if data_sources.get("ZMECON") is not None:
    df_new["LOCATIONID"] = data_sources["ZMECON"].iloc[:, 25].apply(
        lambda x: str(int(x)) if pd.notna(x) and isinstance(x, (int, float)) else str(x)
    ).str.strip()
    print(f"Extracted LOCATIONID from ZMECON Premise column")

# --------------------------
# Create three-row transformation logic
# --------------------------
print("\nCreating three rows per ZMECON record...")

# Store the base data before transformation
base_df = df_new.copy()
print(f"Base records before transformation: {len(base_df)}")

# Initialize list to store all transformed rows
all_rows = []

# Process each record and create three rows
for idx, row in base_df.iterrows():
    # Common fields for all three rows
    common_data = {
        'CUSTOMERID': row['CUSTOMERID'],
        'LOCATIONID': row['LOCATIONID'],
        # Date fields - extract from original ZMECON data
        'TRANSACTIONDATE': pd.to_datetime(data_sources["ZMECON"].iloc[idx, 23], errors='coerce').strftime('%Y-%m-%d') if pd.notna(data_sources["ZMECON"].iloc[idx, 23]) else "",  # Column X: "Date to #1"
        'BILLINGDATE': (pd.to_datetime(data_sources["ZMECON"].iloc[idx, 23], errors='coerce') + pd.Timedelta(days=1)).strftime('%Y-%m-%d') if pd.notna(data_sources["ZMECON"].iloc[idx, 23]) else "",  # Column X: "Date to #1" + 1 day
        'DUEDATE': (pd.to_datetime(data_sources["ZMECON"].iloc[idx, 23], errors='coerce') + pd.Timedelta(days=28)).strftime('%Y-%m-%d') if pd.notna(data_sources["ZMECON"].iloc[idx, 23]) else "",  # Column X: "Date to #1" + 28 days
        'BILLORINVOICENUMBER': str(data_sources["ZMECON"].iloc[idx, 3]) if pd.notna(data_sources["ZMECON"].iloc[idx, 3]) else "",  # Column D: "Print Document No."
        # Fixed field values
        'TAXYEAR': "",
        'APPLICATION': 5,
        'BILLTYPE': 0,
        'TENDERTYPE': 0,
        'UPDATEDATE': "",
    }
    
    # Row 1: Service Charge (Type 99)
    service_row = common_data.copy()
    service_row.update({
        'TRANSACTIONTYPE': 99,
        'TRANSACTIONAMOUNT': data_sources["ZMECON"].iloc[idx, 8],  # Column I: "Serv.Charge (Type 99)"
        'TRANSACTIONDESCRIPTION': 'Service Charge'
    })
    all_rows.append(service_row)
    
    # Row 2: Energy Charges (Type 2) - Sum of multiple columns
    energy_amount = (
        data_sources["ZMECON"].iloc[idx, 9] +   # Column J: "Energy Charge"
        data_sources["ZMECON"].iloc[idx, 11] +  # Column L: "Past Gas Adj.Charges"
        data_sources["ZMECON"].iloc[idx, 13] +  # Column N: "Efficiency mine fund"
        data_sources["ZMECON"].iloc[idx, 15]    # Column P: "Transp.Charge"
    )
    energy_row = common_data.copy()
    energy_row.update({
        'TRANSACTIONTYPE': 2,
        'TRANSACTIONAMOUNT': energy_amount,
        'TRANSACTIONDESCRIPTION': 'Energy Charges'
    })
    all_rows.append(energy_row)
    
    # Row 3: Sales Tax (Type 99)
    tax_row = common_data.copy()
    tax_row.update({
        'TRANSACTIONTYPE': 99,
        'TRANSACTIONAMOUNT': data_sources["ZMECON"].iloc[idx, 17],  # Column R: "Sale tax (Type 99)"
        'TRANSACTIONDESCRIPTION': 'Sales Tax'
    })
    all_rows.append(tax_row)

# Create new dataframe from all transformed rows
df_new = pd.DataFrame(all_rows)
print(f"Transformed to {len(df_new)} total rows ({len(df_new)//3} original records × 3)")

# Verify the transformation
print(f"\nTransformation verification:")
print(f"Service Charge rows: {(df_new['TRANSACTIONDESCRIPTION'] == 'Service Charge').sum()}")
print(f"Energy Charges rows: {(df_new['TRANSACTIONDESCRIPTION'] == 'Energy Charges').sum()}")
print(f"Sales Tax rows: {(df_new['TRANSACTIONDESCRIPTION'] == 'Sales Tax').sum()}")

# --------------------------
# Format values with proper quoting
# --------------------------
print("\nFormatting field values...")

def selective_custom_quote(val, column_name):
    # Numeric fields that should not be quoted
    numeric_columns = ['TAXYEAR', 'TRANSACTIONTYPE', 'TRANSACTIONAMOUNT', 'APPLICATION', 
                      'BILLTYPE', 'TENDERTYPE']
    
    if column_name in numeric_columns:
        return str(val) if pd.notna(val) and str(val) != "" else ""
    return "" if pd.isna(val) or str(val) in ['nan', 'NaN', 'NAN', ''] else f'"{val}"'

# Apply formatting to all columns
for col in ['TAXYEAR', 'CUSTOMERID', 'LOCATIONID', 'TRANSACTIONDATE', 'BILLINGDATE',
            'DUEDATE', 'BILLORINVOICENUMBER', 'TRANSACTIONTYPE', 'TRANSACTIONAMOUNT',
            'TRANSACTIONDESCRIPTION', 'APPLICATION', 'BILLTYPE', 'TENDERTYPE', 'UPDATEDATE']:
    if col in df_new.columns:
        df_new[col] = df_new[col].apply(lambda x: selective_custom_quote(x, col))

# --------------------------
# Data validation
# --------------------------
print("\nValidating data...")
initial_count = len(df_new)

# Check for missing required fields
missing_customerid = (df_new['CUSTOMERID'] == '""').sum()
missing_locationid = (df_new['LOCATIONID'] == '""').sum()
missing_transdate = (df_new['TRANSACTIONDATE'] == '""').sum()

print(f"Records missing CUSTOMERID: {missing_customerid}")
print(f"Records missing LOCATIONID: {missing_locationid}")
print(f"Records missing TRANSACTIONDATE: {missing_transdate}")

# Display sample of data
print("\nSample of transformed data:")
print(df_new[['CUSTOMERID', 'LOCATIONID', 'TRANSACTIONDATE', 'TRANSACTIONTYPE', 'TRANSACTIONAMOUNT', 'TRANSACTIONDESCRIPTION']].head(6))

# --------------------------
# Reorder columns based on target format
# --------------------------
column_order = [
    "TAXYEAR", "CUSTOMERID", "LOCATIONID", "TRANSACTIONDATE", "BILLINGDATE", 
    "DUEDATE", "BILLORINVOICENUMBER", "TRANSACTIONTYPE", "TRANSACTIONAMOUNT", 
    "TRANSACTIONDESCRIPTION", "APPLICATION", "BILLTYPE", "TENDERTYPE", "UPDATEDATE"
]

# Keep only the required columns in the correct order
df_new = df_new[column_order]
print(f"\nOrdered columns according to target format. Final columns: {len(df_new.columns)}")

# --------------------------
# Add trailer row
# --------------------------
trailer_row = pd.DataFrame([["TRAILER"] + [''] * (len(df_new.columns) - 1)], columns=df_new.columns)
df_new = pd.concat([df_new, trailer_row], ignore_index=True)
print(f"Added trailer row. Final row count: {len(df_new)}")

# --------------------------
# Save to CSV
# --------------------------
output_path = os.path.join(os.path.dirname(list(file_paths.values())[0]), 
                          '716_STAGE_TRANSACTIONAL_HIST_ZMECON.csv')
df_new.to_csv(output_path, index=False, header=True, quoting=csv.QUOTE_NONE, escapechar='\\')
print(f"\nCSV file saved at: {output_path}")

# Print summary
print("\n" + "="*50)
print("TRANSFORMATION COMPLETE")
print("="*50)
print(f"Total records processed: {len(df_new) - 1}")  # Minus trailer row
print(f"Output file: {os.path.basename(output_path)}")
print(f"Expected output: {(len(df_new) - 1) // 3} ZMECON records × 3 = {len(df_new) - 1} transaction rows")
