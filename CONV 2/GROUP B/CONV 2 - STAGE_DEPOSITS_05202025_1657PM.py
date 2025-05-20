# CONV 2 - STAGE_DEPOSITS_05202025_1252PM.py
# STAGE_DEPOSITS.py - OPTIMIZED VERSION

import pandas as pd
import os
import csv
import time
import multiprocessing

# This is needed for Windows to avoid the "RuntimeError: 
# An attempt has been made to start a new process before the current process
# has finished its bootstrapping phase" error
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

# Define file paths
file_paths = {
    "FPD2": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_DEPOSITS\FPD2.XLSX",
    "ZDM_PREMDETAILS": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_DEPOSITS\ZDM_PREMDETAILS.XLSX",
    "ZMECON1": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_DEPOSITS\ZMECON 2021 to 03272025.xlsx",
    "ZMECON2": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_DEPOSITS\ZMECON 2015 to 2020.xlsx",
    "DFKKOP1": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_DEPOSITS\DFKKOP 01012015 to 12312015 (1).XLSX",
    "DFKKOP2": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_DEPOSITS\DFKKOP 01012016 to 12312016.XLSX",
    "DFKKOP3": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_DEPOSITS\DFKKOP 01012017 to 12312017.XLSX",
    "DFKKOP4": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_DEPOSITS\DFKKOP 01012018 to 12312018.XLSX",
    "DFKKOP5": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_DEPOSITS\DFKKOP 01012019 to 12312019.XLSX",
    "DFKKOP6": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_DEPOSITS\DFKKOP 01012020 to 12312020.XLSX",
    "DFKKOP7": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_DEPOSITS\DFKKOP 01012021 to 12312021.XLSX",
    "DFKKOP8": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_DEPOSITS\DFKKOP 01012022 to 12312022.XLSX",
    "DFKKOP9": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_DEPOSITS\DFKKOP 01012023 to 12312023.XLSX",
    "DFKKOP10": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_DEPOSITS\DFKKOP 01012024 TO 03272025.XLSX",
}

# Function to load Excel file with optimized parameters
def load_excel(name, path):
    try:
        # First, try to load all columns to see the actual structure
        if name == "ZDM_PREMDETAILS":
            # For ZDM_PREMDETAILS, load all columns first to inspect
            temp_df = pd.read_excel(path, sheet_name="Sheet1", engine="openpyxl", nrows=5)
            print(f"ZDM_PREMDETAILS columns: {len(temp_df.columns)}")
            # Now determine which columns to use based on actual file structure
            if len(temp_df.columns) > 7:  # Make sure we have enough columns
                return pd.read_excel(path, sheet_name="Sheet1", engine="openpyxl", 
                                   usecols=[2, 7], dtype=str)
            else:
                # Fallback to loading all columns if structure differs
                return pd.read_excel(path, sheet_name="Sheet1", engine="openpyxl", dtype=str)
        
        # For DFKKOP files, we only need columns 1, 4, 5, and 11
        elif name.startswith("DFKKOP"):
            return pd.read_excel(path, sheet_name="Sheet1", engine="openpyxl", 
                               usecols=[1, 4, 5, 11], dtype=str)
        
        # For ZMECON files, we only need columns 0, 22, and 24
        elif name.startswith("ZMECON"):
            temp_df = pd.read_excel(path, sheet_name="Sheet1", engine="openpyxl", nrows=5)
            if len(temp_df.columns) >= 25:  # Make sure we have enough columns
                return pd.read_excel(path, sheet_name="Sheet1", engine="openpyxl", 
                                   usecols=[0, 22, 24], dtype=str)
            else:
                # Fallback to loading essential columns if structure differs
                return pd.read_excel(path, sheet_name="Sheet1", engine="openpyxl", dtype=str)
        
        # For FPD2, we need columns 0, 4, 8, 10
        elif name == "FPD2":
            return pd.read_excel(path, sheet_name="Sheet1", engine="openpyxl", 
                               usecols=[0, 4, 8, 10], dtype=str)
        
        else:
            return pd.read_excel(path, sheet_name="Sheet1", engine="openpyxl")
    except Exception as e:
        print(f"Error loading {name}: {e}")
        return None

# Parallel load of files using ProcessPoolExecutor
print("Loading data files...")
data_sources = {}

# Load essential files first (sequential for clarity, but could be paralleled)
data_sources["FPD2"] = load_excel("FPD2", file_paths["FPD2"])
data_sources["ZDM_PREMDETAILS"] = load_excel("ZDM_PREMDETAILS", file_paths["ZDM_PREMDETAILS"])

# Load ZMECON files and concatenate
print("Loading ZMECON files...")
zmecon1 = load_excel("ZMECON1", file_paths["ZMECON1"])
zmecon2 = load_excel("ZMECON2", file_paths["ZMECON2"])
if zmecon1 is not None and zmecon2 is not None:
    data_sources["ZMECON"] = pd.concat([zmecon1, zmecon2], ignore_index=True)
    # Rename columns for clarity and easy access
    if len(data_sources["ZMECON"].columns) >= 25:
        zmecon_columns = {
            data_sources["ZMECON"].columns[0]: "CUSTOMERID",
            data_sources["ZMECON"].columns[22]: "DEPOSIT_DATE",
            data_sources["ZMECON"].columns[24]: "STATUS"
        }
        data_sources["ZMECON"] = data_sources["ZMECON"].rename(columns=zmecon_columns)
        # Pre-process data to avoid repeated operations
        data_sources["ZMECON"]["CUSTOMERID"] = data_sources["ZMECON"]["CUSTOMERID"].astype(str).str.zfill(7)

# Load DFKKOP files sequentially (parallel loading causes pickling issues)
print("Loading DFKKOP files...")
dfkkop_dfs = []
for name, path in file_paths.items():
    if name.startswith("DFKKOP"):
        df = load_excel(name, path)
        if df is not None:
            dfkkop_dfs.append(df)
            print(f"Loaded {name}")

# Concatenate all DFKKOP files
print("Concatenating DFKKOP files...")
if dfkkop_dfs:
    data_sources["DFKKOPA"] = pd.concat(dfkkop_dfs, ignore_index=True)
    
    # Rename columns for clarity and easy access
    if len(data_sources["DFKKOPA"].columns) >= 4:  # Make sure we have expected columns
        dfkkop_columns = {
            data_sources["DFKKOPA"].columns[0]: "CUSTOMERID",
            data_sources["DFKKOPA"].columns[1]: "COL_4",
            data_sources["DFKKOPA"].columns[2]: "COL_5",
            data_sources["DFKKOPA"].columns[3]: "INTEREST_CALC_DATE"
        }
        data_sources["DFKKOPA"] = data_sources["DFKKOPA"].rename(columns=dfkkop_columns)
        
        # Filter only once instead of at each record check
        data_sources["DFKKOP"] = data_sources["DFKKOPA"][
            (data_sources["DFKKOPA"]["COL_4"] == "0025") & 
            (data_sources["DFKKOPA"]["COL_5"] == "0010")
        ]
        
        # Pre-process data to avoid repeated operations
        data_sources["DFKKOP"]["CUSTOMERID"] = data_sources["DFKKOP"]["CUSTOMERID"].astype(str).str.zfill(7)
else:
    print("No DFKKOP files were loaded successfully")

# Prepare ZDM_PREMDETAILS
customerid_to_locationid = {}
if data_sources["ZDM_PREMDETAILS"] is not None:
    try:
        # Determine the actual column structure
        if len(data_sources["ZDM_PREMDETAILS"].columns) >= 2:
            zdm_columns = {
                data_sources["ZDM_PREMDETAILS"].columns[0]: "LOCATIONID",
                data_sources["ZDM_PREMDETAILS"].columns[1]: "CUSTOMERID"
            }
            data_sources["ZDM_PREMDETAILS"] = data_sources["ZDM_PREMDETAILS"].rename(columns=zdm_columns)
            
            # Convert customerid to string safely
            data_sources["ZDM_PREMDETAILS"]["CUSTOMERID"] = data_sources["ZDM_PREMDETAILS"]["CUSTOMERID"].apply(
                lambda x: str(int(float(x))) if pd.notna(x) and str(x).strip() != "" else ""
            )
            
            # Create a dictionary mapping for fast lookups (with safer conversions)
            for idx, row in data_sources["ZDM_PREMDETAILS"].iterrows():
                if pd.notna(row["CUSTOMERID"]) and row["CUSTOMERID"] != "":
                    try:
                        cust_id = str(int(float(row["CUSTOMERID"])))
                        if pd.notna(row["LOCATIONID"]) and row["LOCATIONID"] != "":
                            loc_id = str(int(float(row["LOCATIONID"])))
                            customerid_to_locationid[cust_id] = loc_id
                    except (ValueError, TypeError):
                        continue
            
            print(f"Created mapping dictionary with {len(customerid_to_locationid)} entries")
        else:
            print("ZDM_PREMDETAILS has unexpected column structure")
    except Exception as e:
        print(f"Error processing ZDM_PREMDETAILS: {e}")

# Prepare FPD2
if data_sources["FPD2"] is not None:
    fpd2_columns = {
        data_sources["FPD2"].columns[0]: "CUSTOMERID",
        data_sources["FPD2"].columns[1]: "DEPOSIT_DATE",
        data_sources["FPD2"].columns[2]: "DEPOSIT_AMOUNT",
        data_sources["FPD2"].columns[3]: "DEPOSIT_STATUS"
    }
    data_sources["FPD2"] = data_sources["FPD2"].rename(columns=fpd2_columns)
    
    # Convert CUSTOMERID to string and ensure proper format
    data_sources["FPD2"]["CUSTOMERID"] = data_sources["FPD2"]["CUSTOMERID"].apply(
        lambda x: str(int(x)) if pd.notna(x) and isinstance(x, (int, float)) else str(x)
    ).str.slice(0, 15)

# Create ZMECON lookup dictionary for faster access
zmecon_lookup = {}
if "ZMECON" in data_sources and data_sources["ZMECON"] is not None:
    try:
        # Check if we have the expected columns structure
        if "CUSTOMERID" in data_sources["ZMECON"].columns and \
           "DEPOSIT_DATE" in data_sources["ZMECON"].columns and \
           "STATUS" in data_sources["ZMECON"].columns:
            # Use the renamed columns
            for _, row in data_sources["ZMECON"].iterrows():
                customerid = row["CUSTOMERID"]
                deposit_date = pd.to_datetime(row["DEPOSIT_DATE"], errors='coerce')
                status = row["STATUS"] if pd.notna(row["STATUS"]) else ""
                zmecon_lookup[customerid] = {"deposit_date": deposit_date, "status": status.strip()}
        else:
            # Attempt to use original column positions
            for _, row in data_sources["ZMECON"].iterrows():
                try:
                    customerid = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
                    deposit_date = pd.to_datetime(row.iloc[22], errors='coerce') if len(row) > 22 else None
                    status = str(row.iloc[24]).strip() if len(row) > 24 and pd.notna(row.iloc[24]) else ""
                    if customerid and customerid != "":
                        zmecon_lookup[customerid] = {"deposit_date": deposit_date, "status": status}
                except (IndexError, ValueError) as e:
                    continue
        print(f"Created ZMECON lookup with {len(zmecon_lookup)} entries")
    except Exception as e:
        print(f"Error processing ZMECON data: {e}")

# Create DFKKOP lookup dictionary for faster access
dfkkop_lookup = {}
if "DFKKOP" in data_sources and data_sources["DFKKOP"] is not None:
    try:
        for _, row in data_sources["DFKKOP"].iterrows():
            if "CUSTOMERID" in data_sources["DFKKOP"].columns and "INTEREST_CALC_DATE" in data_sources["DFKKOP"].columns:
                customerid = row["CUSTOMERID"]
                interest_calc_date = row["INTEREST_CALC_DATE"]
                dfkkop_lookup[customerid] = interest_calc_date
            else:
                # Fallback to column indices
                customerid = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
                interest_calc_date = row.iloc[3] if len(row) > 3 else None
                if customerid and customerid != "":
                    dfkkop_lookup[customerid] = interest_calc_date
        print(f"Created DFKKOP lookup with {len(dfkkop_lookup)} entries")
    except Exception as e:
        print(f"Error processing DFKKOP data: {e}")

# Ensure this is in the main block
if __name__ == "__main__":
    # Output for main processing
    print("Processing main data...")
    
    # Initialize df_new with the appropriate columns directly from FPD2
    print("Creating the output dataframe...")
    if data_sources["FPD2"] is not None:
        try:
            # Extract required columns directly from FPD2
            df_new = pd.DataFrame()
            
            # Check and convert CUSTOMERID safely
            if "CUSTOMERID" in data_sources["FPD2"].columns:
                df_new["CUSTOMERID"] = data_sources["FPD2"]["CUSTOMERID"].apply(
                    lambda x: str(int(float(x))) if pd.notna(x) and isinstance(x, (int, float, str)) and str(x).strip() != "" else ""
                ).str.slice(0, 15)
            else:
                # Fallback to column index
                df_new["CUSTOMERID"] = data_sources["FPD2"].iloc[:, 0].apply(
                    lambda x: str(int(float(x))) if pd.notna(x) and isinstance(x, (int, float, str)) and str(x).strip() != "" else ""
                ).str.slice(0, 15)
            
            # Map LOCATIONID using the lookup dictionary (much faster than apply)
            df_new["LOCATIONID"] = df_new["CUSTOMERID"].map(customerid_to_locationid)
            
            # Map DEPOSITSTATUS using vectorized operations with safer approach
            if "DEPOSIT_STATUS" in data_sources["FPD2"].columns:
                df_new["DEPOSITSTATUS"] = data_sources["FPD2"]["DEPOSIT_STATUS"].map(
                    lambda x: 2 if x == "Paid" else (90 if x == "Request" else 0)
                )
            else:
                # Fallback to column index
                df_new["DEPOSITSTATUS"] = data_sources["FPD2"].iloc[:, 3].map(
                    lambda x: 2 if x == "Paid" else (90 if x == "Request" else 0)
                )
            
            # Convert dates once with safer approach
            if "DEPOSIT_DATE" in data_sources["FPD2"].columns:
                df_new["DEPOSITDATE"] = pd.to_datetime(
                    data_sources["FPD2"]["DEPOSIT_DATE"], errors='coerce'
                ).dt.strftime('%Y-%m-%d')
            else:
                # Fallback to column index
                df_new["DEPOSITDATE"] = pd.to_datetime(
                    data_sources["FPD2"].iloc[:, 1], errors='coerce'
                ).dt.strftime('%Y-%m-%d')
            
            # Convert amounts once with safer approach
            if "DEPOSIT_AMOUNT" in data_sources["FPD2"].columns:
                df_new["DEPOSITAMOUNT"] = pd.to_numeric(
                    data_sources["FPD2"]["DEPOSIT_AMOUNT"], errors='coerce'
                ).fillna(0)
            else:
                # Fallback to column index
                df_new["DEPOSITAMOUNT"] = pd.to_numeric(
                    data_sources["FPD2"].iloc[:, 2], errors='coerce'
                ).fillna(0)
            
            # Map DEPOSITINTERESTCALCDATE using dictionary lookup
            df_new["DEPOSITINTERESTCALCDATE"] = df_new["CUSTOMERID"].map(dfkkop_lookup)
            
            # Calculate DEPOSITREFUNDMONTHS using vectorized operations
            def calculate_refund_months(customerid):
                if customerid in zmecon_lookup:
                    status = zmecon_lookup[customerid]["status"]
                    return 12 if status and status == "RES" else 24
                return 24
            
            df_new["DEPOSITREFUNDMONTHS"] = df_new["CUSTOMERID"].apply(calculate_refund_months)
            
            # Assign hardcoded values
            df_new["APPLICATION"] = "5"
            df_new["DEPOSITKIND"] = "CASH"
            
            # Calculate DEPOSITBILLEDFLAG more efficiently
            def check_deposit_billed_flag(row):
                customerid = row["CUSTOMERID"]
                deposit_date = pd.to_datetime(row["DEPOSITDATE"], errors='coerce')
                
                if customerid in zmecon_lookup and pd.notna(deposit_date):
                    zmecon_date = zmecon_lookup[customerid]["deposit_date"]
                    if pd.notna(zmecon_date) and zmecon_date > deposit_date:
                        return "Y"
                return "N"
            
            df_new["DEPOSITBILLEDFLAG"] = df_new.apply(check_deposit_billed_flag, axis=1)
            df_new["DEPOSITACCRUEDINTEREST"] = ""
            df_new["UPDATEDATE"] = " "
            
            # Drop records where LOCATIONID is blank (vectorized operation)
            df_new = df_new[df_new['LOCATIONID'].notna() & (df_new['LOCATIONID'] != '')]
            
            # Custom quote function (more efficient with dictionary approach)
            numeric_columns = ['APPLICATION', 'DEPOSITSTATUS', 'DEPOSITKIND', 'DEPOSITAMOUNT', 
                            'DEPOSITACCRUEDINTEREST', 'DEPOSITREFUNDMONTHS']
            
            # Apply custom quoting to all columns at once
            for col in df_new.columns:
                if col not in numeric_columns:
                    df_new[col] = df_new[col].apply(
                        lambda x: f'"{x}"' if pd.notna(x) and x != "" and x != " " else ''
                    )
            
            # Reorder columns
            column_order = [
                "CUSTOMERID", "LOCATIONID", "APPLICATION", "DEPOSITSTATUS", "DEPOSITKIND",
                "DEPOSITDATE", "DEPOSITAMOUNT", "DEPOSITBILLEDFLAG", "DEPOSITACCRUEDINTEREST",
                "DEPOSITINTERESTCALCDATE", "DEPOSITREFUNDMONTHS", "UPDATEDATE"
            ]
            df_new = df_new[column_order]
            
            # Add trailer row
            trailer_row = pd.DataFrame([["TRAILER"] + [''] * (len(df_new.columns) - 1)], columns=df_new.columns)
            df_new = pd.concat([df_new, trailer_row], ignore_index=True)
            
            # Save to CSV
            output_path = os.path.join(os.path.dirname(list(file_paths.values())[0]), '05_20_25_CONV2_STAGE_DEPOSITS.csv')
            df_new.to_csv(output_path, index=False, header=True, quoting=csv.QUOTE_NONE, escapechar='\\')
            print(f"CSV file saved at {output_path}")
            
            end_time = time.time()
            print(f"Total execution time: {end_time - start_time:.2f} seconds")
        except Exception as e:
            print(f"Error during dataframe creation and processing: {e}")
    else:
        print("Error: FPD2 data is required but not available.")