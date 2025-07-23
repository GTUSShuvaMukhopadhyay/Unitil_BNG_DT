# CONV 2 B - STAGE_CONSUMPTION_HISTORY
# STAGE_CONSUMPTION_HIST.py
# updates were made to use mapping from STAGE_METERED_SVCS


import pandas as pd
import os
import csv  # For CSV saving
import concurrent.futures  # For parallel file loading
from datetime import datetime, timedelta

# Define the 6-year cutoff date
CUTOFF_DATE = datetime.now() - timedelta(days=6*365)  # 6 years ago
print(f"Filtering data for dates after: {CUTOFF_DATE.strftime('%Y-%m-%d')}")


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
    "ZDM_PREMDETAILS": r"C:\Users\us85360\Desktop\CONV 2B - STAGE_CONSUMPTION_HIST\ZDM_PREMDETAILS.XLSX",
    # "ZMECON1": r"C:\Users\us85360\Desktop\CONV 2B - STAGE_CONSUMPTION_HIST\ZMECON 010115 TO 123116.XLSX",
    "ZMECON2": r"C:\Users\us85360\Desktop\CONV 2B - STAGE_CONSUMPTION_HIST\ZMECON 01012017 TO 12312019.XLSX",
    "ZMECON3": r"C:\Users\us85360\Desktop\CONV 2B - STAGE_CONSUMPTION_HIST\ZMECON 01012020 TO 12312021.XLSX",
    "ZMECON4": r"C:\Users\us85360\Desktop\CONV 2B - STAGE_CONSUMPTION_HIST\ZMECON 01012022 TO 12312024 v1.XLSX",
    "ZMECON5": r"C:\Users\us85360\Desktop\CONV 2B - STAGE_CONSUMPTION_HIST\ZMECON 010125 TO 07142025.XLSX",
    "EABL1": r"C:\Users\us85360\Desktop\CONV 2B - STAGE_CONSUMPTION_HIST\EABL 06012019 TO 12312022.XLSX",
    "EABL2": r"C:\Users\us85360\Desktop\CONV 2B - STAGE_CONSUMPTION_HIST\EABL 01012023 TO 06142025.XLSX",
    "TF": r"C:\Users\us85360\Desktop\CONV 2B - STAGE_CONSUMPTION_HIST\ThermFactor.xlsx",
}

# Initialize data_sources dictionary
data_sources = {}

# Function to read an Excel file with date filtering
def read_excel_file_with_filter(name, path):
    try:
        df = pd.read_excel(path, sheet_name="Sheet1", engine="openpyxl")
        
        # Apply date filtering for ZMECON and EABL files
        if name.startswith("ZMECON"):
            # Filter ZMECON by current read date (column index 23)
            if len(df.columns) > 23:
                df['temp_date'] = pd.to_datetime(df.iloc[:, 23], errors='coerce')
                original_count = len(df)
                df = df[df['temp_date'] >= CUTOFF_DATE]
                df = df.drop('temp_date', axis=1)
                print(f"Filtered {name}: {original_count} → {len(df)} rows (removed {original_count - len(df)} rows outside 6-year range)")
        
        elif name.startswith("EABL"):
            # Filter EABL by "Schd MRD" (Scheduled Meter Read Date) - column index 6
            date_col_index = 4  # "Schd MRD" column
            if len(df.columns) > date_col_index:
                df['temp_date'] = pd.to_datetime(df.iloc[:, date_col_index], errors='coerce')
                original_count = len(df)
                df = df[df['temp_date'] >= CUTOFF_DATE]
                df = df.drop('temp_date', axis=1)
                print(f"Filtered {name}: {original_count} → {len(df)} rows (removed {original_count - len(df)} rows outside 6-year range)")
        
        print(f"Successfully loaded {name}: {df.shape[0]} rows, {df.shape[1]} columns")
        return name, df
    except Exception as e:
        print(f"Error loading {name}: {e}")
        return name, None

# Load files in parallel
print("Loading and filtering data sources...")
with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = {executor.submit(read_excel_file_with_filter, name, path): name for name, path in file_paths.items()}
    for future in concurrent.futures.as_completed(futures):
        name, df = future.result()
        data_sources[name] = df

# Create composite dataset for ZMECON (including ZMECON2 with 2019 data)
zmecon_files = ["ZMECON2", "ZMECON3", "ZMECON4", "ZMECON5"]
zmecon_dfs = [data_sources.get(name) for name in zmecon_files if data_sources.get(name) is not None]

if len(zmecon_dfs) > 0:
    data_sources["ZMECON"] = pd.concat(zmecon_dfs, ignore_index=True)
    print(f"Created combined ZMECON dataset from {len(zmecon_dfs)} files with {len(data_sources['ZMECON'])} rows")
    
    # Additional date validation on combined ZMECON
    combined_dates = pd.to_datetime(data_sources["ZMECON"].iloc[:, 23], errors='coerce')
    valid_dates = combined_dates[combined_dates >= CUTOFF_DATE]
    print(f"Date validation: {len(valid_dates)}/{len(combined_dates)} records have valid dates within 6-year range")
else:
    data_sources["ZMECON"] = None
    print("Warning: No ZMECON files were loaded successfully")

# Create composite dataset for EABL with additional filtering
if data_sources.get("EABL1") is not None and data_sources.get("EABL2") is not None:
    data_sources["EABL"] = pd.concat([data_sources["EABL1"], data_sources["EABL2"]], ignore_index=True)
    print(f"Created combined EABL dataset with {len(data_sources['EABL'])} rows")
else:
    data_sources["EABL"] = data_sources.get("EABL1") or data_sources.get("EABL2")
    if data_sources["EABL"] is not None:
        print(f"Using single EABL dataset with {len(data_sources['EABL'])} rows")

# Initialize output DataFrame
df_new = pd.DataFrame()

print("\nStarting field extraction and transformation for 6-year filtered data...")
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
# Extract METERNUMBER from ZMECON (Column U, index 20)
# --------------------------
if data_sources.get("ZMECON") is not None:
    df_new["METERNUMBER"] = data_sources["ZMECON"].iloc[:, 20].fillna('').astype(str).str.strip()
    print(f"Extracted {len(df_new)} METERNUMBER values")

# --------------------------
# Extract CURRREADDATE and PREVREADDATE from ZMECON (indexes 23 and 22)
# --------------------------
if data_sources.get("ZMECON") is not None:
    df_new["CURRREADDATE"] = pd.to_datetime(data_sources["ZMECON"].iloc[:, 23], errors='coerce').dt.strftime('%Y-%m-%d')
    df_new["PREVREADDATE"] = pd.to_datetime(data_sources["ZMECON"].iloc[:, 22], errors='coerce').dt.strftime('%Y-%m-%d')
    print(f"Extracted CURRREADDATE and PREVREADDATE values")
    
    # Validate date ranges
    curr_dates = pd.to_datetime(df_new["CURRREADDATE"], errors='coerce')
    valid_curr_dates = curr_dates[curr_dates >= CUTOFF_DATE]
    print(f"Date range validation: {len(valid_curr_dates)}/{len(curr_dates)} current read dates are within 6-year range")


# --------------------------
# Assign READINGTYPE based on meter patterns
# --------------------------
def determine_reading_type(meter_number):
    # Default to "0" (equivalent to RR="01") as it's the most common value in EABL
    if pd.isna(meter_number) or meter_number == "":
        return "0"
    
    # Convert to string and clean
    meter_str = str(meter_number).strip()
    
    # Rule: If meter number starts with "BGB", assign "0", otherwise "1"
    if meter_str.startswith("BGB"):
        return "0"
    else:
        return "1"

# Apply the function to every row
df_new["READINGTYPE"] = df_new["METERNUMBER"].apply(determine_reading_type)

# Verify that every row has a value
null_count = df_new["READINGTYPE"].isna().sum()
print(f"Rows with null READINGTYPE: {null_count} (should be 0)")
print(f"READINGTYPE value distribution: {df_new['READINGTYPE'].value_counts().to_dict()}")

# --------------------------
# Extract BILLINGUSAGE and BILLEDDATE from ZMECON (indexes 21 and 23)
# --------------------------
if data_sources.get("ZMECON") is not None:
    df_new["BILLEDDATE"] = pd.to_datetime(data_sources["ZMECON"].iloc[:, 23], errors='coerce').dt.strftime('%Y-%m-%d')
    print(f"Extracted BILLINGUSAGE and BILLEDDATE values")

# --------------------------
# Extract METERMULTIPLIER from ZDM_PREMDETAILS with proper matching
# --------------------------
if data_sources.get("ZDM_PREMDETAILS") is not None and data_sources.get("ZMECON") is not None:
    # Create a lookup table from ZDM_PREMDETAILS
    zdm_df = data_sources["ZDM_PREMDETAILS"].copy()
    
    # Extract the key for matching (could be Premise, Installation, etc.)
    zdm_df["Premise"] = zdm_df.iloc[:, 2].apply(
        lambda x: str(int(x)) if pd.notna(x) and isinstance(x, (int, float)) else str(x)
    ).str.strip()
    
    zdm_df["Pressure Factor"] = pd.to_numeric(zdm_df.iloc[:, 22], errors='coerce')
    
    # Create a lookup dictionary
    pressure_lookup = dict(zip(zdm_df["Premise"], zdm_df["Pressure Factor"]))
    
    # Apply the lookup to df_new based on LOCATIONID (which should be Premise)
    df_new["METERMULTIPLIER"] = df_new["LOCATIONID"].map(pressure_lookup).fillna(1.0)
    print(f"Assigned METERMULTIPLIER values to {(df_new['METERMULTIPLIER'] > 0).sum()} rows")
else:
    df_new["METERMULTIPLIER"] = 1.0
    print("Using default METERMULTIPLIER value of 1.0")

# --------------------------
# Assign CURRREADING (Fixed for Proper Chronological Progression)
# --------------------------
print("\nAssigning CURRREADING with proper chronological progression...")

if data_sources.get("EABL") is not None and data_sources.get("ZMECON") is not None:
    # Step 1: Prepare EABL data properly sorted by meter and date
    eabl_df = data_sources["EABL"].copy()
    
    # Clean and prepare EABL fields
    eabl_df["Device"] = eabl_df.iloc[:, 6].astype(str).str.strip()
    eabl_df["Installation"] = eabl_df.iloc[:, 3].astype(str).str.strip()
    eabl_df["Reading"] = pd.to_numeric(eabl_df.iloc[:, 8], errors='coerce').fillna(0)
    eabl_df["ReadDate"] = pd.to_datetime(eabl_df.iloc[:, 4], errors='coerce')
    
    # Remove invalid readings and sort properly
    eabl_df = eabl_df[eabl_df["Reading"] > 0]
    eabl_df = eabl_df.dropna(subset=["ReadDate"])
    eabl_df = eabl_df.sort_values(["Device", "ReadDate"])
    
    print(f"Prepared EABL data: {len(eabl_df)} valid readings")
    
    # Step 2: Create mapping strategies
    zmecon_df = data_sources["ZMECON"].copy()
    zmecon_df["Installation"] = zmecon_df.iloc[:, 26].astype(str).str.strip()
    zmecon_df["Meter"] = zmecon_df.iloc[:, 20].astype(str).str.strip()
    zmecon_df["CustomerID"] = zmecon_df.iloc[:, 0].apply(
        lambda x: str(int(x)) if pd.notna(x) and isinstance(x, (int, float)) else str(x)
    )
    
    # Create mapping dictionaries
    installation_to_customer = dict(zip(zmecon_df["Installation"], zmecon_df["CustomerID"]))
    meter_to_customer = dict(zip(zmecon_df["Meter"], zmecon_df["CustomerID"]))
    
    # Add customer IDs to EABL using multiple strategies
    eabl_df["CustomerID"] = eabl_df["Installation"].map(installation_to_customer).fillna(
        eabl_df["Device"].map(meter_to_customer)
    )
    
    # Keep only matched readings
    matched_eabl = eabl_df.dropna(subset=["CustomerID"])
    print(f"Successfully matched {len(matched_eabl)} EABL readings to customers")
 #replace from here
    if len(matched_eabl) > 0:
        # Step 3: Create meter-specific reading progressions
        print("Creating meter-specific reading progressions...")
        
        # Store original index to preserve order
        df_new["original_index"] = df_new.index
        
        # Sort df_new by meter and date to ensure proper chronological order
        df_new_sorted = df_new.copy()
        df_new_sorted["temp_date"] = pd.to_datetime(df_new_sorted["CURRREADDATE"], errors='coerce')
        df_new_sorted = df_new_sorted.sort_values(["METERNUMBER", "temp_date"])
        
        # Initialize CURRREADING column
        df_new_sorted["CURRREADING"] = 0
        
        # Process each unique meter
        unique_meters = df_new_sorted["METERNUMBER"].unique()
        meters_with_readings = 0
        
        print(f"Processing {len(unique_meters)} unique meters...")
        
        for meter_num in unique_meters:
            if pd.isna(meter_num) or meter_num == "":
                continue
                
            # Get all rows for this meter (chronologically sorted)
            meter_mask = df_new_sorted["METERNUMBER"] == meter_num
            meter_rows = df_new_sorted[meter_mask].copy()
            
            if len(meter_rows) == 0:
                continue
            
            # Try to find matching EABL readings for this meter or its customer
            # Strategy 1: Direct meter match
            meter_readings = matched_eabl[matched_eabl["Device"] == meter_num]
            
            # Strategy 2: If no direct match, try customer-based match
            if len(meter_readings) == 0:
                customer_id = meter_rows.iloc[0]["CUSTOMERID"]
                meter_readings = matched_eabl[matched_eabl["CustomerID"] == customer_id]
            
            # Strategy 3: If still no match, try partial meter number match
            if len(meter_readings) == 0:
                meter_short = str(meter_num)[:6]  # Use first 6 characters
                meter_readings = matched_eabl[matched_eabl["Device"].str.contains(meter_short, na=False)]
            
            if len(meter_readings) > 0:
                # Sort readings chronologically
                meter_readings = meter_readings.sort_values("ReadDate")
                readings_list = meter_readings["Reading"].tolist()
                
                # Assign readings chronologically to this meter's rows
                for i, idx in enumerate(meter_rows.index):
                    if i < len(readings_list):
                        # Use actual reading value
                        df_new_sorted.loc[idx, "CURRREADING"] = readings_list[i]
                    else:
                        # If we run out of readings, extrapolate based on last known reading
                        if len(readings_list) > 1:
                            # Calculate average monthly increase
                            avg_increase = (readings_list[-1] - readings_list[0]) / max(1, len(readings_list) - 1)
                            extrapolated_reading = readings_list[-1] + (avg_increase * (i - len(readings_list) + 1))
                            df_new_sorted.loc[idx, "CURRREADING"] = max(readings_list[-1], extrapolated_reading)
                        else:
                            # Just use the last reading
                            df_new_sorted.loc[idx, "CURRREADING"] = readings_list[-1]
                
                meters_with_readings += 1
        
        print(f"Assigned readings to {meters_with_readings} meters")
        
        # Restore original order properly using the stored index
        df_new_sorted = df_new_sorted.sort_values("original_index")
        df_new_sorted = df_new_sorted.drop(["temp_date", "original_index"], axis=1)
        df_new["CURRREADING"] = df_new_sorted["CURRREADING"].values  # Use .values to ensure alignment
 
        
    else:
        print("No customer matches found. Using sequential assignment with proper progression...")
        # Fallback: Create reasonable progression for each meter
        all_readings = pd.to_numeric(data_sources["EABL"].iloc[:, 8], errors='coerce')
        all_readings = all_readings[all_readings > 0].tolist()
        
        if len(all_readings) > 0:
            base_reading = int(sum(all_readings) / len(all_readings))  # Average reading as base
            
            for meter_num in df_new["METERNUMBER"].unique():
                if pd.isna(meter_num) or meter_num == "":
                    continue
                
                meter_mask = df_new["METERNUMBER"] == meter_num
                meter_rows = df_new[meter_mask].sort_values("CURRREADDATE")
                
                # Create progression starting from base reading
                for i, idx in enumerate(meter_rows.index):
                    monthly_increase = 100 + (i * 50)  # Reasonable monthly gas usage
                    df_new.loc[idx, "CURRREADING"] = base_reading + monthly_increase
        else:
            df_new["CURRREADING"] = 0
    
    # Ensure proper data types
    df_new["CURRREADING"] = pd.to_numeric(df_new["CURRREADING"], errors='coerce').fillna(0)
    df_new["CURRREADING"] = df_new["CURRREADING"].astype(int)
    
    # Set initial RAWUSAGE (will be recalculated after PREVREADING)
    df_new["RAWUSAGE"] = 0
    
    print(f"Final CURRREADING summary:")
    print(f"  Non-zero readings: {(df_new['CURRREADING'] > 0).sum():,}")
    print(f"  Reading range: {df_new['CURRREADING'].min():,} to {df_new['CURRREADING'].max():,}")

else:
    print("Warning: EABL or ZMECON data missing, cannot assign CURRREADING")
    df_new["CURRREADING"] = 0
    df_new["RAWUSAGE"] = 0

# Calculate PREVREADING based on sorted meter readings
if "CURRREADING" in df_new.columns and "METERNUMBER" in df_new.columns and "CURRREADDATE" in df_new.columns:
    print("Calculating PREVREADING and PREVREADDATE with proper logic...")
    
    # Convert CURRREADDATE to datetime for sorting (but keep original format)
    df_new["temp_currreaddate"] = pd.to_datetime(df_new["CURRREADDATE"], errors='coerce')
    
    # Sort by METERNUMBER and CURRREADDATE to ensure chronological order
    df_new = df_new.sort_values(by=["METERNUMBER", "temp_currreaddate"], na_position='last')
    df_new = df_new.reset_index(drop=True)
    
    # Calculate PREVREADING and PREVREADDATE by shifting within each meter group
    df_new["PREVREADING"] = df_new.groupby("METERNUMBER")["CURRREADING"].shift(1)
    df_new["PREVREADDATE"] = df_new.groupby("METERNUMBER")["CURRREADDATE"].shift(1)
    
    # Fill missing values appropriately
    df_new["PREVREADING"] = pd.to_numeric(df_new["PREVREADING"], errors='coerce').fillna(0)
    df_new["PREVREADDATE"] = df_new["PREVREADDATE"].fillna("")
    
    # Convert to proper data types
    df_new["PREVREADING"] = df_new["PREVREADING"].astype(int)
    
    # Drop the temporary date column
    df_new = df_new.drop("temp_currreaddate", axis=1)
    
    print(f"Calculated PREVREADING and PREVREADDATE for {len(df_new)} rows")
    
    # Validation summary
    non_zero_prev = (df_new["PREVREADING"] > 0).sum()
    print(f"Validation: {non_zero_prev:,} rows with non-zero PREVREADING")
    
else:
    df_new["PREVREADING"] = 0
    df_new["PREVREADDATE"] = ""
    print("Warning: Missing required columns for PREVREADING calculation")

# --------------------------
# Calculate RAWUSAGE with DTH Logic
# If EABL UMR = "DTH", then RAWUSAGE = CURRREADING
# Otherwise, RAWUSAGE = CURRREADING - PREVREADING
# --------------------------

# First, create a mapping from meter/installation to UMR values
print("Creating UMR mapping from EABL data...")

if data_sources.get("EABL") is not None:
    # Create UMR mapping from EABL
    eabl_umr_df = data_sources["EABL"].copy()
    eabl_umr_df["Device"] = eabl_umr_df.iloc[:, 6].astype(str).str.strip()      # Column G - Device
    eabl_umr_df["Installation"] = eabl_umr_df.iloc[:, 3].astype(str).str.strip() # Column D - Installation  
    eabl_umr_df["UMR"] = eabl_umr_df.iloc[:, 9].astype(str).str.strip()        # Column J - UMR
    
    # Create mapping dictionaries (Device and Installation to UMR)
    device_to_umr = {}
    installation_to_umr = {}
    
    # Build Device to UMR mapping
    for idx, row in eabl_umr_df.iterrows():
        device = row["Device"]
        installation = row["Installation"]
        umr = row["UMR"]
        
        if device and device not in ["", "nan", "NaN"]:
            device_to_umr[device] = umr
        if installation and installation not in ["", "nan", "NaN"]:
            installation_to_umr[installation] = umr
    
    print(f"Created UMR mappings: {len(device_to_umr):,} devices, {len(installation_to_umr):,} installations")
    
    # Count DTH occurrences
    dth_devices = sum(1 for umr in device_to_umr.values() if umr == "DTH")
    dth_installations = sum(1 for umr in installation_to_umr.values() if umr == "DTH")
    print(f"Found DTH values: {dth_devices:,} devices, {dth_installations:,} installations")
    
    # Add UMR column to df_new based on meter number and location ID
    df_new["UMR_TYPE"] = ""
    
    for idx, row in df_new.iterrows():
        meter = str(row["METERNUMBER"]).strip()
        location = str(row["LOCATIONID"]).strip()
        
        # Try to find UMR by meter number first, then by location ID
        umr_value = device_to_umr.get(meter, installation_to_umr.get(location, ""))
        df_new.loc[idx, "UMR_TYPE"] = umr_value
    
    # Calculate RAWUSAGE based on UMR_TYPE
    print("Calculating RAWUSAGE with DTH logic...")
    
    # Initialize RAWUSAGE
    df_new["RAWUSAGE"] = 0
    
    # For DTH meters: RAWUSAGE = CURRREADING
    dth_mask = df_new["UMR_TYPE"] == "DTH"
    df_new.loc[dth_mask, "RAWUSAGE"] = df_new.loc[dth_mask, "CURRREADING"]
    
    # For non-DTH meters: RAWUSAGE = CURRREADING - PREVREADING
    non_dth_mask = df_new["UMR_TYPE"] != "DTH"
    df_new.loc[non_dth_mask, "RAWUSAGE"] = (
        df_new.loc[non_dth_mask, "CURRREADING"] - df_new.loc[non_dth_mask, "PREVREADING"]
    )
    
    # Convert to integers
    df_new["RAWUSAGE"] = df_new["RAWUSAGE"].astype(int)
    
    # Validation summary
    dth_rows = dth_mask.sum()
    non_dth_rows = non_dth_mask.sum()
    
    print(f"RAWUSAGE calculation summary:")
    print(f"  DTH meters (RAWUSAGE = CURRREADING): {dth_rows:,} rows")
    print(f"  Non-DTH meters (RAWUSAGE = CURR - PREV): {non_dth_rows:,} rows")
    print(f"  RAWUSAGE range: {df_new['RAWUSAGE'].min():,} to {df_new['RAWUSAGE'].max():,}")
    
    # Show sample DTH calculations
    if dth_rows > 0:
        sample_dth = df_new[dth_mask].head(3)
        print(f"\nSample DTH calculations:")
        for idx, row in sample_dth.iterrows():
            print(f"  Row {idx}: UMR={row['UMR_TYPE']}, CURR={row['CURRREADING']}, RAWUSAGE={row['RAWUSAGE']}")
    
    # Check for negative usage
    negative_usage = (df_new["RAWUSAGE"] < 0).sum()
    print(f"Validation: {negative_usage:,} rows with negative RAWUSAGE (may indicate meter corrections)")
    
else:
    print("Warning: EABL data not available for UMR mapping")
    # Fallback to standard calculation
    df_new["RAWUSAGE"] = df_new["CURRREADING"] - df_new["PREVREADING"]
    df_new["RAWUSAGE"] = df_new["RAWUSAGE"].astype(int)


# --------------------------
# Assign THERMFACTOR from ThermFactor.xlsx
# --------------------------
if data_sources.get("TF") is not None:
    print("\nAssigning THERMFACTOR values...")
    therm_df = data_sources["TF"].copy()
    therm_df.columns = therm_df.columns.str.strip()
    therm_df["Valid from"] = pd.to_datetime(therm_df["Valid from"], errors="coerce")
    therm_df["Valid to"] = pd.to_datetime(therm_df["Valid to"], errors="coerce")
    
    # Use CURRREADDATE and PREVREADDATE from ZMECON for date range matching
    df_new["DATE_FROM"] = pd.to_datetime(data_sources["ZMECON"].iloc[:, 22], errors="coerce")
    df_new["DATE_TO"] = pd.to_datetime(data_sources["ZMECON"].iloc[:, 23], errors="coerce")
    
    def find_matching_btu(start, end):
        if pd.isna(start) or pd.isna(end):
            return 1.0  # Default value for missing dates
        
        match = therm_df[(therm_df["Valid from"] <= end) & (therm_df["Valid to"] >= start)]
        if not match.empty:
            return match.iloc[0]["Avg. BTU"]
        return 1.0  # Default if no match
    
    df_new["THERMFACTOR"] = df_new.apply(lambda row: find_matching_btu(row["DATE_FROM"], row["DATE_TO"]), axis=1)
    df_new.drop(columns=["DATE_FROM", "DATE_TO"], inplace=True)
    
    print(f"Assigned THERMFACTOR values to {(df_new['THERMFACTOR'] > 0).sum()} rows")
else:
    df_new["THERMFACTOR"] = 1.0
    print("Warning: ThermFactor file not loaded. Using default value of 1.0.")


# --------------------------
# Calculate BILLINGUSAGE using client's specific formula (without negative multiplier)
# Modified client formula: round((Round(([PRESENTREADING]-[PREVIOUSREADING])*[MULTIPLIER],0)*[THERMFACTOR]),3)
# --------------------------
print("\nCalculating BILLINGUSAGE using client's specific rounding formula (without negative)...")

# Ensure all components are numeric
df_new["CURRREADING"] = pd.to_numeric(df_new["CURRREADING"], errors='coerce').fillna(0)
df_new["PREVREADING"] = pd.to_numeric(df_new["PREVREADING"], errors='coerce').fillna(0)
df_new["METERMULTIPLIER"] = pd.to_numeric(df_new["METERMULTIPLIER"], errors='coerce').fillna(1.0)
df_new["THERMFACTOR"] = pd.to_numeric(df_new["THERMFACTOR"], errors='coerce').fillna(1.0)

# Apply the client's formula step by step using the CORRECT RAWUSAGE (which includes DTH logic)
# For DTH meters: RAWUSAGE = CURRREADING, so BILLINGUSAGE uses CURRREADING directly
# For non-DTH meters: RAWUSAGE = CURRREADING - PREVREADING

# Step 1: Use the already-calculated RAWUSAGE (which has DTH logic applied)
raw_usage_for_billing = pd.to_numeric(df_new["RAWUSAGE"], errors='coerce').fillna(0)

# Step 2: Apply multiplier and round to whole number
usage_with_multiplier = raw_usage_for_billing * df_new["METERMULTIPLIER"]
usage_rounded = usage_with_multiplier.round(0)

# Step 3: Apply thermal factor
usage_with_thermal = usage_rounded * df_new["THERMFACTOR"]

# Step 4: Final rounding to 3 decimal places (REMOVED *-1 since BILLINGUSAGE should be positive)
df_new["BILLINGUSAGE"] = usage_with_thermal.round(3)

# DO NOT OVERWRITE RAWUSAGE - it already has the correct DTH logic applied

# Validation and sample calculation
non_zero_billing = (df_new["BILLINGUSAGE"] != 0).sum()
positive_billing = (df_new["BILLINGUSAGE"] > 0).sum()

print(f"Calculated BILLINGUSAGE for {len(df_new)} rows using client formula (without negative)")
print(f"Non-zero BILLINGUSAGE values: {non_zero_billing:,}")
print(f"Positive BILLINGUSAGE values: {positive_billing:,} ({positive_billing/len(df_new)*100:.1f}%)")
print(f"BILLINGUSAGE range: {df_new['BILLINGUSAGE'].min():.3f} to {df_new['BILLINGUSAGE'].max():.3f}")

# Show detailed sample calculation for verification
if len(df_new) > 0:
    # Show both DTH and non-DTH examples
    dth_sample = df_new[df_new["UMR_TYPE"] == "DTH"].head(1)
    non_dth_sample = df_new[df_new["UMR_TYPE"] != "DTH"].head(1)
    
    if len(dth_sample) > 0:
        idx = dth_sample.index[0]
        print(f"\nSample DTH calculation:")
        print(f"  UMR=DTH: RAWUSAGE={df_new.loc[idx, 'RAWUSAGE']} = CURRREADING={df_new.loc[idx, 'CURRREADING']}")
        print(f"  BILLINGUSAGE = {df_new.loc[idx, 'RAWUSAGE']} * {df_new.loc[idx, 'METERMULTIPLIER']} * {df_new.loc[idx, 'THERMFACTOR']} = {df_new.loc[idx, 'BILLINGUSAGE']}")
    
    if len(non_dth_sample) > 0:
        idx = non_dth_sample.index[0]
        print(f"\nSample Non-DTH calculation:")
        print(f"  UMR≠DTH: RAWUSAGE={df_new.loc[idx, 'RAWUSAGE']} = CURR-PREV = {df_new.loc[idx, 'CURRREADING']}-{df_new.loc[idx, 'PREVREADING']}")
        print(f"  BILLINGUSAGE = {df_new.loc[idx, 'RAWUSAGE']} * {df_new.loc[idx, 'METERMULTIPLIER']} * {df_new.loc[idx, 'THERMFACTOR']} = {df_new.loc[idx, 'BILLINGUSAGE']}")

# Set BILLEDDATE to match CURRREADDATE since we're calculating billing usage
df_new["BILLEDDATE"] = df_new["CURRREADDATE"]
# --------------------------
# Assign BILLINGRATE and SALESREVENUECLASS with improved mapping logic
# --------------------------
if data_sources.get("ZMECON") is not None and data_sources.get("ZDM_PREMDETAILS") is not None:
    print("\nAssigning BILLINGRATE and SALESREVENUECLASS with comprehensive mapping logic...")
    
    # Define comprehensive mappings from the STAGE_METERED_SVCS file
    BILLINGRATE_category_mapping = {
        "T_ME_RESID": "8002",
        "T_ME_LIHEA": "8002",
        "T_ME_SCISL": "8040",
        "T_ME_LCISL": "8042",
        "T_ME_SCITR": "8040",
        "T_ME_LCITR": "8042",
        "G_ME_RESID": "8002",
        "G_ME_SCISL": "8040",
        "G_ME_LCISL": "8042",
        "G_ME_SCITR": "8040",
        "G_ME_LCITR": "8042",
        # Add simpler mappings as fallbacks
        "RES": "8002",
        "SCI": "8040",
        "LCI": "8042",
        "SCIT": "8040",
        "LCIT": "8042"
    }
     
    SALESREVENUECLASS_category_mapping = {
        "T_ME_RESID": "8002",
        "T_ME_LIHEA": "8002",
        "T_ME_SCISL": "8040",
        "T_ME_LCISL": "8042",
        "T_ME_SCITR": "8240",
        "T_ME_LCITR": "8242",
        "G_ME_RESID": "8002",
        "G_ME_SCISL": "8040",
        "G_ME_LCISL": "8042",
        "G_ME_SCITR": "8240",
        "G_ME_LCITR": "8242",
        # Add simpler mappings as fallbacks
        "RES": "8002",
        "SCI": "8040",
        "LCI": "8042",
        "SCIT": "8240",
        "LCIT": "8242"
    }
    
    # Define meter exceptions with custom rate values
    meter_exceptions = {
        "BG0848667": {"BILLINGRATE": "8265", "SALESREVENUECLASS": "8265"},
        "BGB01024": {"BILLINGRATE": "8261", "SALESREVENUECLASS": "8261"},
        "BG02-3000272": {"BILLINGRATE": "8261", "SALESREVENUECLASS": "8261"},
        "BGB01509": {"BILLINGRATE": "8262", "SALESREVENUECLASS": "8262"},
        "BGB00791": {"BILLINGRATE": "8267", "SALESREVENUECLASS": "8267"},
        "2052335": {"BILLINGRATE": "8261", "SALESREVENUECLASS": "8261"},
        "BGB00818": {"BILLINGRATE": "8261", "SALESREVENUECLASS": "8261"},
        "BGB002732": {"BILLINGRATE": "8269", "SALESREVENUECLASS": "8269"},
        "BGB00882": {"BILLINGRATE": "8261", "SALESREVENUECLASS": "8261"},
        "BG01-3400145": {"BILLINGRATE": "8268", "SALESREVENUECLASS": "8268"},
        "110327": {"BILLINGRATE": "8260", "SALESREVENUECLASS": "8260"},
        "1957609": {"BILLINGRATE": "8270", "SALESREVENUECLASS": "8270"},
        "2033572": {"BILLINGRATE": "8271", "SALESREVENUECLASS": "8271"},
        "1911924": {"BILLINGRATE": "8266", "SALESREVENUECLASS": "8266"},
        "BGB003389": {"BILLINGRATE": "8263", "SALESREVENUECLASS": "8063"},
        "BG1305837": {"BILLINGRATE": "8263", "SALESREVENUECLASS": "8063"},
        "23W914135": {"BILLINGRATE": "8263", "SALESREVENUECLASS": "8063"},
        "BGB02741": {"BILLINGRATE": "8263", "SALESREVENUECLASS": "8063"},
        "2228916": {"BILLINGRATE": "8263", "SALESREVENUECLASS": "8063"},
        "BGB01874": {"BILLINGRATE": "8263", "SALESREVENUECLASS": "8063"},
        "BGB02739": {"BILLINGRATE": "8263", "SALESREVENUECLASS": "8063"},
        "BGB00861": {"BILLINGRATE": "8263", "SALESREVENUECLASS": "8063"},
    }
    
    # Define excluded customer IDs
    excluded_customer_ids = {
        "210792305", "210806609", "210826823", "210800918", "210824447", "210830220", "210816965",
        "200332427", "200611277", "210820685", "210793791", "200413813", "200437326", "200561498",
        "210796711", "210797040", "210796579", "210796654", "210796769", "210796844", "210796909", "210796977"
    }
    
    # Create mapping dictionary from METERNUMBER to rate category using ZDM_PREMDETAILS
    meter_to_category = {}
    
    # Extract meter numbers and rate categories from ZDM_PREMDETAILS
    # ZDM_PREMDETAILS column structure: meter numbers in column 18, rate categories in column 4
    meters = data_sources["ZDM_PREMDETAILS"].iloc[:, 18].fillna('').astype(str)
    rate_categories = data_sources["ZDM_PREMDETAILS"].iloc[:, 4].fillna('').astype(str)
    
    # Build mapping from meter to rate category
    for i in range(len(meters)):
        meter = meters.iloc[i].strip()
        if meter:  # Only map non-empty meter numbers
            meter_to_category[meter] = rate_categories.iloc[i]
    
    print(f"Created mapping for {len(meter_to_category)} meter numbers to rate categories")
    
    # First, create a copy of the ZMECON Rate #1 column for fallback
    if "ZMECON" in data_sources and data_sources["ZMECON"] is not None:
        # Extract Rate #1 from ZMECON column 24
        rate_column = data_sources["ZMECON"].iloc[:, 24].fillna('').astype(str)
        
        # Process the rate values to extract the category (RES, SCI, etc.)
        def extract_rate_category(rate_value):
            # Strip spaces and convert to uppercase
            rate_value = rate_value.strip().upper()
            
            # Extract the category part
            if "RES" in rate_value:
                return "RES"
            elif "SCIT" in rate_value:
                return "SCIT"
            elif "LCIT" in rate_value:
                return "LCIT"
            elif "SCI" in rate_value:
                return "SCI"
            elif "LCI" in rate_value:
                return "LCI"
            else:
                return ""  # No match
        
        # Process each rate value
        zmecon_rate_categories = [extract_rate_category(rate) for rate in rate_column]
        
        # Create a mapping dictionary from CustomerID to rate category
        customer_to_rate_category = {}
        for i, customer_id in enumerate(data_sources["ZMECON"].iloc[:, 0].apply(
            lambda x: str(int(x)) if pd.notna(x) and isinstance(x, (int, float)) else str(x)
        )):
            if i < len(zmecon_rate_categories):
                customer_to_rate_category[customer_id] = zmecon_rate_categories[i]
        
        # Create a mapping from meter number to customer ID
        meter_to_customer = {}
        for i, meter in enumerate(data_sources["ZMECON"].iloc[:, 20].fillna('').astype(str)):
            if i < len(data_sources["ZMECON"]):
                customer_id = str(data_sources["ZMECON"].iloc[i, 0])
                if pd.notna(customer_id) and isinstance(customer_id, (int, float)):
                    customer_id = str(int(customer_id))
                meter_to_customer[meter.strip()] = customer_id
    
    # Initialize the fields in df_new
    df_new["BILLINGRATE"] = ""
    df_new["SALESREVENUECLASS"] = ""
    
    # Apply the mappings to each row in df_new
    for idx, row in df_new.iterrows():
        # Skip trailer row
        if idx == len(df_new) - 1 and row["CUSTOMERID"] == "TRAILER":
            continue
            
        meter = row['METERNUMBER'].strip() if isinstance(row['METERNUMBER'], str) else str(row['METERNUMBER']).strip()
        customer_id = row['CUSTOMERID'] if 'CUSTOMERID' in row else ""
        
        # Skip excluded customers
        if customer_id in excluded_customer_ids:
            continue
        
        # First check if this meter is in the exceptions list
        if meter in meter_exceptions:
            exception_mapping = meter_exceptions[meter]
            df_new.loc[idx, 'BILLINGRATE'] = exception_mapping.get('BILLINGRATE', "")
            df_new.loc[idx, 'SALESREVENUECLASS'] = exception_mapping.get('SALESREVENUECLASS', "")
            continue
            
        # Look up rate category for this meter from ZDM_PREMDETAILS
        rate_category = meter_to_category.get(meter, "")
        
        # If we found a rate category, use it to map values
        if rate_category:
            df_new.loc[idx, 'BILLINGRATE'] = BILLINGRATE_category_mapping.get(rate_category, "")
            df_new.loc[idx, 'SALESREVENUECLASS'] = SALESREVENUECLASS_category_mapping.get(rate_category, "")
        else:
            # Fallback: Use customer_to_rate_category mapping from ZMECON if available
            if "customer_to_rate_category" in locals() and customer_id in customer_to_rate_category:
                simple_category = customer_to_rate_category[customer_id]
                df_new.loc[idx, 'BILLINGRATE'] = BILLINGRATE_category_mapping.get(simple_category, "")
                df_new.loc[idx, 'SALESREVENUECLASS'] = SALESREVENUECLASS_category_mapping.get(simple_category, "")
    
    # Check results
    missing_br = sum(df_new['BILLINGRATE'] == "")
    missing_src = sum(df_new['SALESREVENUECLASS'] == "")
    
    print(f"After mapping: {missing_br} records missing BILLINGRATE, {missing_src} missing SALESREVENUECLASS")
    print(f"BILLINGRATE values: {pd.Series(df_new['BILLINGRATE']).value_counts().to_dict()}")
    print(f"SALESREVENUECLASS values: {pd.Series(df_new['SALESREVENUECLASS']).value_counts().to_dict()}")
elif data_sources.get("ZMECON") is not None:
    # Fallback to original simpler mappings if ZDM_PREMDETAILS is not available
    print("\nAssigning BILLINGRATE and SALESREVENUECLASS based on Rate #1 (simplified)...")
    
    # Define mappings
    BILLINGRATE_category_mapping = {
        "RES": "8002",
        "SCI": "8040",
        "LCI": "8042",
        "SCIT": "8040",
        "LCIT": "8042"
    }
     
    SALESREVENUECLASS_category_mapping = {
        "RES": "8002",
        "SCI": "8040",
        "LCI": "8042",
        "SCIT": "8240",
        "LCIT": "8242"
    }
    
    # Extract Rate #1 from ZMECON - it's at column index 24, not 20
    rate_column = data_sources["ZMECON"].iloc[:, 24].fillna('').astype(str)
    
    # Process the rate values to extract the category (RES, SCI, etc.)
    def extract_rate_category(rate_value):
        # Strip spaces and convert to uppercase
        rate_value = rate_value.strip().upper()
        
        # Extract the category part
        if "RES" in rate_value:
            return "RES"
        elif "SCIT" in rate_value:
            return "SCIT"
        elif "LCIT" in rate_value:
            return "LCIT"
        elif "SCI" in rate_value:
            return "SCI"
        elif "LCI" in rate_value:
            return "LCI"
        else:
            return ""  # No match
    
    # Process each row
    rate_categories = [extract_rate_category(rate) for rate in rate_column]
    
    # Map to billing rate and sales revenue class without defaults
    df_new["BILLINGRATE"] = [BILLINGRATE_category_mapping.get(cat, "") for cat in rate_categories]
    df_new["SALESREVENUECLASS"] = [SALESREVENUECLASS_category_mapping.get(cat, "") for cat in rate_categories]
    
    print(f"Assigned BILLINGRATE values: {pd.Series(df_new['BILLINGRATE']).value_counts().to_dict()}")
    print(f"Assigned SALESREVENUECLASS values: {pd.Series(df_new['SALESREVENUECLASS']).value_counts().to_dict()}")
else:
    # No default values if ZMECON is not available
    df_new["BILLINGRATE"] = ""
    df_new["SALESREVENUECLASS"] = ""
    print("No values assigned for BILLINGRATE and SALESREVENUECLASS (data sources not available)")


# --------------------------
# Extract BILLINGBATCHNUMBER from ZMECON (Column D - Print Document No., index 3)
# --------------------------
if data_sources.get("ZMECON") is not None:
    df_new["BILLINGBATCHNUMBER"] = data_sources["ZMECON"].iloc[:, 3].apply(
        lambda x: str(int(x))[2:10] if pd.notna(x) and isinstance(x, (int, float)) else ""
    )
    print(f"Extracted and truncated BILLINGBATCHNUMBER values from ZMECON column D")
    
    # Validation: Check the length of the truncated values
    max_length = df_new["BILLINGBATCHNUMBER"].str.len().max()
    print(f"Maximum BILLINGBATCHNUMBER length after truncation: {max_length} characters")
    
else:
    df_new["BILLINGBATCHNUMBER"] = ""
    print("Warning: ZMECON data not available for BILLINGBATCHNUMBER")


# --------------------------
# Assign hardcoded values for remaining required fields
# --------------------------
print("\nAssigning hardcoded values for fixed fields...")
df_new["APPLICATION"] = "5"
df_new["SERVICENUMBER"] = "1"
df_new["METERREGISTER"] = "1"
df_new["READINGCODE"] = "2"
df_new["UNITOFMEASURE"] = "CF"
df_new["READERID"] = " "
df_new["BILLEDAMOUNT"] = " "
df_new["HEATINGDEGREEDAYS"] = " "
df_new["COOLINGDEGREEDAYS"] = " "
df_new["UPDATEDATE"] = " "

# --------------------------
# COMPREHENSIVE DATA VALIDATION CHECKS (BEFORE FORMATTING!)
# --------------------------
print("\n" + "="*60)
print("COMPREHENSIVE DATA VALIDATION CHECKS")
print("="*60)

# 1. Check for completely empty critical fields
critical_fields = ["CUSTOMERID", "LOCATIONID", "METERNUMBER", "CURRREADDATE"]
for field in critical_fields:
    empty_count = (df_new[field] == "").sum() + df_new[field].isna().sum()
    if empty_count > 0:
        print(f"⚠️  WARNING: {empty_count:,} rows have empty {field}")
    else:
        print(f"✅ {field}: No empty values")

# 2. Check date format consistency
print(f"\n📅 DATE VALIDATION:")
# Check CURRREADDATE format
valid_curr_dates = pd.to_datetime(df_new["CURRREADDATE"], errors='coerce', format='%Y-%m-%d')
invalid_curr_dates = valid_curr_dates.isna().sum()
if invalid_curr_dates == 0:
    print(f"✅ CURRREADDATE: All dates in YYYY-MM-DD format")
else:
    print(f"⚠️  WARNING: {invalid_curr_dates:,} invalid CURRREADDATE values")

# Check PREVREADDATE (excluding blanks for first readings)
non_blank_prev = df_new[df_new["PREVREADDATE"] != ""]["PREVREADDATE"]
if len(non_blank_prev) > 0:
    valid_prev_dates = pd.to_datetime(non_blank_prev, errors='coerce', format='%Y-%m-%d')
    invalid_prev_dates = valid_prev_dates.isna().sum()
    if invalid_prev_dates == 0:
        print(f"✅ PREVREADDATE: All non-blank dates in YYYY-MM-DD format")
    else:
        print(f"⚠️  WARNING: {invalid_prev_dates:,} invalid PREVREADDATE values")

# 3. Check reading progression logic (sample first 100 meters)
print(f"\n📊 READING PROGRESSION VALIDATION:")
reading_issues = 0
sample_meters = [m for m in df_new["METERNUMBER"].unique()[:100] if pd.notna(m) and m != ""]

for meter in sample_meters:
    meter_data = df_new[df_new["METERNUMBER"] == meter].copy()
    if len(meter_data) > 1:
        meter_data["temp_date"] = pd.to_datetime(meter_data["CURRREADDATE"], errors='coerce')
        meter_data = meter_data.sort_values("temp_date")
        
        readings = pd.to_numeric(meter_data["CURRREADING"], errors='coerce').tolist()
        decreases = sum(1 for i in range(1, len(readings)) if readings[i] < readings[i-1])
        if decreases > len(readings) * 0.3:
            reading_issues += 1

if reading_issues > 0:
    print(f"⚠️  WARNING: {reading_issues} meters have frequent reading decreases (check for meter replacements)")
else:
    print(f"✅ Reading progression looks reasonable for sampled meters")

# 4. Check RAWUSAGE reasonableness (convert to numeric first to avoid string comparison errors)
print(f"\n⚡ USAGE VALIDATION:")
numeric_rawusage = pd.to_numeric(df_new["RAWUSAGE"], errors='coerce')
negative_usage = (numeric_rawusage < 0).sum()
zero_usage = (numeric_rawusage == 0).sum()
extreme_usage = (numeric_rawusage > 50000).sum()

print(f"📈 RAWUSAGE Statistics:")
print(f"   Negative usage: {negative_usage:,} rows ({negative_usage/len(df_new)*100:.1f}%)")
print(f"   Zero usage: {zero_usage:,} rows ({zero_usage/len(df_new)*100:.1f}%)")
print(f"   Extreme usage (>50k): {extreme_usage:,} rows ({extreme_usage/len(df_new)*100:.1f}%)")

if negative_usage > len(df_new) * 0.1:
    print(f"⚠️  WARNING: High percentage of negative usage - check meter reading logic")

# 5. Check BILLINGRATE/SALESREVENUECLASS mapping success
print(f"\n💰 BILLING VALIDATION:")
missing_billing_rate = (df_new["BILLINGRATE"] == "").sum()
missing_sales_class = (df_new["SALESREVENUECLASS"] == "").sum()

print(f"   Missing BILLINGRATE: {missing_billing_rate:,} rows ({missing_billing_rate/len(df_new)*100:.1f}%)")
print(f"   Missing SALESREVENUECLASS: {missing_sales_class:,} rows ({missing_sales_class/len(df_new)*100:.1f}%)")

if missing_billing_rate > len(df_new) * 0.05:
    print(f"⚠️  WARNING: High percentage of missing billing rates")

# 6. Check for duplicate rows
print(f"\n🔄 DUPLICATE CHECK:")
duplicates = df_new.duplicated(subset=["CUSTOMERID", "LOCATIONID", "METERNUMBER", "CURRREADDATE"]).sum()
if duplicates > 0:
    print(f"⚠️  WARNING: {duplicates:,} potential duplicate rows found")
else:
    print(f"✅ No duplicate rows found")

# 7. Final row count validation
print(f"\n📋 FINAL SUMMARY:")
total_rows = len(df_new)  # No trailer row yet
numeric_curr = pd.to_numeric(df_new["CURRREADING"], errors='coerce')
numeric_prev = pd.to_numeric(df_new["PREVREADING"], errors='coerce')

print(f"   Total data rows: {total_rows:,}")
print(f"   Non-zero CURRREADING: {(numeric_curr > 0).sum():,} ({(numeric_curr > 0).sum()/total_rows*100:.1f}%)")
print(f"   Non-zero PREVREADING: {(numeric_prev > 0).sum():,} ({(numeric_prev > 0).sum()/total_rows*100:.1f}%)")

# 8. Sample data preview for manual inspection
print(f"\n🔍 SAMPLE DATA (First 3 rows for Customer {df_new.iloc[0]['CUSTOMERID']}):")
sample_customer = df_new.iloc[0]["CUSTOMERID"]
sample_data = df_new[df_new["CUSTOMERID"] == sample_customer].head(3)
for idx, row in sample_data.iterrows():
    curr_reading = pd.to_numeric(row['CURRREADING'], errors='coerce')
    prev_reading = pd.to_numeric(row['PREVREADING'], errors='coerce')
    raw_usage = pd.to_numeric(row['RAWUSAGE'], errors='coerce')
    print(f"   Row {idx}: CURR={curr_reading}, PREV={prev_reading}, "
          f"USAGE={raw_usage}, DATE={row['CURRREADDATE']}")

print("="*60)
print("VALIDATION COMPLETE - Review warnings above before delivering")
print("="*60)

# --------------------------
# Format values with proper quoting
# --------------------------
print("\nFormatting field values...")
def custom_quote(val):
    if pd.isna(val) or val in ["", " "]:
        return ""
    return f'"{val}"'
    
def selective_custom_quote(val, column_name):
    if column_name in ['APPLICATION', 'SERVICENUMBER', 'METERREGISTER', 'READINGCODE', 'READINGTYPE',
                       'CURRREADING', 'PREVREADING', 'RAWUSAGE', 'BILLINGUSAGE', 'METERMULTIPLIER',
                       'THERMFACTOR', 'READERID', 'BILLEDAMOUNT', 'BILLINGBATCHNUMBER',
                       'BILLINGRATE', 'SALESREVENUECLASS', 'HEATINGDEGREEDAYS', 'COOLINGDEGREEDAYS', 'UPDATEDATE']:
        return val
    return "" if val in [None, 'nan', 'NaN', 'NAN'] else custom_quote(val)
    
df_new = df_new.fillna("")
for col in df_new.columns:
    df_new[col] = df_new[col].apply(lambda x: selective_custom_quote(x, col))

# --------------------------
# Reorder columns based on target format
# --------------------------
column_order = [
    "CUSTOMERID", "LOCATIONID", "APPLICATION", "SERVICENUMBER", "METERNUMBER",
    "METERREGISTER", "READINGCODE", "READINGTYPE", "CURRREADDATE",
    "PREVREADDATE", "CURRREADING", "PREVREADING", "UNITOFMEASURE", "RAWUSAGE",
    "BILLINGUSAGE", "METERMULTIPLIER", "BILLEDDATE", "THERMFACTOR", "READERID",
    "BILLEDAMOUNT", "BILLINGBATCHNUMBER", "BILLINGRATE", "SALESREVENUECLASS",
    "HEATINGDEGREEDAYS", "COOLINGDEGREEDAYS", "UPDATEDATE"
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
# this passed on 722 - output_path = os.path.join(os.path.dirname(list(file_paths.values())[0]), '722_test_STAGE_CONSUMPTION_HIST.csv')
output_path = os.path.join(os.path.dirname(list(file_paths.values())[0]), 'STAGE_CONSUMPTION_HIST.csv')

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
