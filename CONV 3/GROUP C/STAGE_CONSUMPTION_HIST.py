# STAGE_CONSUMPTION_HIST.py
# updates were made to use mapping from STAGE_METERED_SVCS
# 8/1/2025 - Fixed RAWUSAGE Roll over issue.
# 815 - update the DTH Thermfactor


import pandas as pd
import os
import csv  # For CSV saving
import concurrent.futures  # For parallel file loading 
 
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
    "ZDM_PREMDETAILS": r"c:\Users\GTUSER1\Documents\CONV 3\ZDM_PREMDETAILS.XLSX",
    "ZMECON1": r"c:\Users\GTUSER1\Documents\CONV 3\ZMECON 08012019 to 08012025.xlsx",
    
    "EABL1": r"c:\Users\GTUSER1\Documents\CONV 3\EABL 08012019 TO 08012025.XLSX",
    "TF": r"c:\Users\GTUSER1\Documents\CONV 3\ThermFactor.xlsx",
}
 
# Initialize data_sources dictionary
data_sources = {}
 
# Function to read an Excel file with date filtering
def read_excel_file_with_filter(name, path):
    try:
        # Use correct sheet name based on file type
        if "ZMECON" in name:
            df = pd.read_excel(path, sheet_name="ZMECON", engine="openpyxl")
        else:
            df = pd.read_excel(path, sheet_name="Sheet1", engine="openpyxl")
        # Only apply filter to ZMECON files
        if "ZMECON" in name:
            # Convert column 23 (index 23) to datetime safely
            date_col = pd.to_datetime(df.iloc[:, 23], errors='coerce')
            start_date = pd.to_datetime("2019-06-01")
            end_date = pd.to_datetime("2025-09-14")
            mask = (date_col >= start_date) & (date_col <= end_date)
            original_rows = df.shape[0]
            df = df[mask]
            print(f"Filtered {name}: {original_rows} → {df.shape[0]} rows in date range {start_date.date()} to {end_date.date()}")

        else:
            print(f"Loaded {name}: {df.shape[0]} rows (no date filter)")

        return name, df
    except Exception as e:
        print(f"Error loading {name}: {e}")
        return name, None
 
# Load files in parallel
print("Loading and filtering data sources...")
data_sources = {}
with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = {executor.submit(read_excel_file_with_filter, name, path): name for name, path in file_paths.items()}
    for future in concurrent.futures.as_completed(futures):
        name, df = future.result()
        data_sources[name] = df
 
# Create composite dataset for ZMECON (including ZMECON2 with 2019 data)
zmecon_files = ["ZMECON1"]
zmecon_dfs = [data_sources.get(name) for name in zmecon_files if data_sources.get(name) is not None]
 
if len(zmecon_dfs) > 0:
    data_sources["ZMECON"] = pd.concat(zmecon_dfs, ignore_index=True)
    print(f"Created combined ZMECON dataset from {len(zmecon_dfs)} files with {len(data_sources['ZMECON'])} rows")
else:
    data_sources["ZMECON"] = None
    print("Warning: No ZMECON files were loaded successfully")
# SM had to comment out and recode for issue with EABL1 and EABL2
"""" 
# Create composite dataset for EABL with additional filtering
if data_sources.get("EABL1") is not None and data_sources.get("EABL2") is not None:
    data_sources["EABL"] = pd.concat([data_sources["EABL1"], data_sources["EABL2"]], ignore_index=True)
    print(f"Created combined EABL dataset with {len(data_sources['EABL'])} rows")
else:
    data_sources["EABL"] = data_sources.get("EABL1") or data_sources.get("EABL2")
    if data_sources["EABL"] is not None:
        print(f"Using single EABL dataset with {len(data_sources['EABL'])} rows")
"""
# Create composite dataset for EABL with additional filtering
if data_sources.get("EABL1") is not None and data_sources.get("EABL2") is not None:
    data_sources["EABL"] = pd.concat([data_sources["EABL1"], data_sources["EABL2"]], ignore_index=True)
    print(f"Created combined EABL dataset with {len(data_sources['EABL'])} rows")
else:
    eabl1 = data_sources.get("EABL1")
    eabl2 = data_sources.get("EABL2")
    if eabl1 is not None:
        data_sources["EABL"] = eabl1
    elif eabl2 is not None:
        data_sources["EABL"] = eabl2
    else:
        data_sources["EABL"] = None
    
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
    df_new["PREVREADDATE"] = (
        pd.to_datetime(data_sources["ZMECON"].iloc[:, 22], errors='coerce') - pd.Timedelta(days=1)
        ).dt.strftime('%Y-%m-%d')
    print(f"Extracted CURRREADDATE and PREVREADDATE values")

df_new["Installation"] = data_sources["ZMECON"].iloc[:, 26].astype(str).str.strip()
df_new["Device"] = data_sources["ZMECON"].iloc[:, 20].astype(str).str.strip()

# READINGTYPE Mapping — from EABL to df_new

# Prepare EABL lookup keys
eabl_df1 = data_sources["EABL"].copy()
eabl_df1["Installation"] = (
    eabl_df1.iloc[:, 3]
    .apply(lambda x: str(int(float(x))) if pd.notna(x) else "")
    .str.strip()
)
eabl_df1["ReadDate"] = pd.to_datetime(eabl_df1.iloc[:, 4], errors='coerce')
eabl_df1["ReadingType"] = eabl_df1.iloc[:, 10].apply(
    lambda x: str(int(float(x))) if pd.notna(x) and str(x).strip() != "" else ""
).str.strip()

# Create composite key in EABL
eabl_df1["match_key"] = (
    eabl_df1["Installation"] + "|" +
    eabl_df1["ReadDate"].dt.strftime("%Y-%m-%d")
)

# Create lookup dictionary from EABL
readingtype_lookup = dict(zip(eabl_df1["match_key"], eabl_df1["ReadingType"]))

# Prepare composite key for df_new using ZMECON fields and CURRREADDATE
# i will update the below commented out line for 
# install_vals = data_sources["ZMECON"].iloc[:, 26].apply(lambda x: str(int(float(x))) if pd.notna(x) else "").str.strip()
install_vals = data_sources["ZMECON"].iloc[:, 26].apply(
    lambda x: str(int(float(x))) if pd.notna(x) and str(x).strip() != "" and str(x).strip() != "nan" else ""
).str.strip()
curread_vals = pd.to_datetime(df_new["CURRREADDATE"], errors='coerce').dt.strftime("%Y-%m-%d")
match_keys = install_vals + "|" + curread_vals

# Map ReadingType from EABL into df_new using composite keys
df_new["READINGTYPE"] = match_keys.map(readingtype_lookup).fillna("")

# --------------------------
# Extract BILLINGBATCHNUMBER from ZMECON (Column D - Print Document No., index 3)
# --------------------------
if data_sources.get("ZMECON") is not None:
    
    # Define the 8-character billing batch function
    def clean_billingbatch_8char(value):
        """Extract 8-character billing batch number"""
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
                        return ""
                else:
                    int_val = int(str_value)
                
                str_result = str(int_val)
                
                # FIXED: Ensure consistent 8-character output
                if len(str_result) >= 10:
                    # For 10+ digit numbers: remove first 2, take next 8
                    return str_result[2:10]
                elif len(str_result) >= 9:
                    # For 9-digit numbers: remove first 1, take next 8
                    return str_result[1:9]
                elif len(str_result) >= 8:
                    # For 8-digit numbers: take all 8
                    return str_result
                elif len(str_result) >= 2:
                    # For shorter numbers: pad to 8 with leading zeros
                    extracted = str_result[2:] if len(str_result) > 2 else str_result
                    return extracted.zfill(8)
                else:
                    return ""
                    
            except (ValueError, OverflowError):
                return ""
        
        return ""

    # Apply the robust 8-character extraction
    df_new["BILLINGBATCHNUMBER"] = data_sources["ZMECON"].iloc[:, 3].apply(clean_billingbatch_8char)
    print(f"Extracted 8-character BILLINGBATCHNUMBER values from ZMECON column D")
    
    # Enhanced validation: Check the length consistency
    non_empty_batch = df_new[df_new["BILLINGBATCHNUMBER"] != ""]["BILLINGBATCHNUMBER"]
    if len(non_empty_batch) > 0:
        length_counts = non_empty_batch.str.len().value_counts().sort_index()
        print(f"BILLINGBATCHNUMBER length distribution: {length_counts.to_dict()}")
        
        # Check if all are 8 characters
        correct_length = (non_empty_batch.str.len() == 8).sum()
        total_non_empty = len(non_empty_batch)
        print(f"8-character records: {correct_length}/{total_non_empty} ({correct_length/total_non_empty*100:.1f}%)")
        
        # Flag any that aren't 8 characters
        wrong_length = non_empty_batch[non_empty_batch.str.len() != 8]
        if len(wrong_length) > 0:
            print(f"⚠️  {len(wrong_length)} records have incorrect length:")
            print(f"   Samples: {wrong_length.head(5).tolist()}")
        else:
            print(f"✅ All {total_non_empty} records have correct 8-character length")
    
else:
    df_new["BILLINGBATCHNUMBER"] = ""
    print("Warning: ZMECON data not available for BILLINGBATCHNUMBER")

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

# Prepare EABL lookup keys
eabl_df2 = data_sources["EABL"].copy()
eabl_df2["Installation"] = (
    eabl_df2.iloc[:, 3]
    .apply(lambda x: str(int(float(x))) if pd.notna(x) else "")
    .str.strip()
)
eabl_df2["ReadDate"] = pd.to_datetime(eabl_df2.iloc[:, 4], errors='coerce')
eabl_df2["predecimal"] = eabl_df2.iloc[:, 8]

# Create composite key in EABL
eabl_df2["match_key"] = (
    eabl_df2["Installation"] + "|" +
    eabl_df2["ReadDate"].dt.strftime("%Y-%m-%d")
)

print("EABL match_key examples:", eabl_df2["match_key"].dropna().unique()[:5])

print("EABL predecimal types:", eabl_df2["predecimal"].apply(type).value_counts())
print("Sample values:", eabl_df2["predecimal"].dropna().unique()[:5])


# Create lookup dictionary from EABL
curreading_lookup = dict(zip(eabl_df2["match_key"], eabl_df2["predecimal"]))

# Prepare composite key for df_new using ZMECON fields and CURRREADDATE
# encountered issue here with nan values in install_vals1
# install_vals1 = data_sources["ZMECON"].iloc[:, 26].apply(lambda x: str(int(float(x))) if pd.notna(x) else "").str.strip()
install_vals1 = data_sources["ZMECON"].iloc[:, 26].apply(
    lambda x: str(int(float(x))) if pd.notna(x) and str(x).strip() != "" and str(x).strip() != "nan" else ""
).str.strip()
curread_vals1 = pd.to_datetime(df_new["CURRREADDATE"], errors='coerce').dt.strftime("%Y-%m-%d")
match_keys1 = install_vals1 + "|" + curread_vals1

print("df_new match_keys1 examples:", match_keys1.dropna().unique()[:5])

# Map Curreading from EABL into df_new using composite keys
df_new["CURRREADING"] = match_keys1.map(curreading_lookup)
print("CURREADING created:", "CURRREADING" in df_new.columns)
print(df_new["CURRREADING"].head())

'''
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
'''
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
   
    # Fill missing values appropriately
    df_new["PREVREADING"] = pd.to_numeric(df_new["PREVREADING"], errors='coerce').fillna(0)
   
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
    rollover_mask = (non_dth_mask) & (df_new["CURRREADING"] < df_new["PREVREADING"])
    normal_usage_mask = non_dth_mask & ~rollover_mask
    df_new.loc[normal_usage_mask, "RAWUSAGE"] = (
        df_new.loc[normal_usage_mask, "CURRREADING"] - df_new.loc[normal_usage_mask, "PREVREADING"]
    )

    def calculate_rollover_usage(row):
        curr = row["CURRREADING"]
        prev = row["PREVREADING"]

        # Infer max_reading from typical rollover thresholds
        if prev > 900000:                   # assume 1000000-reset meter
            max_reading = 1000000
        elif prev > 90000:               # assume 100000-reset meter
            max_reading = 100000
        elif prev > 9000:              # assume 10000-reset meter
            max_reading = 10000
        else:
            return None  # Unknown or invalid rollover scenario

        return (max_reading - prev) + curr

    # Apply only to rollover rows
    df_new.loc[rollover_mask, "RAWUSAGE"] = df_new.loc[rollover_mask].apply(calculate_rollover_usage, axis=1)

   
    # Convert to integers
    df_new["RAWUSAGE"] = df_new["RAWUSAGE"].fillna(0).astype(int)
   
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
    '''  
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
   
    print(f"Assigned THERMFACTOR values to {(df_new['THERMFACTOR'] > 0).sum()} rows")'''
    
    def get_therm_factor(curr_date):
        if pd.isna(curr_date):
            return None
        match = therm_df[
            (therm_df["Valid from"] <= curr_date) &
            (therm_df["Valid to"] >= curr_date)
        ]
        if not match.empty:
            return match.iloc[0]["Avg. BTU"]
        return None

    df_new["THERMFACTOR"] = df_new["CURRREADDATE"].apply(get_therm_factor)
    dth_mask = df_new["UMR_TYPE"] == "DTH"
    df_new.loc[dth_mask, "THERMFACTOR"] = 1.0
    
    dth_count = dth_mask.sum()
    total_count = len(df_new)
    print(f"Set THERMFACTOR to 1.0 for {dth_count:,} DTH meters out of {total_count:,} total rows")
    
    # Show summary of THERMFACTOR values
    therm_summary = df_new["THERMFACTOR"].value_counts().sort_index()
    print(f"THERMFACTOR distribution: {dict(therm_summary.head(10))}")

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
    print("\n✅ Assigning BILLINGRATE and SALESREVENUECLASS with dictionary-based lookup...")
    BILLINGRATE_category_mapping = {
        "T_ME_RESID": "8002", "T_ME_LIHEA": "8002", "T_ME_SCISL": "8040", "T_ME_LCISL": "8042",
        "T_ME_SCITR": "8040", "T_ME_LCITR": "8042", "G_ME_RESID": "8002", "G_ME_SCISL": "8040",
        "G_ME_LCISL": "8042", "G_ME_SCITR": "8040", "G_ME_LCITR": "8042", "RES": "8002",
        "SCI": "8040", "LCI": "8042", "SCIT": "8040", "LCIT": "8042"
        }

    SALESREVENUECLASS_category_mapping = {
        "T_ME_RESID": "8002", "T_ME_LIHEA": "8002", "T_ME_SCISL": "8040", "T_ME_LCISL": "8042",
        "T_ME_SCITR": "8240", "T_ME_LCITR": "8242", "G_ME_RESID": "8002", "G_ME_SCISL": "8040",
        "G_ME_LCISL": "8042", "G_ME_SCITR": "8240", "G_ME_LCITR": "8242", "RES": "8002",
        "SCI": "8040", "LCI": "8042", "SCIT": "8240", "LCIT": "8242"
        }

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

    excluded_customer_ids = {
        "210792305", "210806609", "210826823", "210800918", "210824447", "210830220", "210816965",
        "200332427", "200611277", "210820685", "210793791", "200413813", "200437326", "200561498",
        "210796711", "210797040", "210796579", "210796654", "210796769", "210796844", "210796909", "210796977"
    }

    print("\n🔍 Preparing ZDM_PREMDETAILS data...")
    zdm_df = data_sources["ZDM_PREMDETAILS"].iloc[:, [7, 18, 4]].copy()
    zdm_df.columns = ["CUSTOMERID", "METERNUMBER", "RATE_CATEGORY"]
    zdm_df["CUSTOMERID"] = zdm_df["CUSTOMERID"].apply(lambda x: str(x).lstrip("0").strip())
    zdm_df["CUSTOMERID"] = pd.to_numeric(zdm_df["CUSTOMERID"], errors='coerce').dropna().astype("int64").astype(str)

    df_new["CUSTOMERID"] = df_new["CUSTOMERID"].astype(str).str.strip()
    df_new = df_new[~df_new["CUSTOMERID"].isin(excluded_customer_ids)].copy()

    meter_lookup = dict(zip(zdm_df["CUSTOMERID"], zdm_df["METERNUMBER"]))
    category_lookup = dict(zip(zdm_df["CUSTOMERID"], zdm_df["RATE_CATEGORY"]))

    #df_new["METERNUMBER"] = df_new["CUSTOMERID"].map(meter_lookup)
    df_new["RATE_CATEGORY"] = df_new["CUSTOMERID"].map(category_lookup)

    # Fallback to ZMECON if RATE_CATEGORY is still missing
    fallback_mask = df_new["RATE_CATEGORY"].isna()
    zmecon_df = data_sources["ZMECON"]
    if zmecon_df.shape[1] > 24:
        rate_column = zmecon_df.iloc[:, 24].fillna('').astype(str)
        def extract_rate_category(rate_value):
            rate_value = rate_value.strip().upper()
            if "RES" in rate_value: return "RES"
            elif "SCIT" in rate_value: return "SCIT"
            elif "LCIT" in rate_value: return "LCIT"
            elif "SCI" in rate_value: return "SCI"
            elif "LCI" in rate_value: return "LCI"
            else: return ""

        zmecon_df["RATE_CATEGORY"] = rate_column.map(extract_rate_category)
        zmecon_df["CUSTOMERID"] = zmecon_df.iloc[:, 0].apply(lambda x: str(int(x)).strip() if pd.notna(x) and isinstance(x, (int, float)) else str(x).strip())
        fallback_lookup = dict(zip(zmecon_df["CUSTOMERID"], zmecon_df["RATE_CATEGORY"]))

        df_new.loc[fallback_mask, "RATE_CATEGORY"] = df_new.loc[fallback_mask, "CUSTOMERID"].map(fallback_lookup)

    # Apply meter exceptions
    df_new["BILLINGRATE"] = df_new["METERNUMBER"].map(lambda x: meter_exceptions.get(x, {}).get("BILLINGRATE", ""))
    df_new["SALESREVENUECLASS"] = df_new["METERNUMBER"].map(lambda x: meter_exceptions.get(x, {}).get("SALESREVENUECLASS", ""))

    # Fill remaining from RATE_CATEGORY
    br_mask = df_new["BILLINGRATE"] == ""
    src_mask = df_new["SALESREVENUECLASS"] == ""
    df_new.loc[br_mask, "BILLINGRATE"] = df_new.loc[br_mask, "RATE_CATEGORY"].map(BILLINGRATE_category_mapping)
    df_new.loc[src_mask, "SALESREVENUECLASS"] = df_new.loc[src_mask, "RATE_CATEGORY"].map(SALESREVENUECLASS_category_mapping)

    print("✅ BILLINGRATE mapping complete. Missing:", (df_new["BILLINGRATE"] == "").sum())
    print("✅ SALESREVENUECLASS mapping complete. Missing:", (df_new["SALESREVENUECLASS"] == "").sum())

else:
    print("⚠️ Required sources ZMECON or ZDM_PREMDETAILS not available")
    df_new["BILLINGRATE"] = ""
    df_new["SALESREVENUECLASS"] = ""


# --------------------------
# Assign hardcoded values for remaining required fields
# --------------------------
print("\nAssigning hardcoded values for fixed fields...")

'''
if data_sources.get("ZMECON") is not None:
    df_new["METERNUMBER"] = data_sources["ZMECON"].iloc[:, 20].apply(
        lambda x: str(int(x)) if pd.notna(x) and isinstance(x, (int, float)) else str(x)
    ).str.slice(0, 15)
    print(f"Extracted {len(df_new)} CUSTOMERID values")'''

df_new["APPLICATION"] = 5
df_new["SERVICENUMBER"] = 1
df_new["METERREGISTER"] = 1
df_new["READINGCODE"] = 2
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
   
df_new = df_new[df_new["CURRREADING"] != 0]
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
# output_path = os.path.join(os.path.dirname(list(file_paths.values())[0]), 'STAGE_CONSUMPTION_HIST_8_1_Checking CURREAD.csv')
output_path = r"C:\Users\GTUSER1\Documents\CONV 3\output\Group C\STAGE_CONSUMPTION_HIST.csv"

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
