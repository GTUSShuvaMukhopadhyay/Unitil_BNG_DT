# STAGE_CONSUMPTION_HISTORY
# STAGE_CONSUMPTION_HIST.py

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
    "ZDM_PREMDETAILS": r"STAGE_CONSUMPTION_HIST\ZDM_PREMDETAILS.XLSX",
    "ZMECON1": r"STAGE_CONSUMPTION_HIST\ZMECON 01012015 to 12312020.xlsx",
    "ZMECON2": r"STAGE_CONSUMPTION_HISTZMECON 01012021 to 02132025.xlsx",
    "EABL1": r"STAGE_CONSUMPTION_HIST\EABL 01012015 TO 12312019.XLSX",
    "EABL2": r"STAGE_CONSUMPTION_HIST\EABL 01012020 TO 2132025.XLSX",
    "MM": r"STAGE_CONSUMPTION_HIST\METERMULTIPLIER_PressureFactor.xlsx",
    "TF": r"STAGE_CONSUMPTION_HIST\ThermFactor.xlsx",
}

# Initialize data_sources dictionary to hold our data
data_sources = {}

# Function to read an Excel file (executed in parallel)
def read_excel_file(name, path):
    try:
        df = pd.read_excel(path, sheet_name="Sheet1", engine="openpyxl")
        print(f"Successfully loaded {name}: {df.shape[0]} rows, {df.shape[1]} columns")
        return name, df
    except Exception as e:
        print(f"Error loading {name}: {e}")
        return name, None

# Load files in parallel
print("Loading data sources...")
with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = {executor.submit(read_excel_file, name, path): name for name, path in file_paths.items()}
    for future in concurrent.futures.as_completed(futures):
        name, df = future.result()
        data_sources[name] = df

# Create composite datasets for ZMECON and EABL
if data_sources.get("ZMECON1") is not None and data_sources.get("ZMECON2") is not None:
    data_sources["ZMECON"] = pd.concat([data_sources["ZMECON1"], data_sources["ZMECON2"]], ignore_index=True)
    print(f"Created combined ZMECON dataset with {len(data_sources['ZMECON'])} rows")
else:
    data_sources["ZMECON"] = data_sources.get("ZMECON1") or data_sources.get("ZMECON2")
    if data_sources["ZMECON"] is not None:
        print(f"Using single ZMECON dataset with {len(data_sources['ZMECON'])} rows")

if data_sources.get("EABL1") is not None and data_sources.get("EABL2") is not None:
    data_sources["EABL"] = pd.concat([data_sources["EABL1"], data_sources["EABL2"]], ignore_index=True)
    print(f"Created combined EABL dataset with {len(data_sources['EABL'])} rows")
else:
    data_sources["EABL"] = data_sources.get("EABL1") or data_sources.get("EABL2")
    if data_sources["EABL"] is not None:
        print(f"Using single EABL dataset with {len(data_sources['EABL'])} rows")

# Initialize output DataFrame (df_new)
df_new = pd.DataFrame()

print("\nStarting field extraction and transformation...")

# --------------------------
# Extract CUSTOMERID from ZMECON (Column A = iloc[:, 0])
# --------------------------
if data_sources.get("ZMECON") is not None:
    df_new["CUSTOMERID"] = data_sources["ZMECON"].iloc[:, 0].apply(
        lambda x: str(int(x)) if pd.notna(x) and isinstance(x, (int, float)) else str(x)
    ).str.slice(0, 15)
    print(f"Extracted {len(df_new)} CUSTOMERID values")

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
    df_new["BILLINGUSAGE"] = pd.to_numeric(data_sources["ZMECON"].iloc[:, 21], errors='coerce').fillna(0)
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
# Assign CURRREADING and calculate PREVREADING
# --------------------------
print("\nAssigning CURRREADING and calculating PREVREADING...")

# Create a robust connection between EABL readings and ZMECON customer data
if data_sources.get("EABL") is not None and data_sources.get("ZMECON") is not None:
    # Step 1: Prepare the lookup data from EABL
    eabl_df = data_sources["EABL"].copy()
    
    # Clean the key fields
    eabl_df["Device"] = eabl_df.iloc[:, 6].astype(str).str.strip()  # Device column
    eabl_df["Installation"] = eabl_df.iloc[:, 3].astype(str).str.strip()  # Installation column
    eabl_df["Reading"] = pd.to_numeric(eabl_df.iloc[:, 8], errors='coerce')  # Predecimal column
    
    # Step 2: Prepare ZMECON for matching
    zmecon_df = data_sources["ZMECON"].copy()
    zmecon_df["Installation"] = zmecon_df.iloc[:, 26].astype(str).str.strip()  # Installation column
    zmecon_df["Meter"] = zmecon_df.iloc[:, 20].astype(str).str.strip()  # Meter column
    zmecon_df["CustomerID"] = zmecon_df.iloc[:, 0].apply(
        lambda x: str(int(x)) if pd.notna(x) and isinstance(x, (int, float)) else str(x)
    )
    
    # Step 3: Create a comprehensive matching structure
    # First, try to create a meter-to-installation mapping
    meter_to_installation = dict(zip(zmecon_df["Meter"], zmecon_df["Installation"]))
    
    # Then, create an installation-to-customerID mapping
    installation_to_customer = dict(zip(zmecon_df["Installation"], zmecon_df["CustomerID"]))
    
    # Step 4: Add installation information to EABL if it's missing
    if eabl_df["Installation"].isna().any():
        eabl_df["Installation"] = eabl_df["Device"].map(meter_to_installation)
    
    # Step 5: Add customer ID to EABL
    eabl_df["CustomerID"] = eabl_df["Installation"].map(installation_to_customer)
    
    # Step 6: Group EABL by CustomerID and get the readings
    customer_readings = {}
    for customer_id, group in eabl_df.groupby("CustomerID"):
        if pd.notna(customer_id):
            # Use the average or most recent reading for each customer
            customer_readings[customer_id] = group["Reading"].mean()
    
    # Step 7: Map these readings to df_new
    df_new["CURRREADING"] = df_new["CUSTOMERID"].map(customer_readings).fillna(0)
    
    # Check if we found any matches
    matches_found = (df_new['CURRREADING'] > 0).sum()
    print(f"Matched readings for {len(customer_readings)} customers")
    print(f"Rows with non-zero CURRREADING: {matches_found}")
    
    # If no matches were found, use direct assignment as fallback
    if matches_found == 0:
        print("No matches found using CustomerID mapping. Using direct assignment fallback.")
        
        # Direct sequential assignment (simplest but least accurate)
        if len(data_sources["EABL"]) > 0:
            readings = pd.to_numeric(data_sources["EABL"].iloc[:, 8], errors='coerce').fillna(0).tolist()
            readings_cycle = readings * (len(df_new) // len(readings) + 1)  # Repeat readings to cover all rows
            df_new["CURRREADING"] = readings_cycle[:len(df_new)]
            print(f"Direct assignment complete. Using {len(readings)} readings for {len(df_new)} rows.")
    
    # Set initial RAWUSAGE
    df_new["RAWUSAGE"] = df_new["CURRREADING"]
else:
    print("Warning: EABL or ZMECON data missing, cannot assign CURRREADING")
    df_new["CURRREADING"] = 0
    df_new["RAWUSAGE"] = 0

# Calculate PREVREADING based on sorted meter readings
if "CURRREADING" in df_new.columns and "METERNUMBER" in df_new.columns and "CURRREADDATE" in df_new.columns:
    # Store original CURRREADDATE format
    original_dates = df_new["CURRREADDATE"].copy()
    
    # Convert CURRREADDATE to datetime for sorting
    df_new["CURRREADDATE"] = pd.to_datetime(df_new["CURRREADDATE"], errors='coerce')
    
    # Sort by METERNUMBER and CURRREADDATE
    df_new.sort_values(by=["METERNUMBER", "CURRREADDATE"], inplace=True)
    
    # Calculate PREVREADING by shifting CURRREADING within each meter group
    df_new["PREVREADING"] = df_new.groupby("METERNUMBER")["CURRREADING"].shift(1)
    df_new["PREVREADING"] = pd.to_numeric(df_new["PREVREADING"], errors='coerce').fillna(0)
    df_new["PREVREADING"] = df_new["PREVREADING"].astype(int)

    
    # Update RAWUSAGE as CURRREADING - PREVREADING
    df_new["RAWUSAGE"] = df_new["CURRREADING"] - df_new["PREVREADING"]
    # Handle negative usage (meter rollover) by setting to 0
    df_new.loc[df_new["RAWUSAGE"] < 0, "RAWUSAGE"] = 0
    
    # remove decimal places from RAWUSAGE and BILLINGUSAGE
    df_new["RAWUSAGE"] = df_new["RAWUSAGE"].astype(int)
    df_new["BILLINGUSAGE"] = df_new["BILLINGUSAGE"].astype(int)


    # Restore original CURRREADDATE format
    df_new["CURRREADDATE"] = original_dates
    
    print(f"Calculated PREVREADING and updated RAWUSAGE for {len(df_new)} rows")
else:
    df_new["PREVREADING"] = 0
    print("Warning: Missing required columns for PREVREADING calculation")

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
df_new["BILLINGBATCHNUMBER"] = " "
df_new["BILLINGRATE"] = "2506"
df_new["SALESREVENUECLASS"] = "8211"
df_new["HEATINGDEGREEDAYS"] = " "
df_new["COOLINGDEGREEDAYS"] = " "
df_new["UPDATEDATE"] = " "

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
