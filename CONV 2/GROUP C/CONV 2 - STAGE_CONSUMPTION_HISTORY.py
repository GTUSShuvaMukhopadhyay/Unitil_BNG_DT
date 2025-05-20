# CONV 2 - STAGE_CONSUMPTION_HISTORY
# STAGE_CONSUMPTION_HIST.py
# updates were made to use mapping from STAGE_METERED_SVCS


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
    "ZDM_PREMDETAILS": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_CONSUMPTION_HIST\ZDM_PREMDETAILS.XLSX",
    "ZMECON1": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_CONSUMPTION_HIST\ZMECON 2015 to 2020.xlsx",
    "ZMECON2": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_CONSUMPTION_HIST\ZMECON 2021 to 03272025.xlsx",
    "EABL1": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_CONSUMPTION_HIST\EABL 01012015 to 12312019.XLSX",
    "EABL2": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_CONSUMPTION_HIST\EABL 01012020 to 03272025.XLSX",
    "TF": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_CONSUMPTION_HIST\ThermFactor.xlsx",
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