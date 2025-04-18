# STAGE_METERED_SVCS_(POST CONV 1 Updated).py

# 02_0417_POST_CONV1 -  STAGE_METERED_SVCS.py was successful test file and this is now the final version.

# 04172025 redone and updates made to:
#   INITIALSERVICEDATE,
#   BILLINGSTARTDATE, 
#   LASTREADING, 
#   LASTREADDATE,
#   REMOVEDDATE

# 04182025 redone and updates made to:
#   SERVICESTATUS


# STAGE_METERED_SVCS.py
 
# we need to exclude the contractids in the list below from our data set ~ will code around it later
# ISSUES ARE MULITPLIER AND BILLINGRATE1
 
import pandas as pd
import os
import re
import csv  # Import the correct CSV module
 
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
    "ZDM_PREMDETAILS":  r"C:\Users\us85360\Desktop\STAGE_METERED_SVCS\ZDM_PREMDETAILS.XLSX",
    "ZNC_ACTIVE_CUS": r"C:\Users\us85360\Desktop\STAGE_METERED_SVCS\ZNC_ACTIVE_CUS.XLSX",
    "EABL1": r"C:\Users\us85360\Desktop\STAGE_METERED_SVCS\EABL 01012020 TO 2132025.XLSX",
    "EABL2": r"C:\Users\us85360\Desktop\STAGE_METERED_SVCS\EABL 01012015 TO 12312019.XLSX",
    "MM": r"C:\Users\us85360\Desktop\STAGE_METERED_SVCS\METERMULTIPLIER_PressureFactor.xlsx",
 
}
 
# Load the data from each spreadsheet
data_sources = {}
for name, path in file_paths.items():
    try:
        data_sources[name] = pd.read_excel(path, sheet_name="Sheet1", engine="openpyxl")
    except Exception as e:
        data_sources[name] = None
        print(f"Error loading {name}: {e}")
 
data_sources["EABL"] = pd.concat([data_sources["EABL1"], data_sources["EABL2"]], ignore_index=True)
 
# Initialize df_new as an empty DataFrame
df_new = pd.DataFrame()
 
# Extract CUSTOMERID from ZDM_PREMDETAILS
if data_sources["ZDM_PREMDETAILS"] is not None:
    df_new["CUSTOMERID"] = data_sources["ZDM_PREMDETAILS"].iloc[:, 7].fillna('').apply(lambda x: str(int(x)) if isinstance(x, (int, float)) else str(x)).str.slice(0, 15)
 
# Extract LOCATIONID from ZDM_PREMDETAILS
if data_sources["ZDM_PREMDETAILS"] is not None:
    df_new["LOCATIONID"] = data_sources["ZDM_PREMDETAILS"].iloc[:, 2].fillna('').astype(str)
 
# Extract METERNUMBER from ZDM_PREMDETAILS
if data_sources["ZDM_PREMDETAILS"] is not None:
    df_new["METERNUMBER"] = data_sources["ZDM_PREMDETAILS"].iloc[:, 18].fillna('').astype(str)
 
# Define exclusion list for CUSTOMERID
excluded_customer_ids = {
    "210792305", "210806609", "210826823", "210800918", "210824447", "210830220", "210816965",
    "200332427", "200611277", "210820685", "210793791", "200413813", "200437326", "200561498",
    "210796711", "210797040", "210796579", "210796654", "210796769", "210796844", "210796909", "210796977"
}
 
# Define mappings
BILLINGRATE1_category_mapping = {
    "T_ME_RESID": "8002",
    "T_ME_SCISL": "8040",
    "T_ME_LCISL": "8042",
    "T_ME_SCITR": "8040",
    "T_ME_LCITR": "8042",
    "G_ME_RESID": "8002",
    "G_ME_SCISL": "8040",
    "G_ME_LCISL": "8042",
    "G_ME_SCITR": "8040",
    "G_ME_LCITR": "8042"
}
 
SALESCLASS1_category_mapping = {
    "T_ME_RESID": "8002",
    "T_ME_SCISL": "8040",
    "T_ME_LCISL": "8042",
    "T_ME_SCITR": "8240",
    "T_ME_LCITR": "8242",
    "G_ME_RESID": "8002",
    "G_ME_SCISL": "8040",
    "G_ME_LCISL": "8042",
    "G_ME_SCITR": "8240",
    "G_ME_LCITR": "8242"
}
 
BILLINGRATE2_category_mapping = {
    "T_ME_RESID": "8300",
    "T_ME_SCISL": "8302",
    "T_ME_LCISL": "8304",
    "T_ME_SCITR": "9800",
    "G_ME_LCITR": "9800",
    "G_ME_RESID": "8300",
    "G_ME_SCISL": "8302",
    "G_ME_LCISL": "8304",
    "G_ME_SCITR": "9800",
    "G_ME_LCITR": "9800"
}
 
SALESCLASS2_category_mapping = {
    "T_ME_RESID": "8002",
    "T_ME_SCISL": "8040",
    "T_ME_LCISL": "8042",
    "T_ME_SCITR": "8240",
    "T_ME_LCITR": "8242",
    "G_ME_RESID": "8002",
    "G_ME_SCISL": "8040",
    "G_ME_LCISL": "8042",
    "G_ME_SCITR": "8240",
    "G_ME_LCITR": "8242"
}
 
# Extract BILLINGRATE1, SALESCLASS1, BILLINGRATE2, and SALESCLASS2 from ZDM_PREMDETAILS
if data_sources["ZDM_PREMDETAILS"] is not None:
    rate_category_column = data_sources["ZDM_PREMDETAILS"].iloc[:, 4].fillna('').astype(str)
    df_new["BILLINGRATE1"] = [BILLINGRATE1_category_mapping.get(rate_category_column[i], "") if df_new["CUSTOMERID"].iloc[i] not in excluded_customer_ids else "" for i in range(len(df_new))]
    df_new["SALESCLASS1"] = [SALESCLASS1_category_mapping.get(rate_category_column[i], "") if df_new["CUSTOMERID"].iloc[i] not in excluded_customer_ids else "" for i in range(len(df_new))]
    df_new["BILLINGRATE2"] = [BILLINGRATE2_category_mapping.get(rate_category_column[i], "") if df_new["CUSTOMERID"].iloc[i] not in excluded_customer_ids else "" for i in range(len(df_new))]
    df_new["SALESCLASS2"] = [SALESCLASS2_category_mapping.get(rate_category_column[i], "") if df_new["CUSTOMERID"].iloc[i] not in excluded_customer_ids else "" for i in range(len(df_new))]

# UPDATED CODE FOR INITIALSERVICEDATE/BILLINGSTARTDATE
# Extract from ZNC_ACTIVE_CUS and convert to proper format
if data_sources["ZNC_ACTIVE_CUS"] is not None:
    # Print column names for debugging - remove in final version
    print("ZNC_ACTIVE_CUS columns:")
    for i, col_name in enumerate(data_sources["ZNC_ACTIVE_CUS"].columns):
        print(f"Column {i}: {col_name}")
        
    try:
        # Create a copy of ZNC_ACTIVE_CUS data for processing
        znc_df = data_sources["ZNC_ACTIVE_CUS"].copy()
        
        # Print sample values from MR Unit column to understand format
        print("\nSample MR Unit values from ZNC_ACTIVE_CUS:")
        print(znc_df.iloc[0:5, 0].tolist())
        
        # Print sample values from CUSTOMERID in df_new
        print("\nSample CUSTOMERID values from df_new:")
        print(df_new["CUSTOMERID"].head().tolist())
        
        # Try different approaches to format the customer ID
        # Approach 1: Basic formatting
        znc_df["MATCH_CUSTOMERID1"] = znc_df.iloc[:, 0].fillna('').astype(str).str.strip()
        
        # Approach 2: Convert to integer then string (handles numeric IDs)
        znc_df["MATCH_CUSTOMERID2"] = znc_df.iloc[:, 0].fillna('').apply(
            lambda x: str(int(float(x))) if isinstance(x, (int, float)) or (isinstance(x, str) and x.replace('.', '', 1).isdigit()) else x
        )
        
        # Approach 3: Try mapping using Premise (column 1) instead
        znc_df["MATCH_CUSTOMERID3"] = znc_df.iloc[:, 1].fillna('').astype(str).str.strip()
        
        # Approach 4: Try BPartner (column 2)
        znc_df["MATCH_CUSTOMERID4"] = znc_df.iloc[:, 2].fillna('').astype(str).str.strip()
        
        # Convert service date
        znc_df["SERVICE_DATE"] = pd.to_datetime(znc_df.iloc[:, 7], errors='coerce').dt.strftime('%Y-%m-%d')
        
        # Check matches for each approach
        matches1 = sum(df_new["CUSTOMERID"].isin(znc_df["MATCH_CUSTOMERID1"]))
        matches2 = sum(df_new["CUSTOMERID"].isin(znc_df["MATCH_CUSTOMERID2"]))
        matches3 = sum(df_new["CUSTOMERID"].isin(znc_df["MATCH_CUSTOMERID3"]))
        matches4 = sum(df_new["CUSTOMERID"].isin(znc_df["MATCH_CUSTOMERID4"]))
        
        print(f"Matches using approach 1: {matches1} out of {len(df_new)}")
        print(f"Matches using approach 2: {matches2} out of {len(df_new)}")
        print(f"Matches using approach 3: {matches3} out of {len(df_new)}")
        print(f"Matches using approach 4: {matches4} out of {len(df_new)}")
        
        # Choose the best approach based on match count
        max_matches = max(matches1, matches2, matches3, matches4)
        if max_matches > 0:
            if max_matches == matches1:
                match_col = "MATCH_CUSTOMERID1"
                print("Using approach 1 for customer matching")
            elif max_matches == matches2:
                match_col = "MATCH_CUSTOMERID2"
                print("Using approach 2 for customer matching")
            elif max_matches == matches3:
                match_col = "MATCH_CUSTOMERID3"
                print("Using approach 3 for customer matching (Premise column)")
            else:
                match_col = "MATCH_CUSTOMERID4"
                print("Using approach 4 for customer matching (BPartner column)")
            
            # Create a mapping dictionary using the best approach
            customer_to_date = dict(zip(znc_df[match_col], znc_df["SERVICE_DATE"]))
            
            # Map values to df_new
            df_new["INITIALSERVICEDATE"] = df_new["CUSTOMERID"].map(customer_to_date)
            df_new["BILLINGSTARTDATE"] = df_new["CUSTOMERID"].map(customer_to_date)
            
            # Print statistics on populated fields
            initial_count = sum(~df_new["INITIALSERVICEDATE"].isna())
            billing_count = sum(~df_new["BILLINGSTARTDATE"].isna())
            
            print(f"Populated INITIALSERVICEDATE for {initial_count} rows")
            print(f"Populated BILLINGSTARTDATE for {billing_count} rows")
        else:
            print("No matches found between ZDM_PREMDETAILS and ZNC_ACTIVE_CUS")
            print("Attempting direct assignment method...")
            
            # Fall back to direct assignment if no matches found
            df_new["INITIALSERVICEDATE"] = pd.to_datetime(data_sources["ZNC_ACTIVE_CUS"].iloc[:, 7], errors='coerce').dt.strftime('%Y-%m-%d')
            df_new["BILLINGSTARTDATE"] = pd.to_datetime(data_sources["ZNC_ACTIVE_CUS"].iloc[:, 7], errors='coerce').dt.strftime('%Y-%m-%d')
    
    except Exception as e:
        print(f"Error processing ZNC_ACTIVE_CUS data: {e}")
        # Ensure these columns exist even if processing fails
        if 'INITIALSERVICEDATE' not in df_new.columns:
            df_new['INITIALSERVICEDATE'] = ""
        if 'BILLINGSTARTDATE' not in df_new.columns:
            df_new['BILLINGSTARTDATE'] = ""

# UPDATED CODE FOR LASTREADING/LASTREADDATE 04172025

# Extract from EABL based on most recent date by DEVICE and link to CUSTOMERID
if data_sources["EABL"] is not None:
    # Print column names for debugging - can remove in final version
    print("EABL columns:")
    for i, col_name in enumerate(data_sources["EABL"].columns):
        print(f"Column {i}: {col_name}")
    
    try:
        # Create a copy of EABL data for processing
        eabl_df = data_sources["EABL"].copy()
        
        # Convert 'Schd MRD' to datetime for proper comparison
        eabl_df['Schd MRD'] = pd.to_datetime(eabl_df['Schd MRD'], errors='coerce')
        
        # Drop rows with invalid dates
        eabl_df = eabl_df.dropna(subset=['Schd MRD'])
        
        print(f"Total rows in EABL after removing invalid dates: {len(eabl_df)}")
        print(f"Total unique devices in EABL: {eabl_df['Device'].nunique()}")
        
        # Sort by date (descending) and drop duplicates to keep only the most recent reading for each Device
        latest_readings = eabl_df.sort_values('Schd MRD', ascending=False).drop_duplicates('Device')
        
        print(f"Found {len(latest_readings)} unique devices with latest readings")
        
        # Create mappings from Device to reading value and date
        device_to_reading = dict(zip(
            latest_readings['Device'].astype(str).str.strip(),
            latest_readings['Predecimal']
        ))
        
        device_to_date = dict(zip(
            latest_readings['Device'].astype(str).str.strip(),
            latest_readings['Schd MRD'].dt.strftime('%Y-%m-%d')
        ))
        
        # First create a mapping from meternumber to customerid in df_new
        meter_to_customer = dict(zip(
            df_new['METERNUMBER'].astype(str).str.strip(),
            df_new['CUSTOMERID']
        ))
        
        # Create temporary columns to hold the mapping results
        df_new['TEMP_READING'] = df_new['METERNUMBER'].astype(str).str.strip().map(device_to_reading)
        df_new['TEMP_READDATE'] = df_new['METERNUMBER'].astype(str).str.strip().map(device_to_date)
        
        # Now fill in LASTREADING and LASTREADDATE properly
        # Initialize the columns
        df_new['LASTREADING'] = None
        df_new['LASTREADDATE'] = None
        
        # Group by CUSTOMERID to handle cases where a customer has multiple meters
        for customerid, group in df_new.groupby('CUSTOMERID'):
            # Find the most recent date for this customer (if any)
            valid_dates = group['TEMP_READDATE'].dropna()
            
            if not valid_dates.empty:
                max_date = max(valid_dates)
                # Find the index of the row with this max date
                max_date_idx = group.loc[group['TEMP_READDATE'] == max_date].index[0]
                # Get the corresponding reading
                max_reading = group.loc[max_date_idx, 'TEMP_READING']
                
                # Assign to all rows for this customer
                df_new.loc[df_new['CUSTOMERID'] == customerid, 'LASTREADING'] = max_reading
                df_new.loc[df_new['CUSTOMERID'] == customerid, 'LASTREADDATE'] = max_date
        
        # Drop temporary columns
        df_new = df_new.drop(['TEMP_READING', 'TEMP_READDATE'], axis=1)
        
        # Print match statistics for debugging
        reading_matches = sum(~df_new['LASTREADING'].isna())
        date_matches = sum(~df_new['LASTREADDATE'].isna())
        
        print(f"Mapped LASTREADING for {reading_matches} out of {len(df_new)} rows")
        print(f"Mapped LASTREADDATE for {date_matches} out of {len(df_new)} rows")
        
    except Exception as e:
        print(f"Error processing EABL data: {e}")
        # Ensure these columns exist even if processing fails
        if 'LASTREADING' not in df_new.columns:
            df_new['LASTREADING'] = ""
        if 'LASTREADDATE' not in df_new.columns:
            df_new['LASTREADDATE'] = ""
 
# --- Assign MULTIPLIER based on METERNUMBER matched to METERMULTIPLIER_PressureFactor.xlsx ---
if data_sources.get("MM") is not None and "Meter #1" in data_sources["MM"].columns and "PressureFactor" in data_sources["MM"].columns:
    mm_df = data_sources["MM"]
 
    # Standardize for match
    mm_df["Meter #1"] = mm_df["Meter #1"].astype(str).str.strip()
    df_new["METERNUMBER"] = df_new["METERNUMBER"].astype(str).str.strip()
 
    # Build dictionary from Meter #1 → PressureFactor
    meter_to_multiplier = dict(zip(mm_df["Meter #1"], mm_df["PressureFactor"]))
 
    # Map MULTIPLIER into df_new
    df_new["MULTIPLIER"] = df_new["METERNUMBER"].map(meter_to_multiplier)
else:
    print("⚠️ Warning: 'MM' file missing 'Meter #1' or 'PressureFactor' columns.")

# Create a new field SERVICESTATUS based on CUSTOMERID and METERNUMBER values
if data_sources["ZDM_PREMDETAILS"] is not None:
    # Get CUSTOMERID and METERNUMBER values
    customer_ids = data_sources["ZDM_PREMDETAILS"].iloc[:, 7].fillna('')
    meter_numbers = data_sources["ZDM_PREMDETAILS"].iloc[:, 18].fillna('')
    
    # Apply the logic
    df_new["SERVICESTATUS"] = [
        "0" if (customer_ids[i] != '' and meter_numbers[i] != '') else
        "1" if (customer_ids[i] == '' and meter_numbers[i] != '') else
        "2"  # Default case: no CUSTOMERID and no METERNUMBER
        for i in range(len(df_new))
    ]
    
    print(f"SERVICESTATUS assigned: '0': {sum(df_new['SERVICESTATUS'] == '0')}, '1': {sum(df_new['SERVICESTATUS'] == '1')}, '2': {sum(df_new['SERVICESTATUS'] == '2')}")

 
# Assign hardcoded values
df_new["APPLICATION"] = "5"
df_new["SERVICENUMBER"] = "1"
df_new["SERVICETYPE"] = "0"
df_new["METERREGISTER"] = "1"
df_new["LATITUDE"] = ""
df_new["READSEQUENCE"] = "0" # NEED UPDATED MAPPING
df_new["LONGITUDE"] = ""
df_new["HHCOMMENTS"] = ""
df_new["SERVICECOMMENTS"] = ""
df_new["USERDEFINED"] = ""
df_new["STOPESTIMATE"] = ""
df_new["LOCATIONCODE"] = ""
df_new["INSTRUCTIONCODE"] = ""
df_new["TAMPERCODE"] = ""
df_new["AWCVALUE"] = ""
df_new["UPDATEDATE"] = ""
df_new["REMOVEDDATE"] = "" # NEED UPDATED MAPPING
 
# Extract INITIALSERVICEDATE and BILLINGSTARTDATE from ZNC_ACTIVE_CUS
# if data_sources["ZDM_PREMDETAILS"] is not None:
#    df_new["REMOVEDDATE"] = pd.to_datetime(data_sources["ZDM_PREMDETAILS"].iloc[:, 7], errors='coerce').dt.strftime('%Y-%m-%d')
 
 
# Function to wrap values in double quotes, but leave blanks and NaN as they are
def custom_quote(val):
    """Wraps all values in quotes except for blank or NaN ones."""
    if pd.isna(val) or val == "" or val == " ":
        return ''  # Return an empty string for NaN or blank fields
    return f'"{val}"'  # Wrap other values in double quotes
 
# Apply custom_quote function to all columns
df_new = df_new.fillna('')
 
# Apply selective quoting
def selective_custom_quote(val, column_name):
    if column_name in ['APPLICATION', 'SERVICENUMBER', 'SERVICETYPE', 'METERREGISTER', 'SERVICESTATUS', 'BILLINGRATE1', 'SALESCLASS1', 'BILLINGRATE2', 'SALESCLASS2', 'READSEQUENCE', 'LASTREADING','MULTIPLIER']:
        return val  # Keep numeric values unquoted
    return '' if val in [None, 'nan', 'NaN', 'NAN'] else custom_quote(val)
 
 
df_new = df_new.apply(lambda col: col.map(lambda x: selective_custom_quote(x, col.name)))
 
 
 
 
# Drop duplicate records based on LOCATIONID, APPLICATION, and SERVICENUMBER
df_new = df_new.drop_duplicates(subset=['LOCATIONID', 'APPLICATION','SERVICENUMBER'], keep='first')
 
 
# Reorder columns based on user preference
column_order = [
    "CUSTOMERID", "LOCATIONID", "APPLICATION", "SERVICENUMBER", "SERVICETYPE",
    "METERNUMBER", "METERREGISTER", "SERVICESTATUS", "INITIALSERVICEDATE",
    "BILLINGSTARTDATE", "BILLINGRATE1", "SALESCLASS1", "BILLINGRATE2",
    "SALESCLASS2", "READSEQUENCE", "LASTREADING", "LASTREADDATE", "MULTIPLIER",
    "LATITUDE", "LONGITUDE", "HHCOMMENTS", "SERVICECOMMENTS", "USERDEFINED",
    "STOPESTIMATE", "LOCATIONCODE", "INSTRUCTIONCODE", "TAMPERCODE", "AWCVALUE",
    "UPDATEDATE", "REMOVEDDATE"
]
 
df_new = df_new[column_order]
 
 
# Add a trailer row with default values
trailer_row = pd.DataFrame([["TRAILER"] + [''] * (len(df_new.columns) - 1)], columns=df_new.columns)
df_new = pd.concat([df_new, trailer_row], ignore_index=True)
 
 
# Define output path for the CSV file
output_path = os.path.join(os.path.dirname(list(file_paths.values())[0]), 'STAGE_METERED_SVCS_(POST CONV 1 Updated).csv')
 
# Save to CSV with proper quoting and escape character
df_new.to_csv(output_path, index=False, header=True, quoting=csv.QUOTE_NONE, escapechar='\\')
 
# Confirmation message
print(f"CSV file saved at {output_path}")