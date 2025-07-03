import pandas as pd
import os
import re
import csv
from fuzzywuzzy import process  # Importing fuzzywuzzy for fuzzy string matching

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

# Function to normalize strings by removing all spaces and unnecessary characters
def normalize_string(value):
    if isinstance(value, str):
        value = value.strip()  # Remove leading and trailing spaces
        return re.sub(r'\s+', '', value).upper()  # Remove all white spaces and convert to upper case
    return value

# Fuzzy matching function to find the best match from a list of potential matches
def fuzzy_match(value, match_list, threshold=80):
    normalized_value = normalize_string(value)
    match = process.extractOne(normalized_value, match_list)
    if match and match[1] >= threshold:
        return match[0]
    return None  # Return None if no good match is found

# File paths
#file_path1 = r"C:\Users\US82783\Downloads\ZINS.XLSX"
file_path = r"C:\Users\US82783\OneDrive - Grant Thornton Advisors LLC\Desktop\python\CONV 2B _ 2nd run\DATA SOURCES\ZDM_PREMDETAILS.XLSX"
file_path1 = r"C:\Users\US82783\OneDrive - Grant Thornton Advisors LLC\Desktop\python\CONV 2B _ 2nd run\DATA SOURCES\BNG Gas Meter Attributes Cleanup Conv_2 05_20_25.xlsx"
config_path = r"C:\Users\US82783\OneDrive - Grant Thornton Advisors LLC\Desktop\python\CONV 2B _ 2nd run\DATA SOURCES\Configuration 13.xlsx"
file_path2 = r"C:\Users\US82783\OneDrive - Grant Thornton Advisors LLC\Desktop\python\CONV 2B _ 2nd run\DATA SOURCES\ZINS.XLSX"
print(f"Loading the files")

# Read the Excel file and load the specific sheets
bng_gas_meter_df = pd.read_excel(file_path1, sheet_name='ZINS Meter Table', engine='openpyxl')
df = pd.read_excel(file_path, sheet_name='Sheet1', engine='openpyxl')
zins_df = pd.read_excel(file_path2, sheet_name='Sheet1', engine='openpyxl')

# Initialize df_new with the same index as df
df_new = pd.DataFrame(index=df.index)

# Load configuration sheets
DeviceMake = pd.read_excel(config_path, sheet_name='Device Make', engine='openpyxl')
DeviceSize = pd.read_excel(config_path, sheet_name='Device Size', engine='openpyxl')
DeviceRegisterType = pd.read_excel(config_path, sheet_name='Device RegisterType', engine='openpyxl')
DeviceModel = pd.read_excel(config_path, sheet_name='Device Model', engine='openpyxl')

print(f"Files loaded")

df_new['APPLICATION'] = "5"

# DEVICECODE and METERNUMBER columns with truncation
df_new['DEVICECODE'] = df.apply(
    lambda row: str(0).zfill(1) if pd.notna(row.iloc[19]) and row.iloc[19] != ''
    else ('' if pd.notna(row.iloc[25]) and row.iloc[25] != '' else ''),
    axis=1
)

df_new['METERNUMBER'] = df.apply(
    lambda row: str(row.iloc[19]).strip()[:12] if pd.notna(row.iloc[19]) and row.iloc[19] != ''
    else (str(row.iloc[25]).strip()[:12] if pd.notna(row.iloc[25]) and row.iloc[25] != '' else ''),
    axis=1
)

df_new['REGISTERNUM'] = "1"

# Truncating BUILTCONFIG and INSTALLCONFIG
df_new['BUILTCONFIG'] = df.apply(
    lambda row: str(501) if pd.notna(row.iloc[19]) and row.iloc[19] != ''
    else (str(591) if pd.notna(row.iloc[25]) and row.iloc[25] != '' else '592'),
    axis=1
)

df_new['INSTALLCONFIG'] = df.apply(
    lambda row: str(501) if pd.notna(row.iloc[19]) and row.iloc[19] != ''
    else (str(591) if pd.notna(row.iloc[25]) and row.iloc[25] != '' else '592'),
    axis=1
)

df_new['BILLEDFLAG'] = "Y"
df_new['REGISTERCONFIG'] = "1"

# Fetching SERIALNUMBER and truncating during assignment
def fetch_zins_value(meter_number):
    meter_number = meter_number.strip() if isinstance(meter_number, str) else meter_number
    # Find the row in bng_gas_meter_df that matches METERNUMBER (iloc[0] column)
    bng_gas_meter_clean = bng_gas_meter_df.iloc[:, 0].str.strip()
    matched_row = bng_gas_meter_df[bng_gas_meter_clean == meter_number]
    
    if not matched_row.empty:
        # If a match is found, return the value from iloc[2]
        return str(matched_row.iloc[0, 3]).strip()
    else:
        return None

df_new['SERIALNUMBER'] = df_new['METERNUMBER'].apply(fetch_zins_value)
df_new['OTHERDEVICECODE1'] = ""
df_new['OTHERDEVICEID1'] = ""
df_new['OTHERDEVICEMARRY1'] = ""
df_new['OTHERDEVICECODE2'] = ""
df_new['OTHERDEVICEID2'] = ""
df_new['OTHERDEVICEMARRY2'] = ""

# Fetch METERMAKE with fuzzy matching and normalization
def fetch_meter_make(meter_number):
    meter_number = meter_number.strip() if isinstance(meter_number, str) else meter_number
    bng_gas_meter_clean = bng_gas_meter_df.iloc[:, 0].str.strip()
    matched_row = bng_gas_meter_df[bng_gas_meter_clean == meter_number]

    if not matched_row.empty:
        meter_make_value = str(matched_row.iloc[0, 2]).strip()  # Get full value from iloc[12]
        
        # Normalize meter_make_value
        normalized_meter_make_value = normalize_string(meter_make_value)
        
        # Lookup the normalized meter_make_value in the DeviceMake configuration file
        device_make_clean = DeviceMake.iloc[:, 0].str.strip().apply(normalize_string)
        
        # Apply fuzzy matching
        best_match = fuzzy_match(normalized_meter_make_value, device_make_clean)
        
        if best_match:
            # Get the corresponding value from DeviceMake for the best match
            make_match = DeviceMake[DeviceMake.iloc[:, 0].apply(normalize_string) == best_match]
            return str(make_match.iloc[0, 1]).strip()  # Get value from iloc[1] of DeviceMake
        else:
            return '99'  # Return '99' if no match is found
    else:
        return '99'  # Return '99' if no match is found in zins_df
df_new['METERMAKE'] = df_new['METERNUMBER'].apply(fetch_meter_make)

# Fetch MAKESIZE with fuzzy matching
def fetch_makesize(meter_number):
    meter_number = meter_number.strip() if isinstance(meter_number, str) else meter_number
    bng_gas_meter_clean = bng_gas_meter_df.iloc[:, 0].str.strip()
    matched_row = bng_gas_meter_df[bng_gas_meter_clean == meter_number]

    if not matched_row.empty:
        makesize_value = str(matched_row.iloc[0, 4]).strip()  # Get full value from iloc[14]
        
        # Normalize makesize_value
        normalized_makesize_value = normalize_string(makesize_value)
        
        # Lookup the normalized makesize_value in the DeviceSize configuration file
        device_size_clean = DeviceSize.iloc[:, 0].str.strip().apply(normalize_string)
        
        # Apply fuzzy matching
        best_match = fuzzy_match(normalized_makesize_value, device_size_clean)
        
        if best_match:
            # Get the corresponding value from DeviceSize for the best match
            size_match = DeviceSize[DeviceSize.iloc[:, 0].apply(normalize_string) == best_match]
            return str(size_match.iloc[0, 1]).strip()  # Get value from iloc[1] of DeviceSize
        else:
            return '99'  # Return '99' if no match is found
    else:
        return '99'  # Return '99' if no match is found in zins_df
    
df_new['METERSIZE'] = df_new['METERNUMBER'].apply(fetch_makesize)

def fetch_meterkind(meter_number):
    meter_number = meter_number.strip() if isinstance(meter_number, str) else meter_number
    bng_gas_meter_clean = bng_gas_meter_df.iloc[:, 0].str.strip()
    matched_row = bng_gas_meter_df[bng_gas_meter_clean == meter_number]

    if not matched_row.empty:
        makesize_value = str(matched_row.iloc[0, 5]).strip()  # Get full value from iloc[14]
        
        # Normalize makesize_value
        normalized_makesize_value = normalize_string(makesize_value)
        
        # Lookup the normalized makesize_value in the DeviceSize configuration file
        register_type_clean = DeviceRegisterType.iloc[:, 0].str.strip().apply(normalize_string)
        
        # Apply fuzzy matching
        best_match = fuzzy_match(normalized_makesize_value, register_type_clean)
        
        if best_match:
            # Get the corresponding value from DeviceSize for the best match
            size_match = DeviceRegisterType[DeviceRegisterType.iloc[:, 0].apply(normalize_string) == best_match]
            return str(size_match.iloc[0, 1]).strip()  # Get value from iloc[1] of DeviceSize
        else:
            return '99'  # Return '99' if no match is found
    else:
        return '99'  # Return '99' if no match is found in zins_df

df_new['METERKIND'] = df_new['METERNUMBER'].apply(fetch_meterkind)

# Fetch METERMODEL with fuzzy matching
def fetch_metermodel(meter_number):
    meter_number = meter_number.strip() if isinstance(meter_number, str) else meter_number
    bng_gas_meter_clean = bng_gas_meter_df.iloc[:, 0].str.strip()
    matched_row = bng_gas_meter_df[bng_gas_meter_clean == meter_number]

    if not matched_row.empty:
        # Get value from iloc[14] of ZINS
        device_size_value = str(matched_row.iloc[0, 4]).strip()  
        
        # Normalize device_size_value
        normalized_device_size_value = normalize_string(device_size_value)
        
        # Lookup the normalized device_size_value in the DeviceSize configuration file
        device_size_clean = DeviceSize.iloc[:, 0].str.strip().apply(normalize_string)
        
        # Apply fuzzy matching
        best_match = fuzzy_match(normalized_device_size_value, device_size_clean)
        
        if best_match:
            # Get the corresponding value from DeviceSize for the best match, iloc[4]
            device_size_match = DeviceSize[DeviceSize.iloc[:, 0].apply(normalize_string) == best_match]
            model_from_device_size = str(device_size_match.iloc[0, 4]).strip()  # Fetch iloc[4] from DeviceSize
            
            # Now look up the value in DeviceModel for the best match in DeviceModel
            # Filter out 'NULL' and other empty or invalid values in DeviceModel
            valid_device_model = DeviceModel[DeviceModel.iloc[:, 0].str.strip() != 'NULL']
            valid_device_model_clean = valid_device_model.iloc[:, 0].str.strip().apply(normalize_string)
            
            # Perform fuzzy matching on valid data
            best_match_model = fuzzy_match(model_from_device_size, valid_device_model_clean)
            
            if best_match_model:
                # Get value from DeviceModel corresponding to the best match, iloc[1]
                model_match = valid_device_model[valid_device_model.iloc[:, 0].apply(normalize_string) == best_match_model]
                
                # Check if model_match is not empty before accessing iloc
                if not model_match.empty:
                    return str(model_match.iloc[0, 1]).strip()  # Get value from iloc[1] of DeviceModel
                else:
                    return '99'  # Return '99' if no match is found in DeviceModel
            else:
                return '99'  # Return '99' if no match is found in DeviceSize
        else:
            return '99'  # Return '99' if no match is found in DeviceSize
    else:
        return '99'  # Return '99' if no match is found in zins_df

df_new['METERMODEL'] = df_new['METERNUMBER'].apply(fetch_metermodel)

def fetch_dials(meter_number):
    meter_number = meter_number.strip() if isinstance(meter_number, str) else meter_number
    # Find the row in bng_gas_meter_df that matches METERNUMBER (iloc[0] column)
    bng_gas_meter_clean = bng_gas_meter_df.iloc[:, 0].str.strip()
    matched_row = bng_gas_meter_df[bng_gas_meter_clean == meter_number]
    
    if not matched_row.empty:
        # If a match is found, return the value from iloc[2]
        return str(matched_row.iloc[0, 6]).strip()
    else:
        return '99'  # Return '99' if no match is found in bng_gas_meter_df
df_new['DIALS'] = df_new['METERNUMBER'].apply(fetch_dials)

df_new['DEADZEROES'] = "2"
df_new['READTYPE'] = "1"
df_new['TESTCIRCLE'] = ""
df_new['AMPS'] = ""
df_new['VOLTS'] = ""
df_new['FLEXFIELD1'] = ""
df_new['FLEXFIELD2'] = ""
df_new['FLEXFIELD3'] = ""

# Create mappings from zins_df
zins_initialDate = dict(zip(zins_df.iloc[:, 0], zins_df.iloc[:, 2]))  # SERIALNUMBER → YEAR
zins_lastDate = dict(zip(zins_df.iloc[:, 0], zins_df.iloc[:, 4]))     # SERIALNUMBER → CURRENTINSTALLDATE

def format_date(date_val):
    try:
        return pd.to_datetime(date_val).strftime("%Y-%m-%d")
    except:
        return None
    
# Function to get INITIALINSTALLDATE: if year is 1900, fallback to CURRENTINSTALLDATE
def get_initial_install_date(meter_number):
    year = zins_initialDate.get(meter_number)
    current_date = zins_lastDate.get(meter_number)
    try:
        year_int = int(year)
        if year_int == 1900:
            return format_date(current_date) # fallback to CURRENTINSTALLDATE
        else:
            return f"{year_int}-01-01"
    except:
        return format_date(current_date)  # fallback in case of error

# Apply logic for INITIALINSTALLDATE
df_new["INITIALINSTALLDATE"] = df_new["METERNUMBER"].apply(get_initial_install_date)

# Now assign CURRENTINSTALLDATE from mapping (no changes)
df_new["CURRENTINSTALLDATE"] = df_new["METERNUMBER"].map(zins_lastDate).apply(format_date)

df_new['MULTIPLIER'] = "1"
df_new['CTNUMBER'] = ""
df_new["VTNUMBER"] = ""
df_new['PONUMBER'] = ""

def fetch_poDate(meter_number):
    meter_number = meter_number.strip() if isinstance(meter_number, str) else meter_number
    # Find the row in bng_gas_meter_df that matches METERNUMBER (iloc[0] column)
    bng_gas_meter_clean = bng_gas_meter_df.iloc[:, 0].str.strip()
    matched_row = bng_gas_meter_df[bng_gas_meter_clean == meter_number]
    
    if not matched_row.empty:
        # If a match is found, return the value from iloc[2]
        po_date = pd.to_datetime(matched_row.iloc[0, 1])
        formatted_date = po_date.strftime('%Y-%m-%d')  # Format as 'YYYY-MM-DD'
        return formatted_date
    else:
        return None

df_new['PODATE'] = df_new['METERNUMBER'].apply(fetch_poDate)
df_new['PURCHASECOST'] = ""
df_new['RETIREDATE'] = ""
df_new['ASSETTAXDISTRICT'] = "8"
df_new['BIDIRECTIONALFLAG'] = "N"
df_new['PRIVATELYOWNED'] = "N"
df_new['COMMENTS'] = ""
df_new['BATTERYDATE'] = ""
df_new['AMIFLAG'] = "Y"
df_new['AMITYPE'] = "0"
df_new['IPADDRESS'] = ""
df_new['PROBEMETERID'] = ""
df_new['PROBEMETERPASSWORD'] = "0"
df_new["PROBEMETERNAME"] = ""
df_new["UPDATEDATE"] = ""

# Remove rows where any of the specified fields are blank (NaN or empty)
df_new = df_new[~df_new[['APPLICATION','DEVICECODE', 'METERNUMBER', 'REGISTERNUM']].isin(['', None]).any(axis=1)]
print(f"Removed duplicates")

df_new = df_new.drop_duplicates(subset=['METERNUMBER'])

# Add a trailer row with default values
trailer_row = pd.DataFrame([['TRAILER', ''] + [''] * (len(df_new.columns) - 2)],
                           columns=df_new.columns)

print(f"Added Trailer")

# Append the trailer row to the DataFrame
df_new = pd.concat([df_new, trailer_row], ignore_index=True)


# Save to CSV
output_path = r"C:\Users\US82783\OneDrive - Grant Thornton Advisors LLC\Desktop\python\CONV 2B _ 2nd run\Group A\STAGE_DEVICE.csv"

# List of columns that are numeric and should not have quotes
numeric_columns = [
    'APPLICATION','REGISTERNUM','REGISTERCONFIG','TESTCIRCLE','AMPS','VOLT','FLEXFIELD1',
    'FLEXFIELD2','FLEXFIELD3','DEVICECODE', 'BUILTCONFIG', 'INSTALLCONFIG', 'OTHERDEVICETYPE1', 
    'OTHERDEVICETYPE2', 'METERMAKE', 'METERSIZE', 'METERKIND', 'METERMODEL','DIALS', 
    'DEADZEROES', 'READTYPE', 'MULTIPLIER', 'PONUMBER', 'ASSETTAXDISTRICT', 'AMITYPE'
]

def custom_quote(val, column):
    # Check if the column is in the list of numeric columns
    if column in numeric_columns:
        return val  # No quotes for numeric fields
    # Otherwise, add quotes for non-numeric fields
    return f'"{val}"' if val not in ["", None] else val

df_new = df_new.apply(lambda col: col.apply(lambda val: custom_quote(val, col.name)))
df_new.to_csv(output_path, index=False, header=True, quoting=csv.QUOTE_NONE)

print(f"CSV file saved at {output_path}")