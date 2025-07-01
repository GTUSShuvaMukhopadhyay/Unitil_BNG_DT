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
file_path1 = r"C:\Users\US82783\Downloads\ZINS.XLSX"
file_path2 = r"C:\Users\US82783\Downloads\documents_20250317\Device\BNG Gas Meter Attributes Cleanup Conv_2 05_20_25.xlsx"
config_path = r"C:\Users\US82783\Downloads\configuration.xlsx"

print("🔄 Loading input files...")

# Read the Excel file and load the specific sheets
bng_gas_meter_df = pd.read_excel(file_path2, sheet_name='ZINS Meter Table', engine='openpyxl')
zins_df = pd.read_excel(file_path1, sheet_name='Sheet1', engine='openpyxl')

print(f"Drop Zins records based on Status and Current Address columns")
zins_df = zins_df[
    (zins_df.iloc[:, 1].isna() | (zins_df.iloc[:, 1].astype(str).str.strip() == '')) &
    (zins_df.iloc[:, 6].isna() | (zins_df.iloc[:, 6].astype(str).str.strip() == ''))
]
print(f"ZINS and BNG Gas Meter files loaded. /nTotal records in ZINS: {len(zins_df)}")

# Initialize df_new with the same index as df
df_new = pd.DataFrame(index=zins_df.index)

# Load configuration sheets
DeviceMake = pd.read_excel(config_path, sheet_name='Device Make', engine='openpyxl')
DeviceSize = pd.read_excel(config_path, sheet_name='Device Size', engine='openpyxl')
DeviceRegisterType = pd.read_excel(config_path, sheet_name='Device RegisterType', engine='openpyxl')
DeviceModel = pd.read_excel(config_path, sheet_name='Device Model', engine='openpyxl')
print("Configuration files loaded.")

# Begin transformations
print("Starting data transformation...")

# DEVICECODE and METERNUMBER columns with truncation
df_new['METERNUMBER'] = zins_df.iloc[:, 0].astype(str).str.slice(0, 15)

#Defaulting it to "0", "501" and "501" for DEVICECODE, BUILTCONFIG and INSTALLCONFIG, we need get proper mapping details
df_new['DEVICECODE'] = "0"

# Truncating BUILTCONFIG and INSTALLCONFIG
df_new['BUILTCONFIG'] = "501"
df_new['INSTALLCONFIG'] = "501"

df_new['PONUMBER'] = ""

# Fetch PO Date with fuzzy matching and normalization
def fetch_podate(meter_number):
    meter_number = meter_number.strip() if isinstance(meter_number, str) else meter_number
    bng_gas_meter_clean = bng_gas_meter_df.iloc[:, 0].str.strip()
    matched_row = bng_gas_meter_df[bng_gas_meter_clean == meter_number]

    if not matched_row.empty:
        podate = matched_row.iloc[0, 1]
        parsed_date = pd.to_datetime(podate)
        return parsed_date.strftime('%Y%m%d')

print("Processing PODATE...")
df_new['PODATE'] = df_new['METERNUMBER'].apply(fetch_podate)
print("PODATE populated.")

df_new['PURCHASECOST'] = ""
df_new['SHAFTREDUCTION'] = ""
df_new['BIDIRECTIONALFLAG'] = "N"
df_new['AMIFLAG'] = "Y"
df_new['AMITYPE'] = "0"
df_new['ASSETTAXDISTRICT'] = "8"
df_new['COMMENTS'] = ""
df_new['COMMENTS2'] = ""

# Fetch PO Date with fuzzy matching and normalization
def fetch_serial(meter_number):
    meter_number = meter_number.strip() if isinstance(meter_number, str) else meter_number
    bng_gas_meter_clean = bng_gas_meter_df.iloc[:, 0].str.strip()
    matched_row = bng_gas_meter_df[bng_gas_meter_clean == meter_number]

    if not matched_row.empty:
        return str(matched_row.iloc[0, 3]).strip()

print("Processing PODATE...")
df_new['SERIALNUMBER'] = df_new['METERNUMBER'].apply(fetch_serial)
print("PODATE populated.")



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

print("Processing METERMAKE...")
df_new['METERMAKE'] = df_new['METERNUMBER'].apply(fetch_meter_make)
print("METERMAKE populated.")

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

print("Processing METERSIZE...")   
df_new['METERSIZE'] = df_new['METERNUMBER'].apply(fetch_makesize)
print("METERSIZE populated.")

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

print("Processing METERKIND...")
df_new['METERKIND'] = df_new['METERNUMBER'].apply(fetch_meterkind)
print("METERKIND populated.")

df_new['MATERIAL'] = "99"

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

print("Processing METERMODEL...")
df_new['METERMODEL'] = df_new['METERNUMBER'].apply(fetch_metermodel)
print("METERMODEL populated.")

df_new['REGISTER'] = "0"

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

print("Processing DIALS...")
df_new['DIALS'] = df_new['METERNUMBER'].apply(fetch_dials)
print("DIALS populated.")

df_new['DEADZEROES'] = "2"
df_new['MULTIPLIER'] = "1"

#Get Drive/FT value for undefined1
def fetch_userdefined1(meter_number):
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
            return str(size_match.iloc[0, 5]).strip()  # Get value from iloc[4] of DeviceSize
        else:
            return ''  # Return '99' if no match is found
    else:
        return ''  # Return '99' if no match is found in zins_df

print("Processing USERDEFINED1...")   
df_new['USERDEFINED1'] = df_new['METERNUMBER'].apply(fetch_userdefined1)
print("USERDEFINED1 populated.")

#Get Top values for undefined2
def fetch_userdefined2(meter_number):
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
            return str(size_match.iloc[0, 6]).strip()  # Get value from iloc[4] of DeviceSize
        else:
            return ''  # Return '99' if no match is found
    else:
        return ''  # Return '99' if no match is found in zins_df

print("Processing USERDEFINED2...")   
df_new['USERDEFINED2'] = df_new['METERNUMBER'].apply(fetch_userdefined2)
print("USERDEFINED2 populated.")

df_new["USERDEFINED3"] = "0"
df_new['READTYPE'] = "1"
df_new['OTHERDEVICEID1'] = ""
df_new['OTHERDEVICECODE1'] = ""
df_new['PURCHASE ORDER'] = ""
df_new['PURCHASE DATE'] = ""
df_new['SHAFT REDUCTION'] = ""
df_new['TAXDISTRICT'] = ""
df_new['COMMENTS1'] = ""
df_new['MAKE'] = ""
df_new['KIND'] = ""
df_new['MODEL'] = ""
df_new['METERREADING'] = ""
df_new['ENDPOINTREADING'] = ""
df_new['BEFORETEST1OPEN'] = ""
df_new['BEFORETEST1CHECK'] = ""
df_new['PO NUMBER'] = ""
df_new['DATE'] = ""
df_new['TESTER'] = ""

# Remove duplicates based on METERNUMBER
# Remove rows where any of the specified fields are blank (NaN or empty)
df_new = df_new[~df_new[['DEVICECODE', 'METERNUMBER', 'BUILTCONFIG', 'INSTALLCONFIG']].isin(['', None]).any(axis=1)]
print(f"Removed duplicates")
# Summary
print(f"All processing complete. Total records processed: {len(df_new)}")
df_new = df_new.drop_duplicates(subset=['METERNUMBER'])

# Add a trailer row with default values
trailer_row = pd.DataFrame([['TRAILER', ''] + [''] * (len(df_new.columns) - 2)],
                           columns=df_new.columns)

print(f"Added Trailer")

# Append the trailer row to the DataFrame
df_new = pd.concat([df_new, trailer_row], ignore_index=True)


# Save to CSV
output_path = r"C:\Users\US82783\OneDrive - Grant Thornton LLP\Desktop\python\conv 2\Meter\STAGE_METER_INVENTORY.csv"
df_new.to_csv(output_path, index=False)

print(f"CSV file saved at {output_path}")