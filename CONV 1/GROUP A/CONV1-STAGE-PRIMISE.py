import pandas as pd
import re
import csv

# Load the data
file_path = r"C:\Users\US97684\Downloads\documents_20250317_New\Outbound\ZDM_PREMDETAILS.XLSX"
file_path2 = r"C:\Users\US97684\Downloads\documents_20250317_New\Outbound\TE422.XLSX"
file_path1 = r"C:\Users\US97684\Downloads\documents_20250317_New\Premise_clean_final.xlsx"
df = pd.read_excel(file_path, sheet_name='Sheet1', engine='openpyxl')
df_Portion = pd.read_excel(file_path2, sheet_name='Sheet1', engine='openpyxl')
df_Premise = pd.read_excel(file_path1, sheet_name='Clean_Data', engine='openpyxl')

# Load configuration file for suffix lookup
config_path = r"C:\Users\US97684\Downloads\documents_20250317_New\Configuration.xlsx"
sheet1 = pd.read_excel(config_path, sheet_name='Street Abbreviation', engine='openpyxl')
sheet2 = pd.read_excel(config_path, sheet_name='Premise Designation', engine='openpyxl')

# Initialize df_new as an empty DataFrame
df_new = pd.DataFrame()

# Column 1: Column B (index 1)
df_new['LOCATIONID'] = df.iloc[:, 2]

# Apply the function to extract street number from the relevant column (iloc(26))
def fetch_streetnumber(location_id):
    location_id = str(location_id).strip()  # Convert LOCATIONID to string and remove extra spaces
    premise_clean = df_Premise.iloc[:, 0].astype(str).str.strip()  # Clean whitespace and convert to string  
    matched_row = df_Premise[premise_clean.str.contains(location_id)]
    
    if not matched_row.empty:
        return str(matched_row.iloc[0, 3]).strip()  # Return street number from the second column
    else:
        return None

df_new['STREETNUMBER'] = df_new['LOCATIONID'].apply(fetch_streetnumber)

# Function to extract STREETNUMBER and move suffix to STREETNUMBERSUFFIX
def move_suffix_to_streetnumbersuffix(streetnumber):
    if streetnumber:
        streetnumber = str(streetnumber).strip()  # Ensure it's a string and remove any extra spaces

        # Match numbers followed by any non-numeric characters (letters, symbols, fractions, etc.)
        match = re.match(r'(\d+)([^\d].*)', streetnumber)  # (\d+) captures the number, [^\d].* captures the suffix
        
        if match:
            # If match is found, separate street number and suffix
            number_part = match.group(1)
            suffix_part = match.group(2).strip()  # Trim the suffix part
            return number_part, suffix_part  # Return the numeric part and the suffix
        else:
            return streetnumber, ""  # If no suffix found, return the number and an empty string
    return "", ""  # If streetnumber is empty, return empty values

# Apply the function to split STREETNUMBER and assign suffix to STREETNUMBERSUFFIX
df_new[['STREETNUMBER', 'STREETNUMBERSUFFIX']] = df_new['STREETNUMBER'].apply(
    lambda x: pd.Series(move_suffix_to_streetnumbersuffix(x))
)

df_new['STREETNAME'] = 8
# Pre-direction mapping dictionary
pre_direction_map = {
    'N': 'N',
    'S': 'S',
    'E': 'E',
    'W': 'W',
    'NE': 'NE',
    'SE': 'SE',
    'SW': 'SW',
    'NORTH': 'N',
    'SOUTH': 'S',
    'EAST': 'E',
    'WEST': 'W',
    'NORTHEAST': 'NE'
}

# Function to fetch pre-direction from df_Premise based on column E
def fetch_predirection(location_id):
    location_id = str(location_id).strip()  # Convert LOCATIONID to string and remove extra spaces
    premise_clean = df_Premise.iloc[:, 4].astype(str).str.strip()  # Clean whitespace in column E (index 4)
    
    # Match based on the pre-direction mapping
    matched_row = df_Premise[premise_clean.str.contains(location_id, case=False, na=False)]
    
    if not matched_row.empty:
        # Check the pre-direction and map it accordingly using the pre_direction_map
        predirection_value = str(matched_row.iloc[0, 4]).strip()  # Assuming pre-direction is in column E (index 4)
        
        # Map to the correct pre-direction abbreviation
        return pre_direction_map.get(predirection_value.upper(), "")
    else:
        return ""  # Return empty string if no match is found

# Apply the fetch_predirection function to the LOCATIONID column
df_new['PREDIRECTION'] = df_new['LOCATIONID'].apply(fetch_predirection)

# Function to fetch abbreviation based on location_id
def fetch_Abbr(location_id):
    location_id = str(location_id).strip()
    premise_clean = df_Premise.iloc[:, 0].astype(str).str.strip()  # Ensure we're looking at the correct column for location_id

    # Match the LOCATIONID in the premise data
    matched_row = df_Premise[premise_clean.str.contains(location_id, case=False, na=False)]
    
    if not matched_row.empty:
        street_suffix = str(matched_row.iloc[0, 6]).strip()  # Assuming street suffix is in column G (index 6) of df_Premise
        
        # Now look for a match for the street suffix in the configuration sheet (sheet1)
        matching_row = sheet1[sheet1['Abbreviation'].str.strip().str.upper() == street_suffix.upper()]
        
        if not matching_row.empty:
            return matching_row.iloc[0, 1]  # Assuming the abbreviation is in the first column of sheet1
        else:
            return ""  # Return an empty string if no match is found in the configuration sheet
    return ""  # Return empty string if no match is found in the premise data

df_new['ABBRIVATION'] = df_new['LOCATIONID'].apply(fetch_Abbr)

# Function to fetch street name based on location_id
def fetch_streetname(location_id):
    location_id = str(location_id).strip()  # Convert LOCATIONID to string and remove extra spaces
    premise_clean = df_Premise.iloc[:, 0].astype(str).str.strip()  # Clean whitespace and convert to string  

    # Check if location_id is found in premise_clean using case-insensitive matching
    matched_row = df_Premise[premise_clean.str.contains(location_id, case=False, na=False)]  # Added case=False and na=False
    
    if not matched_row.empty:
        street_name = str(matched_row.iloc[0, 5]).strip()  # Assuming street name is in the 6th column
        return street_name if street_name != 'nan' else ""  # Return empty string if nan is found
    else:
        return ""  # Return empty string if no match is found

df_new['STRNAME'] = df_new['LOCATIONID'].apply(fetch_streetname)

# Pre-direction mapping dictionary
post_direction_map = {
    'N': 'N',
    'S': 'S',
    'E': 'E',
    'W': 'W',
    'NE': 'NE',
    'SE': 'SE',
    'SW': 'SW',
    'NORTH': 'N',
    'SOUTH': 'S',
    'EAST': 'E',
    'WEST': 'W',
    'NORTHEAST': 'NE'
}

# Function to fetch post-direction from df_Premise based on column F
def fetch_postdirection(location_id):
    location_id = str(location_id).strip()  # Convert LOCATIONID to string and remove extra spaces
    premise_clean = df_Premise.iloc[:, 6].astype(str).str.strip()  # Clean whitespace in column F (index 6)
    
    # Match based on the post-direction mapping
    matched_row = df_Premise[premise_clean.str.contains(location_id, case=False, na=False)]
    
    if not matched_row.empty:
        postdirection_value = str(matched_row.iloc[0, 6]).strip()  # Assuming post-direction is in column F (index 6)
        return post_direction_map.get(postdirection_value.upper(), "")
    else:
        return ""  # Return empty string if no match is found

df_new['POSTDIRc'] = df_new['LOCATIONID'].apply(fetch_postdirection)

# Concatenate PREDIRECTION and STRNAME into STREETNAME
df_new['STREETNAME'] = df_new['PREDIRECTION'] + " " + df_new['STRNAME'] + " " + df_new['ABBRIVATION'] + " "+ df_new['POSTDIRc'] 

# Function to fetch designation
def fetch_designation(location_id):
    location_id = str(location_id).strip()  # Convert LOCATIONID to string and remove extra spaces
    premise_clean = df_Premise.iloc[:, 0].astype(str).str.strip()  # Clean whitespace and convert to string  
    matched_row = df_Premise[premise_clean.str.contains(location_id)]
    
    if not matched_row.empty:
        designation = str(matched_row.iloc[0, 8]).strip()
        designation = designation.replace("-", "").replace(".", "")
        return designation
    else:
        return None

df_new['DESIGNATION'] = df_new['LOCATIONID'].apply(fetch_designation)

df_new['ADDITIONALDESC'] = ""  # Initially leave ADDITIONALDESC empty

# Town
def fetch_town(location_id):
    location_id = str(location_id).strip()  # Convert LOCATIONID to string and remove extra spaces
    premise_clean = df_Premise.iloc[:, 0].astype(str).str.strip()  # Clean whitespace and convert to string  
    matched_row = df_Premise[premise_clean.str.contains(location_id)]
    
    if not matched_row.empty:
        return str(matched_row.iloc[0, 2]).strip()  # Return street name from the second column
    else:
        return None

df_new['TOWN'] = df_new['LOCATIONID'].apply(fetch_town)

df_new['STATE'] = "ME"

# Ensure ZIPCODE is always treated as a string with leading zeros
df_new['ZIPCODE'] = df.iloc[:, 27].astype(str).str.zfill(5)  # Ensure it's 5 digits, filling leading zeros if necessary

# Handle ZIPPLUSFOUR and invalid ZIPCODE entries
ZIPCODE = pd.to_numeric(df_new['ZIPCODE'], errors='coerce')  # Convert ZIPCODE to numeric, errors coerced to NaN

# Ensure proper handling of NaN or invalid ZIPCODE values when adding '4'
df_new['ZIPPLUSFOUR'] = ZIPCODE.apply(
    lambda x: str(int(x) + 4) if pd.notna(x) and x != 0 else '00000'  # Handle NaN or zero cases
)

df_new['OWNERCUSTOMER'] = df.iloc[:, 1]  # Assuming column 1 is CUSTOMER field

# Map the PROPERTYCLASS value from some condition or another function
def map_property_class(value):
    if value == "Residential":
        return "R"
    elif value == "Commercial":
        return "C"
    return "O"

df_new['PROPERTYCLASS'] = df.iloc[:, 4].apply(map_property_class)

# Add final constant fields
df_new['TAXDISTRICT'] = 8
df_new['SERVICEAREA'] = "80"
df_new['SERVICEFACILITY'] = ""
df_new['PRESSUREDISTRICT'] = ""

# Add trailer row for the file structure
trailer_row = pd.DataFrame([['TRAILER'] + [''] * (len(df_new.columns) - 1)], columns=df_new.columns)
df_new = pd.concat([df_new, trailer_row], ignore_index=True)

# Output the final dataframe to CSV
output_path = r"C:\Users\US97684\Downloads\documents_20250317\Outbound\STAGE_PREMISEUniq1.csv"

numeric_columns = [
    'STREETNUMBER', 'OWNERMAILSEQ', 'PROPERTYCLASS', 'TAXDISTRICT', 'BILLINGCYCLE', 'READINGROUTE',
    'SERVICEAREA', 'SERVICEFACILITY', 'PRESSUREDISTRICT', 'LATITUDE', 'LONGITUDE', 'PARCELAREATYPE',
    'PARCELAREA', 'IMPERVIOUSSQUAREFEET', 'PROPERTYUSECLASSIFICATION1', 'PROPERTYUSECLASSIFICATION2', 'AMPS', 'VOLTS', 'FLEXFIELD1', 'FLEXFIELD2',
    'PROPERTYUSECLASSIFICATION3', 'PROPERTYUSECLASSIFICATION4', 'PROPERTYUSECLASSIFICATION5'
]

def custom_quote(val, column):
    # Check if the column is in the list of numeric columns
    if column in numeric_columns:
        return val  # No quotes for numeric fields
    # Otherwise, add quotes for non-numeric fields
    return f'"{val}"' if val not in ["", None] else val

df_new = df_new.apply(lambda col: col.apply(lambda val: custom_quote(val, col.name)))

# Save to CSV with custom quoting and escape character
df_new.to_csv(output_path, index=False, quoting=csv.QUOTE_NONE, escapechar='\\')

print(f"File successfully saved to: {output_path}")

