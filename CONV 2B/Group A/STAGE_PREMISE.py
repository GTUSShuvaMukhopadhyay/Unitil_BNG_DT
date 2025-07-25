import pandas as pd
import re
import csv
 
file_path = r"C:\Users\US82783\OneDrive - Grant Thornton Advisors LLC\Desktop\python\CONV 2B _ 2nd run\DATA SOURCES\ZDM_PREMDETAILS.XLSX"
file_path1 = r"C:\Users\US82783\OneDrive - Grant Thornton Advisors LLC\Desktop\python\CONV 2B _ 2nd run\DATA SOURCES\Premise_clean_final_2B.xlsx"
df = pd.read_excel(file_path, sheet_name='Sheet1', engine='openpyxl')
df = df[~df.iloc[:, 4].astype(str).str.startswith("G_ME_")]
df_Premise = pd.read_excel(file_path1, sheet_name='Clean_Data', engine='openpyxl')
 
# Load configuration file for suffix lookup
config_path = r"C:\Users\US82783\OneDrive - Grant Thornton Advisors LLC\Desktop\python\CONV 2B _ 2nd run\DATA SOURCES\Configuration 13.xlsx"
sheet1 = pd.read_excel(config_path, sheet_name='Street Abbreviation', engine='openpyxl')
#sheet2 = pd.read_excel(config_path, sheet_name='Premise Designation', engine='openpyxl')
 
# Initialize df_new as an empty DataFrame
df_new = pd.DataFrame()
 
# Column 1: Column B (index 1)
df_new['LOCATIONID'] = df.iloc[:, 2]
 
# Function to fetch street number
def fetch_streetnumber(location_id):
    location_id = str(location_id).strip()
    premise_clean = df_Premise.iloc[:, 0].astype(str).str.strip()
    matched_row = df_Premise[premise_clean.str.contains(location_id, na=False)]
    if not matched_row.empty:
        return str(matched_row.iloc[0, 3]).strip()
    return ""
 
df_new['STREETNUMBER'] = df_new['LOCATIONID'].apply(fetch_streetnumber)
 
# Move suffix to separate column
def move_suffix_to_streetnumbersuffix(streetnumber):
    if streetnumber:
        streetnumber = str(streetnumber).strip()
        match = re.match(r'(\d+)([^\d].*)', streetnumber)
        if match:
            return match.group(1), match.group(2).strip()
        else:
            return streetnumber, ""
    return "", ""
 
df_new[['STREETNUMBER', 'STREETNUMBERSUFFIX']] = df_new['STREETNUMBER'].apply(
    lambda x: pd.Series(move_suffix_to_streetnumbersuffix(x))
)
 
# Direction mapping
direction_map = {
    'N': 'N', 'S': 'S', 'E': 'E', 'W': 'W',
    'NE': 'NE', 'SE': 'SE', 'SW': 'SW', 'NW': 'NW',
    'NORTH': 'N', 'SOUTH': 'S', 'EAST': 'E', 'WEST': 'W',
    'NORTHEAST': 'NE', 'SOUTHEAST': 'SE', 'SOUTHWEST': 'SW', 'NORTHWEST': 'NW'
}
 
# Load Street Abbreviation config
street_abbreviation_df = pd.read_excel(config_path, sheet_name='Street Abbreviation', engine='openpyxl')
 
def fetch_streetname(location_id):
    location_id = str(location_id).strip()
    premise_clean = df_Premise.iloc[:, 0].astype(str).str.strip()
    matched_row = df_Premise[premise_clean.str.contains(location_id, case=False, na=False)]
    if not matched_row.empty:
        parts = [
            str(matched_row.iloc[0, 4]).strip(),
            str(matched_row.iloc[0, 5]).strip(),
            str(matched_row.iloc[0, 6]).strip(),
            str(matched_row.iloc[0, 7]).strip()
        ]
        for i in [0, 3]:
            parts[i] = direction_map.get(parts[i].upper(), "")
        if parts[2]:
            abbr_match = street_abbreviation_df[street_abbreviation_df.iloc[:, 0] == parts[2]]
            if not abbr_match.empty:
                parts[2] = abbr_match.iloc[0, 1]
            else:
                parts[2] = ""
        return " ".join(part for part in parts if part)
    return ""
 
df_new['STREETNAME'] = df_new['LOCATIONID'].apply(fetch_streetname)
 
def fetch_designation(location_id):
    location_id = str(location_id).strip()
    premise_clean = df_Premise.iloc[:, 0].astype(str).str.strip()
    matched_row = df_Premise[premise_clean.str.contains(location_id, na=False)]
    if not matched_row.empty:
        designation = str(matched_row.iloc[0, 8]).strip()
        return designation.replace("-", "").replace(".", "")
    return ""
 
df_new['DESIGNATION'] = df_new['LOCATIONID'].apply(fetch_designation)
df_new['ADDITIONALDESC'] = ""
 
# Town
def fetch_town(location_id):
    location_id = str(location_id).strip()
    premise_clean = df_Premise.iloc[:, 0].astype(str).str.strip()
    matched_row = df_Premise[premise_clean.str.contains(location_id, na=False)]
    if not matched_row.empty:
        return str(matched_row.iloc[0, 2]).strip()
    return ""
 
df_new['TOWN'] = df_new['LOCATIONID'].apply(fetch_town).str.upper()
 
# Fixed fields
df_new['STATE'] = "ME"
df_new['ZIPCODE'] = df.iloc[:, 27].astype(str).str.zfill(5)
 
ZIPCODE = pd.to_numeric(df_new['ZIPCODE'], errors='coerce')
df_new['ZIPPLUSFOUR'] = ZIPCODE.apply(lambda x: str(int(x) + 4) if pd.notna(x) and x != 0 else '00000')
 
df_new['OWNERCUSTOMERID'] = ""
df_new['OWNERMAILSEQ'] = "1"
 
# Property Class Mapping
def map_property_class(value):
    mapping = {
        'G_ME_RESID': 1, 'T_ME_RESID': 1,
        'G_ME_SCISL': 2, 'T_ME_SCISL': 2,
        'T_ME_LIHEA': 1, 'G_ME_LCISL': 2,
        'T_ME_LCISL': 2, 'T_ME_LCITR': 2, 'T_ME_SCITR': 2
    }
    return mapping.get(value, 1)
 
df_new['PROPERTYCLASS'] = df.iloc[:, 4].apply(map_property_class)
df_new['TAXDISTRICT'] = 8
 
# Billing Cycle / Reading Route Mapping
billing_and_reading_map = {
    "MEOTP01": 801, "MEOTP02": 802, "MEOROP01": 803,
    "MEOROP02": 804, "MEOROP03": 805, "MEBGRP01": 806,
    "MEBGRP02": 807, "MEBGRP03": 808, "MEBGRP04": 809,
    "MEBGRP05": 810, "MEBGRP06": 811, "MEBGRP07": 812,
    "MEBGRP08": 813, "MEBGRP09": 814, "MEBRWP01": 815,
    "MEBRWP02": 816, "MEBRWP03": 817, "MEBCKP01": 819,
    "MELINC01": 820, "METRNP01": 822, "MEBR9999": 899
}

def map_billing_and_reading(mru):
    return billing_and_reading_map.get(str(mru).strip(), "")
 
df_new['BILLINGCYCLE'] = df.iloc[:, 0].apply(map_billing_and_reading)
df_new['READINGROUTE'] = df.iloc[:, 0].apply(map_billing_and_reading)
 
# Other fields
df_new['SERVICEAREA'] = "80"
df_new['SERVICEFACILITY'] = ""
df_new['PRESSUREDISTRICT'] = ""
df_new['LATITUDE'] = ""
df_new['LONGITUDE'] = ""
df_new['MAPNUMBER'] = ""
df_new['PARCELID'] = ""
df_new['PARCELAREATYPE'] = ""
df_new['PARCELAREA'] = ""
df_new['IMPERVIOUSSQUAREFEET'] = ""
df_new['SUBDIVISION'] = ""
df_new['GISID'] = ""
df_new['FOLIOSEGMENT1'] = ""
df_new['FOLIOSEGMENT2'] = ""
df_new['FOLIOSEGMENT3'] = ""
df_new['FOLIOSEGMENT4'] = ""
df_new['FOLIOSEGMENT5'] = ""
df_new['PROPERTYUSECLASSIFICATION1'] = ""
df_new['PROPERTYUSECLASSIFICATION2'] = ""
df_new['PROPERTYUSECLASSIFICATION3'] = ""
df_new['PROPERTYUSECLASSIFICATION4'] = ""
df_new['PROPERTYUSECLASSIFICATION5'] = ""
df_new['UPDATEDATE'] = ""
 
# Remove rows with missing required fields
required_columns = [
    'LOCATIONID', 'STREETNAME', 'TOWN', 'STATE', 'ZIPCODE',
    'PROPERTYCLASS', 'TAXDISTRICT', 'BILLINGCYCLE', 'READINGROUTE'
]
df_new = df_new.dropna(subset=required_columns)
 
# Remove duplicates by LOCATIONID
df_new = df_new.drop_duplicates(subset='LOCATIONID')
 
# Ensure LOCATIONID is first column
df_new = df_new[['LOCATIONID'] + [col for col in df_new.columns if col != 'LOCATIONID']]
 
# Add trailer row
trailer_row = pd.DataFrame([['TRAILER'] + [''] * (len(df_new.columns) - 1)], columns=df_new.columns)
df_new = pd.concat([df_new, trailer_row], ignore_index=True)
 
# Convert NaN and "nan" strings to blanks
df_new = df_new.replace(['nan', 'NaN', 'None'], '', regex=True).fillna("")
 
# Custom quote logic
numeric_columns = [
    'STREETNUMBER', 'OWNERMAILSEQ', 'PROPERTYCLASS', 'TAXDISTRICT',
    'BILLINGCYCLE', 'READINGROUTE', 'SERVICEAREA', 'SERVICEFACILITY',
    'PRESSUREDISTRICT', 'LATITUDE', 'LONGITUDE', 'PARCELAREATYPE',
    'PARCELAREA', 'IMPERVIOUSSQUAREFEET', 'PROPERTYUSECLASSIFICATION1',
    'PROPERTYUSECLASSIFICATION2', 'AMPS', 'VOLTS', 'FLEXFIELD1', 'FLEXFIELD2',
    'PROPERTYUSECLASSIFICATION3', 'PROPERTYUSECLASSIFICATION4', 'PROPERTYUSECLASSIFICATION5'
]
 
def custom_quote(val, column):
    if column in numeric_columns:
        return val
    return f'"{val}"' if val not in ["", None] else val
 
df_new = df_new.apply(lambda col: col.apply(lambda val: custom_quote(val, col.name)))
 
# Output path
output_path = r"C:\Users\US82783\OneDrive - Grant Thornton Advisors LLC\Desktop\python\CONV 2B _ 2nd run\Group A\STAGE_PREMISE_V1.csv"
df_new.to_csv(output_path, index=False, quoting=csv.QUOTE_NONE, escapechar='\\')
 
print(f"File successfully saved to: {output_path}")
