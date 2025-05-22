# CONV2 - STAGE_CUST_INFO_05222025_0441am.py
# 
# Created: 05202025
# This script processes customer information data from multiple sources,
# applies field mappings and transformations, and exports to CSV.
# Fixed: 05222025

import pandas as pd
import os
import csv
from datetime import datetime
import sys
import logging

# Define the file paths with direct paths
file_paths = {
    "ZDM_PREMDETAILS": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_CUST_NOTES\ZDM_PREMDETAILS.XLSX",
    "CUST": r"C:\Users\us85360\Desktop\CONV 2 - STAGE_CUST_NOTES\5302_IR_Final_04302025.xlsx",
}

# Set up logging - use a path in the same directory as your input files
log_dir = os.path.dirname(list(file_paths.values())[0])
log_file_path = os.path.join(log_dir, f"CONV2_STAGE_CUST_INFO_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

try:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file_path)
        ]
    )
    logger = logging.getLogger()
    logger.info(f"Log file created at: {log_file_path}")
except Exception as e:
    print(f"Error setting up logging: {e}")
    print(f"Will continue without file logging")
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    logger = logging.getLogger()

def load_excel_file(file_path, sheet_name):
    """Load an Excel file and return a DataFrame, with proper error handling."""
    try:
        logger.info(f"Loading {file_path}, sheet: {sheet_name}")
        df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')
        logger.info(f"Successfully loaded {len(df)} rows from {file_path}")
        return df
    except Exception as e:
        logger.error(f"Error loading {file_path}: {e}")
        return None

# Load all the data sources
data_sources = {}
for name, path in file_paths.items():
    # Set appropriate sheet name based on file type
    if name == "ZDM_PREMDETAILS":
        sheet_name = "Sheet1"
    elif name == "CUST":
        sheet_name = "Final IR"  # Use the correct sheet name
    else:
        sheet_name = "Sheet1"
    
    data_sources[name] = load_excel_file(path, sheet_name)

# Check if all required sources are loaded
required_sources = ["ZDM_PREMDETAILS", "CUST"]
for source in required_sources:
    if data_sources.get(source) is None:
        logger.error(f"Required data source {source} could not be loaded. Exiting.")
        sys.exit(1)

# Print column names for debugging
logger.info("ZDM_PREMDETAILS columns:")
for i, col_name in enumerate(data_sources["ZDM_PREMDETAILS"].columns):
    logger.info(f"Column {i}: {col_name}")

logger.info("CUST (Final IR) columns:")
for i, col_name in enumerate(data_sources["CUST"].columns):
    logger.info(f"Column {i}: {col_name}")

# Process the data - Join the two data sources
if data_sources.get("CUST") is not None and data_sources.get("ZDM_PREMDETAILS") is not None:
    cust_df = data_sources["CUST"]
    zdm_df = data_sources["ZDM_PREMDETAILS"]
    
    logger.info(f"CUST data has {len(cust_df)} rows")
    logger.info(f"ZDM_PREMDETAILS data has {len(zdm_df)} rows")
    
    # Extract data from CUST (Final IR)
    cust_df_extracted = pd.DataFrame({
        "CUSTOMERID": cust_df.iloc[:, 0].astype(str).str.strip(),  # Column A - Business Partner
        "BusinessPartner": cust_df.iloc[:, 0].astype(str).str.strip(),  # Column A - for joining
        "NOTEDATE_RAW": cust_df.iloc[:, 1],  # Column B - Record Date
        "NOTEDATA": cust_df.iloc[:, 4].fillna('').astype(str)  # Column E - Customer Notes
    })
    
    # Convert NOTEDATE to YYYY-MM-DD format
    cust_df_extracted["NOTEDATE"] = pd.to_datetime(
        cust_df_extracted["NOTEDATE_RAW"], 
        errors='coerce'
    ).dt.strftime('%Y-%m-%d')
    
    # Handle any conversion errors by setting to empty string
    cust_df_extracted["NOTEDATE"] = cust_df_extracted["NOTEDATE"].fillna('')
    
    logger.info(f"Sample NOTEDATE conversions:")
    for i in range(min(5, len(cust_df_extracted))):
        raw_date = cust_df_extracted.iloc[i]["NOTEDATE_RAW"]
        converted_date = cust_df_extracted.iloc[i]["NOTEDATE"]
        logger.info(f"Row {i}: {raw_date} -> {converted_date}")
    
    # Extract Business Partner and LOCATIONID from ZDM_PREMDETAILS  
    zdm_df_extracted = pd.DataFrame({
        "BusinessPartner": zdm_df.iloc[:, 7].astype(str).str.strip(),  # Column H - Business Partner
        "LOCATIONID": zdm_df.iloc[:, 2].fillna('').astype(str).str.strip()  # Column C - PREMISE
    })
    
    # DEBUG: Let's examine the actual values to understand why there's no match
    logger.info("Sample Business Partner values from CUST:")
    for i in range(min(10, len(cust_df_extracted))):
        bp_value = cust_df_extracted.iloc[i]["BusinessPartner"]
        logger.info(f"CUST Row {i}: '{bp_value}' (type: {type(bp_value)}, length: {len(bp_value)})")
    
    logger.info("Sample Business Partner values from ZDM_PREMDETAILS:")
    for i in range(min(10, len(zdm_df_extracted))):
        bp_value = zdm_df_extracted.iloc[i]["BusinessPartner"]
        logger.info(f"ZDM Row {i}: '{bp_value}' (type: {type(bp_value)}, length: {len(bp_value)})")
    
    # Log join information
    logger.info(f"Unique Business Partners in CUST: {cust_df_extracted['BusinessPartner'].nunique()}")
    logger.info(f"Unique Business Partners in ZDM_PREMDETAILS: {zdm_df_extracted['BusinessPartner'].nunique()}")
    
    # Find common Business Partners
    common_partners = set(cust_df_extracted['BusinessPartner']) & set(zdm_df_extracted['BusinessPartner'])
    logger.info(f"Common Business Partners: {len(common_partners)}")
    
    # If no common partners, let's try some data cleaning approaches
    if len(common_partners) == 0:
        logger.warning("No common Business Partners found. Attempting data cleaning...")
        
        # Try removing leading zeros, spaces, and other common formatting issues
        cust_df_extracted["BusinessPartner_Clean"] = cust_df_extracted["BusinessPartner"].str.lstrip('0').str.strip()
        zdm_df_extracted["BusinessPartner_Clean"] = zdm_df_extracted["BusinessPartner"].str.lstrip('0').str.strip()
        
        # Check for matches with cleaned data
        common_partners_clean = set(cust_df_extracted['BusinessPartner_Clean']) & set(zdm_df_extracted['BusinessPartner_Clean'])
        logger.info(f"Common Business Partners after cleaning: {len(common_partners_clean)}")
        
        if len(common_partners_clean) > 0:
            # Use cleaned data for joining
            join_column = "BusinessPartner_Clean"
            logger.info("Using cleaned Business Partner values for joining")
        else:
            # Still no matches - use original data but warn user
            join_column = "BusinessPartner"
            logger.warning("Still no matches found. Proceeding with left join - all LOCATIONID will be empty")
    else:
        join_column = "BusinessPartner"
    
    # Merge on Business Partner
    df_merged = cust_df_extracted.merge(
        zdm_df_extracted,
        how="left",
        on=join_column
    )
    
    logger.info(f"After merge: {len(df_merged)} rows")
    
    # Create the final dataframe with all required fields
    df_new = pd.DataFrame({
        "CUSTOMERID": df_merged["CUSTOMERID"],
        "LOCATIONID": df_merged["LOCATIONID"].fillna(''),  # Replace NaN with empty string
        "APPLICATION": "5",  # Hardcoded
        "NOTEDATE": df_merged["NOTEDATE"],
        "NOTETYPE": "9990",  # Hardcoded
        "WORKORDERNUMBER": " ",  # Hardcoded as blank
        "NOTEDATA": df_merged["NOTEDATA"],
        "UPDATEDATE": " "  # Hardcoded as blank
    })
    
    logger.info(f"Final dataframe created with {len(df_new)} rows")
    
    # Log statistics
    customerid_populated = sum(df_new['CUSTOMERID'] != '')
    locationid_populated = sum(df_new['LOCATIONID'] != '')
    notedate_populated = sum(df_new['NOTEDATE'] != '')
    notedata_populated = sum(df_new['NOTEDATA'] != '')
    
    logger.info(f"Records with CUSTOMERID populated: {customerid_populated}")
    logger.info(f"Records with LOCATIONID populated: {locationid_populated}")
    logger.info(f"Records with NOTEDATE populated: {notedate_populated}")
    logger.info(f"Records with NOTEDATA populated: {notedata_populated}")
    
    # Show sample of final data
    logger.info("Sample of final data (first 3 rows):")
    for i in range(min(3, len(df_new))):
        sample_row = df_new.iloc[i].to_dict()
        logger.info(f"Row {i}: {sample_row}")

else:
    logger.error("Could not load both required data sources. Exiting.")
    sys.exit(1)

# Check if df_new is empty
if df_new.empty:
    logger.error("No data was processed successfully. Exiting.")
    sys.exit(1)

# Function to wrap values in double quotes, but leave blanks and NaN as they are
def custom_quote(val):
    """Wraps all values in quotes except for blank or NaN ones."""
    if pd.isna(val) or val == "" or val == " ":
        return ''  # Return an empty string for NaN or blank fields
    return f'"{val}"'  # Wrap other values in double quotes

# Apply selective quoting
def selective_custom_quote(val, column_name):
    if column_name in ['APPLICATION', 'NOTETYPE', 'WORKORDERNUMBER']:
        return val  # Keep numeric values unquoted
    return '' if val in [None, 'nan', 'NaN', 'NAN'] else custom_quote(val)

# Apply custom_quote function to all columns
df_new = df_new.fillna('')

# Apply selective quoting to each column
df_new = df_new.apply(lambda col: col.map(lambda x: selective_custom_quote(x, col.name)))

# Reorder columns based on user preference
column_order = [
    "CUSTOMERID", "LOCATIONID", "APPLICATION", "NOTEDATE", "NOTETYPE",
    "WORKORDERNUMBER", "NOTEDATA", "UPDATEDATE"
]

df_new = df_new[column_order]

# Add a trailer row with default values
trailer_row = pd.DataFrame([["TRAILER"] + [''] * (len(df_new.columns) - 1)], columns=df_new.columns)
df_new = pd.concat([df_new, trailer_row], ignore_index=True)

# Define output path for the CSV file

# Define output path for the CSV file
output_path = os.path.join(os.path.dirname(list(file_paths.values())[0]), 'STAGE_CUST_INFO_05222025_0441am.csv')

# Save to CSV with proper quoting and escape character
try:
    df_new.to_csv(output_path, index=False, header=True, quoting=csv.QUOTE_NONE, escapechar='\\')
    logger.info(f"CSV file successfully saved at: {output_path}")
    logger.info(f"Total records exported: {len(df_new) - 1}")  # Subtract 1 for trailer row
    print(f"CSV file saved at {output_path}")
except Exception as e:
    logger.error(f"Error saving output file: {e}")
    # Try an alternative location if the primary location fails
    alt_output_path = os.path.join(os.path.expanduser("~"), "Desktop", f'CONV2_CUST_NOTES_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
    try:
        df_new.to_csv(alt_output_path, index=False, header=True, quoting=csv.QUOTE_NONE, escapechar='\\')
        logger.info(f"File saved to alternative location: {alt_output_path}")
        print(f"CSV file saved at {alt_output_path}")
    except Exception as e2:
        logger.error(f"Error saving to alternative location: {e2}")