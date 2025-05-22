# CONV 2- STAGE_CUST_NOTES_FINAL_0522_25_0936AM.py
# Created: 05222025
# Built using the working join logic as the foundation
# This script processes customer notes data and creates the final STAGE_CUST_NOTES CSV

import pandas as pd
import os
import csv
from datetime import datetime
import logging

# Set up logging
log_file_path = r"C:\Users\us85360\Desktop\CONV 2 - STAGE_CUST_NOTES\STAGE_CUST_NOTES_FINAL.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_file_path)
    ]
)
logger = logging.getLogger()

print("=== STAGE_CUST_NOTES FINAL PROCESSOR ===")
logger.info("Starting STAGE_CUST_NOTES processing...")

# Load the data sources
print("Loading data sources...")

# Load customer notes data from Final IR sheet
cust_df = pd.read_excel(r"C:\Users\us85360\Desktop\CONV 2 - STAGE_CUST_NOTES\5302_IR_Final_04302025.xlsx", 
                        sheet_name="Final IR", engine='openpyxl')

# Load premise details for location mapping
zdm_df = pd.read_excel(r"C:\Users\us85360\Desktop\CONV 2 - STAGE_CUST_NOTES\ZDM_PREMDETAILS.XLSX", 
                       sheet_name="Sheet1", engine='openpyxl')

logger.info(f"Loaded customer data: {len(cust_df)} rows")
logger.info(f"Loaded premise data: {len(zdm_df)} rows")

# Extract all fields from customer data (Final IR)
print("Extracting customer data...")
customer_data = pd.DataFrame({
    'CUSTOMERID': cust_df['Business Partner'].astype(str).str.strip(),
    'NOTEDATE_RAW': cust_df['Record Date'],
    'NOTEDATA': cust_df['Customer notes'].fillna('').astype(str)
})

# Convert NOTEDATE to YYYY-MM-DD format
customer_data['NOTEDATE'] = pd.to_datetime(
    customer_data['NOTEDATE_RAW'], 
    errors='coerce'
).dt.strftime('%Y-%m-%d')

# Handle any conversion errors by setting to empty string
customer_data['NOTEDATE'] = customer_data['NOTEDATE'].fillna('')

logger.info(f"Extracted customer data: {len(customer_data)} rows")
logger.info("Sample NOTEDATE conversions:")
for i in range(min(3, len(customer_data))):
    raw_date = customer_data.iloc[i]['NOTEDATE_RAW']
    converted_date = customer_data.iloc[i]['NOTEDATE']
    logger.info(f"  Row {i}: {raw_date} -> {converted_date}")

# Extract location mapping data from ZDM_PREMDETAILS
print("Extracting location mapping...")
location_data = pd.DataFrame({
    'Business_Partner_ZDM': zdm_df['Business Partener'].astype(str),  # Note the typo in column name
    'LOCATIONID': zdm_df['Premise'].astype(str)
})

# Clean the ZDM Business Partner values to match customer data format
location_data['Business_Partner_Clean'] = (location_data['Business_Partner_ZDM']
                                         .str.replace('.0', '', regex=False)
                                         .str.lstrip('0')
                                         .replace('', '0'))  # Handle all-zero case

logger.info(f"Extracted location data: {len(location_data)} rows")
logger.info("Sample Business Partner cleaning:")
for i in range(min(3, len(location_data))):
    original = location_data.iloc[i]['Business_Partner_ZDM']
    cleaned = location_data.iloc[i]['Business_Partner_Clean']
    location = location_data.iloc[i]['LOCATIONID']
    logger.info(f"  {original} -> {cleaned} (Location: {location})")

# Perform the join to get LOCATIONID for each customer
print("Joining customer data with location data...")
joined_data = customer_data.merge(
    location_data[['Business_Partner_Clean', 'LOCATIONID']], 
    left_on='CUSTOMERID',
    right_on='Business_Partner_Clean',
    how='left'
)

logger.info(f"After join: {len(joined_data)} rows")
print(f"DEBUG: After join: {len(joined_data)} rows")

# Remove duplicates - keep only the first LOCATIONID for each customer/note combination
# (In case a customer has multiple premises, we'll take the first one)
print("Handling duplicate locations...")
print(f"DEBUG: Before deduplication: {len(joined_data)} rows")
print(f"DEBUG: Unique combinations of CUSTOMERID+NOTEDATE+NOTEDATA: {joined_data[['CUSTOMERID', 'NOTEDATE', 'NOTEDATA']].drop_duplicates().shape[0]}")

joined_data_dedupe = joined_data.drop_duplicates(
    subset=['CUSTOMERID', 'NOTEDATE', 'NOTEDATA'], 
    keep='first'
)

logger.info(f"After deduplication: {len(joined_data_dedupe)} rows")
print(f"DEBUG: After deduplication: {len(joined_data_dedupe)} rows")

# Check for empty/null values that might be causing issues
print(f"DEBUG: Records with empty CUSTOMERID: {sum(joined_data_dedupe['CUSTOMERID'].isna() | (joined_data_dedupe['CUSTOMERID'] == ''))}")
print(f"DEBUG: Records with empty NOTEDATE: {sum(joined_data_dedupe['NOTEDATE'].isna() | (joined_data_dedupe['NOTEDATE'] == ''))}")
print(f"DEBUG: Records with empty NOTEDATA: {sum(joined_data_dedupe['NOTEDATA'].isna() | (joined_data_dedupe['NOTEDATA'] == ''))}")

# Show sample of data at this point
print("DEBUG: Sample of joined_data_dedupe:")
for i in range(min(5, len(joined_data_dedupe))):
    row = joined_data_dedupe.iloc[i]
    print(f"  Row {i}: CUSTOMERID={row['CUSTOMERID']}, LOCATIONID={row['LOCATIONID']}, NOTEDATE={row['NOTEDATE']}")

# Also check the original customer_data size
print(f"DEBUG: Original customer_data size: {len(customer_data)}")
print(f"DEBUG: Original location_data size: {len(location_data)}")

# Check how many customers actually match
matching_customers = set(customer_data['CUSTOMERID']) & set(location_data['Business_Partner_Clean'])
print(f"DEBUG: Customers that should match: {len(matching_customers)}")

# Create the final output with all required fields
print("Creating final output structure...")
final_output = pd.DataFrame({
    'CUSTOMERID': joined_data_dedupe['CUSTOMERID'],
    'LOCATIONID': joined_data_dedupe['LOCATIONID'].fillna(''),  # Replace NaN with empty string
    'APPLICATION': '5',  # Hardcoded
    'NOTEDATE': joined_data_dedupe['NOTEDATE'],
    'NOTETYPE': '9990',  # Hardcoded
    'WORKORDERNUMBER': ' ',  # Hardcoded as blank
    'NOTEDATA': joined_data_dedupe['NOTEDATA'],
    'UPDATEDATE': ' '  # Hardcoded as blank
})

logger.info(f"Final output created: {len(final_output)} rows")

# Log statistics
customerid_populated = sum(final_output['CUSTOMERID'] != '')
locationid_populated = sum(final_output['LOCATIONID'] != '')
notedate_populated = sum(final_output['NOTEDATE'] != '')
notedata_populated = sum(final_output['NOTEDATA'] != '')

logger.info(f"Records with CUSTOMERID populated: {customerid_populated}")
logger.info(f"Records with LOCATIONID populated: {locationid_populated}")
logger.info(f"Records with NOTEDATE populated: {notedate_populated}")
logger.info(f"Records with NOTEDATA populated: {notedata_populated}")

# Show sample final data
logger.info("Sample final output:")
for i in range(min(3, len(final_output))):
    sample_row = final_output.iloc[i].to_dict()
    logger.info(f"  Row {i}: {sample_row}")

# Apply proper formatting for CSV export
print("Preparing data for CSV export...")

# Create clean final output without manual quote formatting
final_output_clean = pd.DataFrame({
    'CUSTOMERID': joined_data_dedupe['CUSTOMERID'],
    'LOCATIONID': joined_data_dedupe['LOCATIONID'].fillna(''),
    'APPLICATION': '5',
    'NOTEDATE': joined_data_dedupe['NOTEDATE'],
    'NOTETYPE': '9990',
    'WORKORDERNUMBER': ' ',
    'NOTEDATA': joined_data_dedupe['NOTEDATA'],
    'UPDATEDATE': ' '
})

# Ensure column order matches requirements
column_order = [
    "CUSTOMERID", "LOCATIONID", "APPLICATION", "NOTEDATE", "NOTETYPE",
    "WORKORDERNUMBER", "NOTEDATA", "UPDATEDATE"
]

final_output_clean = final_output_clean[column_order]

# Add trailer row
print("Adding trailer row...")
trailer_row = pd.DataFrame([["TRAILER"] + [''] * (len(final_output_clean.columns) - 1)], 
                          columns=final_output_clean.columns)
final_output_with_trailer = pd.concat([final_output_clean, trailer_row], ignore_index=True)

# Save to CSV
output_path = r"C:\Users\us85360\Desktop\CONV 2 - STAGE_CUST_NOTES\STAGE_CUST_NOTES_FINAL_0522_25_0936AM.csv"

print(f"Saving to CSV: {output_path}")
try:
    # Write CSV manually to ensure proper formatting
    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
        # Write header
        csvfile.write('CUSTOMERID,LOCATIONID,APPLICATION,NOTEDATE,NOTETYPE,WORKORDERNUMBER,NOTEDATA,UPDATEDATE\n')
        
        # Write data rows
        for i, row in final_output_clean.iterrows():
            csvfile.write(f"{row['CUSTOMERID']},{row['LOCATIONID']},{row['APPLICATION']},{row['NOTEDATE']},{row['NOTETYPE']},{row['WORKORDERNUMBER']},{row['NOTEDATA']},{row['UPDATEDATE']}\n")
        
        # Write trailer
        csvfile.write('TRAILER,,,,,,,\n')
    
    logger.info(f"CSV file successfully saved at: {output_path}")
    logger.info(f"Total records exported: {len(final_output_clean)} (plus trailer)")
    print(f"✅ SUCCESS: CSV file saved at {output_path}")
    
except Exception as e:
    logger.error(f"Error saving CSV file: {e}")
    print(f"❌ ERROR saving CSV: {e}")
    
    # Fallback: try using pandas with different settings
    try:
        final_output_with_trailer.to_csv(
            output_path, 
            index=False, 
            header=True, 
            quoting=csv.QUOTE_NONE,
            escapechar=None,
            encoding='utf-8'
        )
        print("✅ Fallback save successful")
    except Exception as e2:
        print(f"❌ Fallback save also failed: {e2}")

# Final summary
print(f"\n=== PROCESSING COMPLETE ===")
print(f"Input records: {len(cust_df)}")
print(f"Output records: {len(final_output)} (plus trailer)")
print(f"Records with LOCATIONID: {locationid_populated}")
print(f"Success rate: {locationid_populated/len(final_output)*100:.1f}%")

logger.info("STAGE_CUST_NOTES processing completed successfully")
print("Done!")