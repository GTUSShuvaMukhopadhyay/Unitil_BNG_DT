# CONV 2- STAGE_CUST_NOTES_FINAL_0522_25_0936AM.py
# Created: 05222025
# Built using the working join logic as the foundation
# This script processes customer notes data and creates the final STAGE_CUST_NOTES CSV

import pandas as pd
import numpy as np
import os
import csv
from datetime import datetime
import logging
import re

 # Add the parent directory to sys.path
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
import Conversion_Utils as cu 

cu.print_checklist()

# Load customer notes data from Final IR sheet
cust_df = cu.get_file("notes", sheet_name='Final IR')

# Load premise details for location mapping
zdm_df = cu.get_file("prem")

# Extract all fields from customer data (Final IR)
cu.log_debug("Extracting customer data...")
customer_data = pd.DataFrame({
    'CUSTOMERID': cust_df['Business Partner'].astype(pd.Int64Dtype()).replace(np.nan, None),  # Convert to Int64 and handle NaN
    'NOTEDATE_RAW': cust_df['Record Date'],
    'NOTEDATA': cust_df['Final IR'].fillna('').astype(str)
})

# Convert NOTEDATE to YYYY-MM-DD format
customer_data['NOTEDATE'] = pd.to_datetime(
    customer_data['NOTEDATE_RAW'], 
    errors='coerce'
).dt.strftime('%Y-%m-%d')

# Handle any conversion errors by setting to empty string
customer_data['NOTEDATE'] = customer_data['NOTEDATE'].fillna('')

cu.log_info(f"Extracted customer data: {len(customer_data)} rows")
cu.log_info("Sample NOTEDATE conversions:")
for i in range(min(3, len(customer_data))):
    raw_date = customer_data.iloc[i]['NOTEDATE_RAW']
    converted_date = customer_data.iloc[i]['NOTEDATE']
    cu.log_info(f"  Row {i}: {raw_date} -> {converted_date}")

# Extract location mapping data from ZDM_PREMDETAILS
cu.log_debug("Extracting location mapping...")
location_data = pd.DataFrame({
    'Business_Partner_ZDM': zdm_df['Business Partener'].astype(pd.Int64Dtype()).replace(np.nan, None),  # Note the typo in column name
    'LOCATIONID': zdm_df['Premise'].astype(int)
})

# Perform the join to get LOCATIONID for each customer
cu.log_debug("Joining customer data with location data...")
joined_data = customer_data.merge(
    location_data[['Business_Partner_ZDM', 'LOCATIONID']], 
    left_on='CUSTOMERID',
    right_on='Business_Partner_ZDM',
    how='left'
)

cu.log_info(f"After join: {len(joined_data)} rows")
cu.log_debug(f"DEBUG: After join: {len(joined_data)} rows")

# Remove duplicates - keep only the first LOCATIONID for each customer/note combination
# (In case a customer has multiple premises, we'll take the first one)
cu.log_debug("Handling duplicate locations...")
cu.log_debug(f"DEBUG: Before deduplication: {len(joined_data)} rows")
cu.log_debug(f"DEBUG: Unique combinations of CUSTOMERID+NOTEDATE+NOTEDATA: {joined_data[['CUSTOMERID', 'NOTEDATE', 'NOTEDATA']].drop_duplicates().shape[0]}")

joined_data_dedupe = joined_data.drop_duplicates(
    subset=['CUSTOMERID', 'NOTEDATE', 'NOTEDATA'], 
    keep='first'
)

cu.log_info(f"After deduplication: {len(joined_data_dedupe)} rows")
cu.log_debug(f"DEBUG: After deduplication: {len(joined_data_dedupe)} rows")

# Check for empty/null values that might be causing issues
cu.log_debug(f"DEBUG: Records with empty CUSTOMERID: {sum(joined_data_dedupe['CUSTOMERID'].isna() | (joined_data_dedupe['CUSTOMERID'] == ''))}")
cu.log_debug(f"DEBUG: Records with empty NOTEDATE: {sum(joined_data_dedupe['NOTEDATE'].isna() | (joined_data_dedupe['NOTEDATE'] == ''))}")
cu.log_debug(f"DEBUG: Records with empty NOTEDATA: {sum(joined_data_dedupe['NOTEDATA'].isna() | (joined_data_dedupe['NOTEDATA'] == ''))}")

# Show sample of data at this point
cu.log_debug("DEBUG: Sample of joined_data_dedupe:")
for i in range(min(5, len(joined_data_dedupe))):
    row = joined_data_dedupe.iloc[i]
    cu.log_debug(f"  Row {i}: CUSTOMERID={row['CUSTOMERID']}, LOCATIONID={row['LOCATIONID']}, NOTEDATE={row['NOTEDATE']}")

# Also check the original customer_data size
cu.log_debug(f"DEBUG: Original customer_data size: {len(customer_data)}")
cu.log_debug(f"DEBUG: Original location_data size: {len(location_data)}")

# Check how many customers actually match
matching_customers = set(customer_data['CUSTOMERID']) & set(location_data['Business_Partner_ZDM'])
cu.log_debug(f"DEBUG: Customers that should match: {len(matching_customers)}")

# Create the final output with all required fields
cu.log_debug("Creating final output structure...")
final_output = pd.DataFrame({
    'CUSTOMERID': joined_data_dedupe['CUSTOMERID'],
    'LOCATIONID': joined_data_dedupe['LOCATIONID'].fillna('').apply(lambda x: str(int(x)) if str(x).strip() != '' else ''),  # Replace NaN with empty string
    'APPLICATION': '5',  # Hardcoded
    'NOTEDATE': joined_data_dedupe['NOTEDATE'],
    'NOTETYPE': '9990',  # Hardcoded
    'WORKORDERNUMBER': ' ',  # Hardcoded as blank
    'NOTEDATA': joined_data_dedupe['NOTEDATA'],
    'UPDATEDATE': ' '  # Hardcoded as blank
})

cu.log_info(f"Final output created: {len(final_output)} rows")

# Log statistics
customerid_populated = sum(final_output['CUSTOMERID'] != '')
locationid_populated = sum(final_output['LOCATIONID'] != '')
notedate_populated = sum(final_output['NOTEDATE'] != '')
notedata_populated = sum(final_output['NOTEDATA'] != '')

cu.log_info(f"Records with CUSTOMERID populated: {customerid_populated}")
cu.log_info(f"Records with LOCATIONID populated: {locationid_populated}")
cu.log_info(f"Records with NOTEDATE populated: {notedate_populated}")
cu.log_info(f"Records with NOTEDATA populated: {notedata_populated}")

# Show sample final data
cu.log_info("Sample final output:")
for i in range(min(3, len(final_output))):
    sample_row = final_output.iloc[i].to_dict()
    cu.log_info(f"  Row {i}: {sample_row}")

# Apply proper formatting for CSV export
cu.log_debug("Preparing data for CSV export...")

# Bad Characters remove
def clean_text(text):
    if pd.isna(text):
        return ''
    text = str(text)
    # Replace known problematic characters
    text = text.replace('Æ', "'").replace('û', '-')
    # Remove non-ASCII characters (or replace as needed)
    return re.sub(r'[^\x00-\x7F]', '', str(text))

# Create clean final output without manual quote formatting
final_output_clean = pd.DataFrame({
    'CUSTOMERID': joined_data_dedupe['CUSTOMERID'],
    'LOCATIONID': joined_data_dedupe['LOCATIONID'].fillna('').apply(lambda x: str(int(x)) if str(x).strip() != '' else ''),
    'APPLICATION': '5',
    'NOTEDATE': joined_data_dedupe['NOTEDATE'],
    'NOTETYPE': '9990',
    'WORKORDERNUMBER': ' ',
    'NOTEDATA': joined_data_dedupe['NOTEDATA'].apply(clean_text),
    'UPDATEDATE': ' '
})

# Ensure column order matches requirements
column_order = [
    "CUSTOMERID", "LOCATIONID", "APPLICATION", "NOTEDATE", "NOTETYPE",
    "WORKORDERNUMBER", "NOTEDATA", "UPDATEDATE"
]

final_output_clean = final_output_clean[column_order]

# Write the DataFrame to a CSV file
cu.write_csv(final_output_clean, "Group C\STAGE_CUSTOMER_NOTES.csv" )

# Final summary
cu.log_debug(f"\n=== PROCESSING COMPLETE ===")
cu.log_debug(f"Input records: {len(cust_df)}")
cu.log_debug(f"Output records: {len(final_output)} (plus trailer)")
cu.log_debug(f"Records with LOCATIONID: {locationid_populated}")
cu.log_debug(f"Success rate: {locationid_populated/len(final_output)*100:.1f}%")

cu.log_info("STAGE_CUST_NOTES processing completed successfully")
cu.log_debug("Done!")
