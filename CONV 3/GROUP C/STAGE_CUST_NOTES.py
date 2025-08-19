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
sys.path.append(r"C:\Users\GTUSER1\Documents\CONV 3")
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
import Conversion_Utils as cu 

cu.print_checklist()

# Load customer notes data from Interaction His 1 sheet
cust_df = cu.get_file("notes", sheet_name='Interaction His 1')

# Load premise details for location mapping
zdm_df = cu.get_file("prem")

# Extract all fields from customer data (Interaction His 1)
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

joined_data_dedupe = joined_data.drop_duplicates(
    subset=['CUSTOMERID', 'NOTEDATE', 'NOTEDATA'], 
    keep='first'
)

cu.log_debug(f"DEBUG: After deduplication: {len(joined_data_dedupe)} rows")

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

# Ensure column order matches requirements
column_order = [
    "CUSTOMERID", "LOCATIONID", "APPLICATION", "NOTEDATE", "NOTETYPE",
    "WORKORDERNUMBER", "NOTEDATA", "UPDATEDATE"
]

final_output = final_output[column_order]

# Write the DataFrame to a CSV file
cu.write_csv(final_output, r"Group C\STAGE_CUSTOMER_NOTES.csv")


