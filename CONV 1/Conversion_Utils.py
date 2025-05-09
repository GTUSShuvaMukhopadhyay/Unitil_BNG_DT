# -----------------------------------------------------------------------------
# File Name: Conversion Utils.py
# Description: Utility functions for data conversion .
# Author: Doug Smith
# Created Date: May 5, 2025
# Last Modified: 
# Version: 1.0
# -----------------------------------------------------------------------------
# Notes:
# - This file contains reusable functions for data conversion tasks.
# - Ensure to follow coding standards and document any changes.
# -----------------------------------------------------------------------------

import pandas as pd
import logging
import time
import re

# Constants
source_directory = r"C:\DV\Unitil\\"

file_paths = {
    "prem": source_directory + r"ZDM_PREMDETAILS.XLSX",
    "ever": source_directory + r"EVER.XLSX",
    "active": source_directory + r"ZNC_ACTIVE_CUS.XLSX",
    "writeoff": source_directory + r"Write off customer history.XLSX",
    "erdk": source_directory + r"ERDK.XLSX",
    "zmecon1": source_directory + r"ZMECON 01012021 to 02132025.xlsx",
    "zmecon2": source_directory + r"ZMECON 01012015 to 12312020.xlsx" }

logging.basicConfig(
    format='%(levelname)s:%(message)s',
    level=logging.DEBUG,
    handlers=[
        logging.FileHandler("conversion.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

start_time = time.time()
last_time = time.time()

def get_file( file_name, columns=None ):
    read_opts = {"engine": "openpyxl"}

    # Concatenate ZMECON files if necessary
    if file_name == "zmecon":
        file_df = pd.concat([get_file("zmecon1"), get_file("zmecon2")], ignore_index=True)
    else:
        # Read the specified file
        file_df = pd.read_excel(file_paths[ file_name ])
        log_info(f"Loaded {file_name} file. Records: " + str(len(file_df)))
    return file_df

def cleanse_string(value, max_length=None):
    """
    Cleanses a string by stripping extra whitespace and truncating to max_length if provided.
    """
    if pd.isna(value):
        return ''
    if isinstance(value, (int, float)):
        value = str(int(value))
    value = str(value).strip()
    value = re.sub( r"\s+", " ", value ) # Replace multiple spaces with a single space
    if max_length:
        value = value[:max_length]
    return value

def get_log_message(message):
    global last_time
    elapsed_time = time.time() - start_time
    interval_time = time.time() - last_time
    last_time = time.time()
    return "Elapsed Time: " + time.strftime("%H:%M:%S", time.gmtime(elapsed_time)) + " Interval Time: " + time.strftime("%H:%M:%S", time.gmtime(interval_time)) + " | Message: " + message
    
def log_info(message):
    logger.info( get_log_message(message))

def log_error(message):
    logger.error( get_log_message(message))

def log_warning(message):
    logger.warning( get_log_message(message))   

def log_debug(message): 
    logger.debug( get_log_message(message))
