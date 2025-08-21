# -----------------------------------------------------------------------------
# File Name: Conversion Utils.py
# Description: Utility functions for data conversion .
# Author: Doug Smith
# Created Date: May 5, 2025
# Last Modified:
# Version: 1.0
# path:C:\Users\GTUSER1\Documents\.py
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
source_directory = r"C:\Users\GTUSER1\Documents\CONV 3\\"
 
file_paths = {
    "active": source_directory + r"ZNC_ACTIVE_CUS.XLSX",
    "config": source_directory + r"Configuration.xlsx",
    "dfkkcoh": source_directory + r"DFKKCOH - 08012019 to 08012025.XLSX",
    "dfkkop1": source_directory + r"DFKKOP\DFKKOP 01012015 to 12312016.XLSX",
    "dfkkop2": source_directory + r"DFKKOP\DFKKOP 01012024 to 12312024.XLSX",
    "dfkkop3": source_directory + r"DFKKOP\DFKKOP 01012017 to 12312017.XLSX",
    "dfkkop4": source_directory + r"DFKKOP\DFKKOP 01012018 to 12312018.XLSX",
    "dfkkop5": source_directory + r"DFKKOP\DFKKOP 01012019 to 12312019.XLSX",
    "dfkkop6": source_directory + r"DFKKOP\DFKKOP 01012020 to 12312020.XLSX",
    "dfkkop7": source_directory + r"DFKKOP\DFKKOP 01012021 to 12312021.XLSX",
    "dfkkop8": source_directory + r"DFKKOP\DFKKOP 01012022 to 12312022.XLSX",
    "dfkkop9": source_directory + r"DFKKOP\DFKKOP 01012023 to 12312023.XLSX",
    "dfkkop10": source_directory + r"DFKKOP\DFKKOP 01012025 TO 12312025.XLSX",
    "dfkkzp": source_directory + r"DFKKZP.XLSX",
    "eabp": source_directory + r"EABP.XLSX",
    "eabl": source_directory + r"EABL - 08012019 TO 08012025.XLSX",
    "el31": source_directory + r"EL31.XLSX",
    "erdk": source_directory + r"ERDK - 08012019 to 08012025.XLSX",
    "etdz": source_directory + r"ETDZ.XLSX",
    "fpd2_full": source_directory + r"FPD2 - Full Report - 0802.XLSX",
    "fpd2_modified": source_directory + r"FPD2 - Modified Report - 0802.XLSX",
    "ever": source_directory + r"EVER - 0802.XLSX",
    "gl_balance": source_directory + r"GL BALANCE",  # Directory
    "interaction_records": source_directory + r"Interaction Records - 08012024 to 08012025.xlsx",
    "notes": source_directory + r"Interaction Records - 08012024 to 08012025.xlsx",
    "invoices": source_directory + r"INVOICES",  # Directory
    "mail": source_directory + r"MAILING_ADDR1.XLSX",
    "meter": source_directory + r"Meter Details Report.xlsx",
    "te107": source_directory + r"TE107 - 08012019 to 08012025.XLSX",
    "te420": source_directory + r"TE420 - 0802.XLSX",
    "te422": source_directory + r"TE422 - 0802.XLSX",
    "zcampaign": source_directory + r"ZCAMPAIGN",  # Directory
    "prem": source_directory + r"ZDM_PREMDETAILS.XLSX",
    "zdmseq": source_directory + r"ZDMSEQ - 0802",  # Directory
    "zins": source_directory + r"ZINS.XLSX",
    "zmecon_text": source_directory + r"ZMECON - In text format - Not required.txt",
    "zmecon1": source_directory + r"ZMECON\ZMECON 010115 TO 07312019.xlsx",
    "zmecon2": source_directory + r"ZMECON\ZMECON 08012019 to 08012025.xlsx",    
    "writeoff": source_directory + r"ZWRITEOFF_ME1 - 0802.XLSX",
    "identification": source_directory + r"5302 - Indentification Details.XLSX.gpg",
    "codes_descriptions": source_directory + r"Codes and descriptions - 0802.xlsx",
    "conversion_request": source_directory + r"Conversion 3 Data Extract Request List.xlsx"}
 
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

def get_file( file_name, columns=None, skip_cache=False):
    read_opts = {"engine": "openpyxl"}
 
    # Concatenate ZMECON files if necessary
    if file_name == "zmecon":
        file_df = pd.concat([get_file("zmecon1", columns, skip_cache), get_file("zmecon2", columns, skip_cache)], ignore_index=True)
    elif file_name == "dfkkop":
        # Concatenate DFKKOP files if necessary
        file_df = pd.concat([get_file("dfkkop1", columns, skip_cache), get_file("dfkkop2", columns, skip_cache), get_file("dfkkop3", columns, skip_cache),
                             get_file("dfkkop4", columns, skip_cache), get_file("dfkkop5", columns, skip_cache), get_file("dfkkop6", columns, skip_cache),
                             get_file("dfkkop7", columns, skip_cache), get_file("dfkkop8", columns, skip_cache), get_file("dfkkop9", columns, skip_cache),
                             get_file("dfkkop10", columns, skip_cache)], ignore_index=True)
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
    value = re.sub( r"\"", "\"\"", value ) # Replace double quotes with 2x double quotes
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