# -----------------------------------------------------------------------------
# File Name: Conversion Utils.py
# Description: Utility functions for data conversion .
# Author: Doug Smith
# Created Date: May 5, 2025
# Last Modified: 
#   May 5, 2025:  Added CSV writing function
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
import Source_Schemas
import csv
import os

# Constants
source_directory = r"C:\DV\Unitil\Conversion 2b\\"
cache_directory = r"C:\DV\Unitil\Conversion 2b\parquet\\"
output_directory = r"C:\DV\Unitil\Conversion 2b\output\\"

file_paths = {
    "active": source_directory + r"ZNC_ACTIVE_CUS.XLSX",
    "dfkkcoh": source_directory + r"DFKKCOH.XLSX",
    "dfkkzp": source_directory + r"DFKKZP.XLSX",
    "eabp": source_directory + r"EABP.XLSX",
    "el31": source_directory + r"EL31.XLSX",
    "erdk": source_directory + r"ERDK.XLSX",
    "etdz": source_directory + r"ETDZ.XLSX",
    "fpd2": source_directory + r"FPD2.XLSX",
    "ever": source_directory + r"EVER.XLSX",
    "mail": source_directory + r"MAILING_ADDR1.XLSX",
    "meter": source_directory + r"METER DETAILS.XLSX",
    "notes": source_directory + r"Interaction Records.XLSX",
    "prem": source_directory + r"ZDM_PREMDETAILS.XLSX",
    "te107": source_directory + r"TE107.XLSX",
    "te420": source_directory + r"TE420.XLSX",
    "te422": source_directory + r"TE422.XLSX",
    "writeoff": source_directory + r"ZWRITEOFF_ME1.XLSX",
    "zcampaign": source_directory + r"ZCAMPAIGN.XLSX",
    "zins": source_directory + r"ZINS.XLSX",
    "zmecon": source_directory + r"ZMECON",
    "dfkkop": source_directory + r"DFKKOP",
    "eabl": source_directory + r"EABL",
    "gldata": source_directory + r"GL Data",
    "zdmseq": source_directory + r"ZDMSEQ",
    "zmecon": source_directory + r"ZMECON",
    "stage_billing_acct": output_directory + r"Group A\STAGE_BILLING_ACCT.csv",
    "stage_premise": output_directory + r"Group A\STAGE_PREMISE.csv",
    "stage_device": output_directory + r"Group A\STAGE_DEVICE.csv",
    "stage_mail_addr": output_directory + r"Group A\STAGE_MAIL_ADDR.csv",
    "stage_cust_info": output_directory + r"Group A\STAGE_CUSTOMER_INFO.csv",
    "stage_streets": output_directory + r"Group A\STAGE_STREETS.csv",
    "stage_cycle": output_directory + r"Group A\STAGE_CYCLE.csv",
    "stage_route": output_directory + r"Group A\STAGE_ROUTE.csv",
    "stage_report_codes": output_directory + r"Group A\STAGE_REPORT_CODES.csv",
    "stage_email": output_directory + r"Group A\STAGE_EMAIL.csv",
    "stage_phone": output_directory + r"Group A\STAGE_PHONE.csv",
    "stage_towns": output_directory + r"Group A\STAGE_TOWNS.csv",
    "stage_ar_balances": output_directory + r"Group B\STAGE_AR_BALANCES.csv",
    "stage_deposits": output_directory + r"Group B\STAGE_DEPOSITS.csv",
    "stage_flat_svcs": output_directory + r"Group B\STAGE_FLAT_SVCS.csv",
    "stage_metered_svcs": output_directory + r"Group B\STAGE_METERED_SVCS.csv",
    "stage_unbilled": output_directory + r"Group B\STAGE_UNBILLED.csv",
    "stage_write_off_balances": output_directory + r"Group B\STAGE_WRITE_OFF_BALANCES.csv",
    "stage_consumption_hist": output_directory + r"Group C\STAGE_CONSUMPTION_HIST.csv",
    "stage_cust_notes": output_directory + r"Group C\STAGE_CUST_NOTES.csv",
    "stage_transactional_history": output_directory + r"Group C\STAGE_TRANSACTIONAL_HIST.csv",
    "stage_taxexempt": output_directory + r"Group D\STAGE_TAXEXEMPT.csv",
    "stage_meter_inventory": output_directory + r"Group M\STAGE_METER_INVENTORY.csv"
    }

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

def read_filepath( file_name, file_path, sheet_name, columns):
    ### Set the schema to read the file
    # Set all columns to string by default
    schema = None
    file_df = pd.DataFrame()
    file_schema = {col: str for col in columns} if columns else {}

    # If a specific schema is defined in Source_Schemas, use it
    if hasattr(Source_Schemas, file_name + "_schema"):
        schema = getattr(Source_Schemas, file_name + "_schema")
        for col in columns if columns else []:
            if col in schema:
                file_schema[col] = schema[col]

    # Read the file based on its extension
    if file_path.upper().endswith(".XLSX"):
       file_df = pd.read_excel(file_path, usecols=columns, dtype=file_schema )
    elif file_path.upper().endswith(".CSV"):
       file_df = pd.read_csv(file_path, sheet_name=sheet_name, usecols=columns, encoding='utf-8', dtype=file_schema)
    else:
        log_error(f"Unsupported file format for {file_name}. Supported formats are .xlsx and .csv.")

    # Enforce the schema if available
    if schema:
        for col, dtype in schema.items():
            if col in file_df.columns:
                if dtype == pd.Timestamp:
                    # Convert to datetime if the schema specifies Timestamp
                    file_df[col] = pd.to_datetime(file_df[col], errors='coerce')
                elif pd.api.types.is_numeric_dtype(dtype):
                    # For numeric types first convert to general numeric with coerce then conver to desired type
                    file_df[col] = pd.to_numeric(file_df[col], errors='coerce').astype(dtype)
                else:
                    # For other types, just convert directly
                    file_df[col] = file_df[col].astype(dtype)

    log_info(f"Loaded {file_path} file. Records: " + str(len(file_df)))
    
    return file_df

# Read the source file and return the DataFrame
def read_file( file_name, sheet_name="Sheet 1", columns=None, skip_cache=False ):
    file_df = pd.DataFrame()

    # Determine the source file path
    file_path = file_paths.get(file_name)
    if not file_path:
        raise ValueError(f"File '{file_name}' not found in file paths.")
    
    # Concatenate files in directories
    if os.path.isdir(file_path):
        df_part = []
        # Concatenate all files in the directory
        for sourcefile in os.listdir(file_path):
            sourcefile_path = os.path.join(file_path, sourcefile)
            df_part.append( read_filepath(file_name, sourcefile_path, sheet_name, columns) )
        file_df = pd.concat(df_part, ignore_index=True)
    else:
        file_df = read_filepath(file_name, file_path, sheet_name, columns)

    return file_df

def read_cache( file_name ):
    """
    Reads a cached DataFrame from a parquet file.
    
    :param file_name: Name of the file to read from cache (without extension)
    :return: DataFrame containing the cached data
    """
    cache_file = cache_directory + file_name + ".parquet"
    if file_name in file_paths and pd.io.common.file_exists(cache_file) :
        df = pd.read_parquet(cache_file)    
        log_info(f"Loaded {file_name} from cache. Records: " + str(len(df)))
        return df
    else:
        return None
    
def write_cache( df, file_name ):
    """
    Writes a DataFrame to a parquet file in the cache directory.
    
    :param df: DataFrame to write to cache
    :param file_name: Name of the file to save in cache (without extension)
    """
    cache_file = cache_directory + file_name + ".parquet"
    df = prepare_dataframe_to_parquet(df) 
    df.to_parquet(cache_file, index=False)
    log_info(f"Saved {file_name} to cache as parquet. Records: " + str(len(df)))

def get_file( file_name, sheet_name=None, columns=None, skip_cache=False ):
    """
    Retrieves a DataFrame for a specified file, optionally selecting specific columns.
    Checks if there is a parquet version of the file first, and if not, reads from the Excel file.
    Creates a parquest file if it does not exist.
    
    :param file_name: Name of the file to retrieve (e.g., 'active', 'erdk', etc.)
    :param columns: List of columns to select from the DataFrame (optional)
    :param skip_cache: If True, skips the cache and reads directly from the source file (default is False)
    :return: DataFrame containing the data from the specified file
    """
    # Ensure the file mapping information is available
    if file_name not in file_paths:
        raise ValueError(f"File '{file_name}' not found in file paths.")

    # Read cache file
    df = read_cache(file_name) if not skip_cache else None
    
    # If not in cache, read from the source file
    if df is None:
        df = read_file(file_name, sheet_name, columns, skip_cache)
        write_cache(df, file_name)  # Write to cache after reading from source

    # If specific columns are requested, filter the DataFrame
        if columns:
            if set(columns).issubset(df.columns):
                df = df[columns]
                return df
            else:
                log_info(f"Columns {columns} not found in {file_name} cache.")
    
    return df

def prepare_dataframe_to_parquet(df):
    """
    Prepares a DataFrame for saving to parquet by converting all columns to string type.
    This is useful for ensuring compatibility with various data types.
    
    :param df: DataFrame to prepare
    :return: DataFrame with all columns converted to string type
    """
    # Check if the DataFrame is empty
    if df.empty:    
        log_warning("DataFrame is empty. Returning an empty DataFrame.")
        return df

    # Convert all non-nuimeric columns to string type
    for col in df.select_dtypes(exclude=['number']).columns:
        df[col] = df[col].astype(str)
    
    return df


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

def get_output_path(file_name):
    """
    Returns the full output path for a given file name.
    
    :param file_name: Name of the file to be saved in the output directory
    :return: Full path to the output file
    """
    return output_directory + file_name

def write_csv(df, file_name):
    """
    Writes a DataFrame to a CSV file with specific formatting rules.
    Cleanses string values, replaces NaN values, adds a trailer row, and applies custom quoting logic.
    Adds trailer row with 'TRAILER' in the first column.
    
    :param df: DataFrame to write to CSV
    :param file_name: Name of the output CSV file
    """
    # Ensure the DataFrame is not empty
    if df.empty:
        log_warning(f"DataFrame is empty. No data to write to {file_name}.")
        return
    
    # Prepare the DataFrame for CSV output
    df = df.applymap(lambda x: cleanse_string(x) if isinstance(x, str) else x)

    # Replace NA with empty string
    df = df.fillna('')

    # Replace NaN and "nan" strings that are blank with None to avoid creating quotes in the CSV
    df = df.replace(['nan', 'NaN', 'None', ' ', ''], None)

    # Add a trailer row
    trailer_row = pd.DataFrame([['TRAILER'] + [''] * (len(df.columns) - 1)], columns=df.columns)
    df = pd.concat([df, trailer_row], ignore_index=True)

    # Write to CSV
    output_path = output_directory + file_name
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_STRINGS, escapechar='\\')
        # Write header
        writer.writerow(df.columns)

        # Write rows
        for row in df.itertuples(index=False):
            processed_row = []
            for val in row:
                # Only qutoe strings
                if isinstance(val, str):
                    # Apply quoting only to non-empty strings
                    if val.strip() == '':
                        processed_row.append('')
                    # Quote Strings will add quotes to all string values which are not empty
                    else:
                        processed_row.append(val)  # Add quotes to all string values, no escape character
                else:
                    processed_row.append(val)
            writer.writerow(processed_row)

    log_info(f"CSV file saved at {output_path} with {len(df)} rows.")
