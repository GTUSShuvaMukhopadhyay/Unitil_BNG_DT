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
import Source_Schemas

# Constants
source_directory = r"C:\DV\Unitil\Conversion 2b\\"
cache_directory = r"C:\DV\Unitil\Conversion 2b\parquet\\"
output_directory = r"C:\DV\Unitil\Conversion 2b\output\\"

file_paths = {
    "active": source_directory + r"ZNC_ACTIVE_CUS.XLSX",
    "erdk": source_directory + r"ERDK.XLSX",
    "ever": source_directory + r"EVER.XLSX",
    "mail": source_directory + r"MAILING_ADDR1.XLSX",
    "prem": source_directory + r"ZDM_PREMDETAILS.XLSX",
    #"writeoff": source_directory + r"Write off customer history.XLSX",
    "writeoff": source_directory + r"ZWRITEOFF_ME1.XLSX",
    #"zmecon1": source_directory + r"ZMECON 01012021 to 02132025.xlsx",
    "zmecon": "Multiple Files",
    "zmecon1": source_directory + r"ZMECON 010121 to 061425.xlsx",
    "zmecon2": source_directory + r"ZMECON 010115 to 12312020.xlsx",
    "dfkkop": "Multiple Files",
    "dfkkop1": source_directory + r"DFKKOP 01012015 to 12312015.XLSX",
    "dfkkop2": source_directory + r"DFKKOP 01012016 to 12312016.XLSX",
    "dfkkop3": source_directory + r"DFKKOP 01012017 to 12312017.XLSX",
    "dfkkop4": source_directory + r"DFKKOP 01012018 to 12312018.XLSX",
    "dfkkop5": source_directory + r"DFKKOP 01012019 to 12312019.XLSX",
    "dfkkop6": source_directory + r"DFKKOP 01012020 to 12312020.XLSX",
    "dfkkop7": source_directory + r"DFKKOP 01012021 to 12312021.XLSX",
    "dfkkop8": source_directory + r"DFKKOP 01012022 to 12312022.XLSX",
    "dfkkop9": source_directory + r"DFKKOP 01012023 to 12312023.XLSX",
    "dfkkop10": source_directory + r"DFKKOP 01012024 TO 03272025.XLSX",
    "stage_transactional_history": output_directory + r"STAGE_TRANSACTIONAL_HIST.csv",
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

def read_file( file_name, columns=None, skip_cache=False ):
    file_df = pd.DataFrame()

    ### Set the schema to read the file
    # Set all columns to string by default
    file_schema = {col: str for col in columns} if columns else {}

    # If a specific schema is defined in Source_Schemas, use it
    if hasattr(Source_Schemas, file_name + "_schema"):
        schema = getattr(Source_Schemas, file_name + "_schema")
        for col in columns if columns else []:
            if col in schema:
                file_schema[col] = schema[col]

    # Concatenate ZMECON files if necessary
    if file_name == "zmecon":
        file_df = pd.concat([get_file("zmecon1", columns, skip_cache), get_file("zmecon2", columns, skip_cache)], ignore_index=True)
    elif file_name == "dfkkop":
        # Concatenate DFKKOP files if necessary
        file_df = pd.concat([get_file("dfkkop1", columns, skip_cache), get_file("dfkkop2", columns, skip_cache), get_file("dfkkop3", columns, skip_cache),
                             get_file("dfkkop4", columns, skip_cache), get_file("dfkkop5", columns, skip_cache), get_file("dfkkop6", columns, skip_cache),
                             get_file("dfkkop7", columns, skip_cache), get_file("dfkkop8", columns, skip_cache), get_file("dfkkop9", columns, skip_cache),
                             get_file("dfkkop10", columns, skip_cache)], ignore_index=True)
    elif file_paths[file_name].upper().endswith(".XLSX"):
       file_df = pd.read_excel(file_paths[ file_name ], usecols=columns, dtype=file_schema )
    elif file_paths[file_name].upper().endswith(".CSV"):
       file_df = pd.read_csv(file_paths[ file_name ], usecols=columns, encoding='utf-8', dtype=file_schema)
       log_info(f"Loaded {file_name} file. Records: " + str(len(file_df)))
    else:
        log_error(f"Unsupported file format for {file_name}. Supported formats are .xlsx and .csv.")

    # Set the column schema if available
    if hasattr(Source_Schemas, file_name + "_schema"):
        schema = getattr(Source_Schemas, file_name + "_schema")
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

    return file_df

def get_file( file_name, columns=None, skip_cache=False ):
    """
    Retrieves a DataFrame for a specified file, optionally selecting specific columns.
    Checks if there is a parquet version of the file first, and if not, reads from the Excel file.
    Creates a parquest file if it does not exist.
    
    :param file_name: Name of the file to retrieve (e.g., 'active', 'erdk', etc.)
    :param columns: List of columns to select from the DataFrame (optional)
    :param skip_cache: If True, skips the cache and reads directly from the source file (default is False)
    :return: DataFrame containing the data from the specified file
    """
    # Check if the file exists in the file cache directory
    cache_file = cache_directory + file_name + ".parquet"
    if file_name in file_paths and pd.io.common.file_exists(cache_file) and not skip_cache:
        df = pd.read_parquet(cache_file)    
        log_info(f"Loaded {file_name} from cache. Records: " + str(len(df)))
        if columns:
            if set(columns).issubset(df.columns):
                df = df[columns]
                return df
            else:
                log_info(f"Columns {columns} not found in {file_name} cache.")
        else:
            return df
    
    # If not in cache, read from the source file
    log_info(f"Loading {file_name} from source file.")
    
    if file_name not in file_paths:
        raise ValueError(f"File '{file_name}' not found in file paths.")
    
    df = read_file(file_name, columns, skip_cache)

    log_info(f"Loaded {file_name} file. Records: " + str(len(df)))
    # Save to cache directory as a parquet file
    df = prepare_dataframe_to_parquet(df) 
    df.to_parquet(cache_file, index=False)
    log_info(f"Saved {file_name} to cache as parquet. Records: " + str(len(df)))

    # If specific columns are requested, filter the DataFrame
    if columns:
        df = df[columns]
    
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