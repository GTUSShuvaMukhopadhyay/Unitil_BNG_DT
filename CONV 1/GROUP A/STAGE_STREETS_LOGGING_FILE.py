#STREETS_LOGGING_FILE

import pandas as pd
import re
import csv
import os
import logging
from datetime import datetime

def setup_logging(input_path):
    """
    Set up logging configuration
    
    Args:
        input_path: Path to the input file, used to determine log directory
    
    Returns:
        log_file: Path to the created log file
    """
    # Create logs directory in the same folder as the input file
    input_dir = os.path.dirname(input_path)
    logs_dir = os.path.join(input_dir, 'logs')
    
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    
    # Set up logging with timestamp in filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(logs_dir, f"address_processing_{timestamp}.log")
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()  # Also output to console
        ]
    )
    
    logging.info("Logging initialized")
    logging.info(f"Log file location: {log_file}")
    return log_file

def main():
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

    print("CSV Staging File Validation Checklist:")
    for item in CHECKLIST:
        print(item)
    print("\n")
    
    # File paths for input and output
    input_path = r"C:\Users\us85360\Desktop\STAGE_STREETS\ZDM_PREMDETAILS.XLSX"
    
    # Define output path and log file directories to be in the same folder as input file
    input_dir = os.path.dirname(input_path)
    output_path = os.path.join(input_dir, 'GT_STAGE_STREETS.csv')
    
    # Set up logging in the same folder as the input file
    log_file = setup_logging(input_path)
    
    logging.info(f"Processing premise data from: {input_path}")
    logging.info(f"Output will be saved to: {output_path}")
    
    try:
        # Load the ZDM_PREMDETAILS data
        logging.info("Loading input Excel file...")
        df_premise = pd.read_excel(input_path, sheet_name='Sheet1', engine='openpyxl')
        logging.info(f"Successfully loaded {len(df_premise)} records from input file")
        
        # Define street type mappings (full names to abbreviations)
        street_type_map = {
            'STREET': 'ST',
            'AVENUE': 'AVE',
            'BOULEVARD': 'BLVD',
            'COURT': 'CT',
            'DRIVE': 'DR',
            'LANE': 'LN',
            'PLACE': 'PL',
            'ROAD': 'RD',
            'TERRACE': 'TER',
            'CIRCLE': 'CIR',
            'WAY': 'WAY',
            'HIGHWAY': 'HWY',
            'PARKWAY': 'PKWY',
            'EXPRESSWAY': 'EXPY',
            'TRAIL': 'TRL',
            'SQUARE': 'SQ',
            'POINT': 'PT',
            'MOUNT': 'MT',
            'JUNCTION': 'JCT',
            'EXTENSION': 'EXT',
            'TURNPIKE': 'TPKE'
        }
        
        # Define direction mappings
        direction_map = { 
            'N': 'N',
            'S': 'S',
            'E': 'E',
            'W': 'W',
            'NE': 'NE',
            'SE': 'SE',
            'SW': 'SW',
            'NW': 'NW',  
            'NORTH': 'N',
            'SOUTH': 'S',
            'EAST': 'E',
            'WEST': 'W',
            'NORTHEAST': 'NE',
            'SOUTHEAST': 'SE',
            'SOUTHWEST': 'SW',
            'NORTHWEST': 'NW'  
        }
        
        # Create a new DataFrame for the output
        df_streets = pd.DataFrame()
        
        # Parse addresses and extract components
        logging.info("Parsing addresses and extracting components...")
        
        # Create a list to track problematic addresses
        problematic_addresses = []
        
        # Function to parse and track problematic addresses
        def parse_and_track(addr, idx):
            result = parse_address(addr, street_type_map, direction_map)
            
            # Check for potential issues
            if isinstance(addr, str):
                # Track if address parsing seems incomplete
                if not result['properName'] or not result['fullname']:
                    problematic_addresses.append({
                        'index': idx,
                        'address': addr,
                        'reason': 'Missing proper name or full name',
                        'parsed_result': str(result)  # Convert to string for CSV storage
                    })
                # Track if address has unusual format
                elif ',' not in addr:
                    problematic_addresses.append({
                        'index': idx,
                        'address': addr,
                        'reason': 'Unusual format - no city delimiter',
                        'parsed_result': str(result)
                    })
                # Track if address has special characters that might indicate complex format
                elif any(char in addr for char in ['#', '&', '/', '\\']):
                    problematic_addresses.append({
                        'index': idx,
                        'address': addr,
                        'reason': 'Contains special characters',
                        'parsed_result': str(result)
                    })
            
            return result
        
        # Extract address components
        logging.info("Extracting address components...")
        address_components = df_premise['Service Address'].apply(
            lambda addr: parse_and_track(addr, df_premise.index[df_premise['Service Address'] == addr][0] 
                                        if isinstance(addr, str) and addr in df_premise['Service Address'].values 
                                        else -1)
        )
        
        # Add components to the dataframe
        df_streets['FULLNAME'] = address_components.apply(lambda x: x.get('fullname', ''))
        df_streets['PREDIRECTION'] = address_components.apply(lambda x: x.get('preDirection', ''))
        df_streets['PROPERNAME'] = address_components.apply(lambda x: x.get('properName', ''))
        df_streets['ABBREVIATION'] = address_components.apply(lambda x: x.get('abbreviation', ''))
        df_streets['POSTDIRECTION'] = address_components.apply(lambda x: x.get('postDirection', ''))
        
        # Log problematic addresses
        if problematic_addresses:
            logging.warning(f"Found {len(problematic_addresses)} problematic addresses")
            
            # Log details about problematic addresses
            logging.info("Writing problematic addresses to CSV...")
            problem_df = pd.DataFrame(problematic_addresses)
            problem_file = os.path.join(input_dir, "logs", "problematic_addresses.csv")
            problem_df.to_csv(problem_file, index=False)
            logging.info(f"Problematic addresses saved to {problem_file}")
            
            # Log a sample of problematic addresses
            sample_size = min(5, len(problematic_addresses))
            logging.info(f"Sample of problematic addresses (showing {sample_size}):")
            for i in range(sample_size):
                addr = problematic_addresses[i]
                logging.info(f"  - Index {addr['index']}: {addr['address']} - Reason: {addr['reason']}")
        else:
            logging.info("No problematic addresses found.")
        
        # Remove rows with missing required fields
        logging.info("Removing rows with missing required fields...")
        before_count = len(df_streets)
        required_columns = ['FULLNAME', 'PROPERNAME']
        df_streets = df_streets.dropna(subset=required_columns)
        after_count = len(df_streets)
        logging.info(f"Removed {before_count - after_count} rows with missing required fields")
        
        # Remove duplicates based on FULLNAME to ensure uniqueness
        logging.info("Removing duplicates based on FULLNAME...")
        before_count = len(df_streets)
        df_streets = df_streets.drop_duplicates(subset='FULLNAME')
        after_count = len(df_streets)
        logging.info(f"Removed {before_count - after_count} duplicate FULLNAME entries")
        
        # Ensure all values are strings
        for col in df_streets.columns:
            df_streets[col] = df_streets[col].fillna('')
        
        # Enforce field length limits from the mapping
        df_streets['FULLNAME'] = df_streets['FULLNAME'].str.slice(0, 45)
        df_streets['PREDIRECTION'] = df_streets['PREDIRECTION'].str.slice(0, 2)
        df_streets['PROPERNAME'] = df_streets['PROPERNAME'].str.slice(0, 24)
        df_streets['ABBREVIATION'] = df_streets['ABBREVIATION'].str.slice(0, 5)
        df_streets['POSTDIRECTION'] = df_streets['POSTDIRECTION'].str.slice(0, 2)
        
        # Add a trailer row with default values
        logging.info("Adding trailer row...")
        trailer_row = pd.DataFrame([['TRAILER'] + [''] * (len(df_streets.columns) - 1)], 
                                columns=df_streets.columns)
        
        # Append the trailer row to the DataFrame
        df_streets = pd.concat([df_streets, trailer_row], ignore_index=True)
        
        # Apply proper quoting to all values
        df_streets = df_streets.apply(lambda col: col.apply(lambda val: custom_quote(val)))
        
        # Save to CSV with escape character set
        logging.info(f"Saving output file to: {output_path}")
        df_streets.to_csv(output_path, index=False, header=True, quoting=csv.QUOTE_NONE, escapechar='\\')
        
        # Log summary statistics
        record_count = len(df_streets) - 1  # -1 for trailer row
        logging.info(f"Processing complete. Total valid records: {record_count}")
        logging.info(f"File successfully saved to: {output_path}")
        
        # Write summary to a separate file
        summary_file = os.path.join(input_dir, "logs", "processing_summary.txt")
        with open(summary_file, 'w') as f:
            f.write(f"Processing Summary\n")
            f.write(f"=================\n")
            f.write(f"Date/Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Input File: {input_path}\n")
            f.write(f"Output File: {output_path}\n")
            f.write(f"Log File: {log_file}\n\n")
            f.write(f"Total Records Processed: {len(df_premise)}\n")
            f.write(f"Valid Records Output: {record_count}\n")
            f.write(f"Problematic Addresses: {len(problematic_addresses)}\n")
            if problematic_addresses:
                f.write(f"Problematic Address Details: {problem_file}\n")
        
        logging.info(f"Processing summary saved to {summary_file}")
        
    except Exception as e:
        logging.error(f"An error occurred during processing: {str(e)}")
        logging.exception("Exception details:")
        print(f"Error: {str(e)}")
        print("Check the log file for details.")


def parse_address(address, street_type_map, direction_map):
    """
    Parse an address string and extract the components needed for enQuestra.
    
    Args:
        address: The street address string
        street_type_map: Dictionary mapping street type names to abbreviations
        direction_map: Dictionary mapping direction names to abbreviations
        
    Returns:
        Dictionary with components: fullname, preDirection, properName, abbreviation, postDirection
    """
    # Log entry for debugging individual address parsing
    if logging.getLogger().isEnabledFor(logging.DEBUG):
        logging.debug(f"Parsing address: {address}")
    
    if not isinstance(address, str):
        return {
            'fullname': '',
            'preDirection': '',
            'properName': '',
            'abbreviation': '',
            'postDirection': ''
        }
    
    # Remove the city part (before the comma)
    parts = address.split(',')
    if len(parts) != 2:
        return {
            'fullname': address.strip(),
            'preDirection': '',
            'properName': address.strip(),
            'abbreviation': '',
            'postDirection': ''
        }
    
    # Get just the street part after the comma
    street_part = parts[1].strip()
    
    # Remove any additional information after a dash
    if ' - ' in street_part:
        street_part = street_part.split(' - ')[0].strip()
    
    # Remove the house number at the beginning
    number_match = re.match(r'^\d+[A-Za-z]?', street_part)
    if number_match:
        street_part = street_part[len(number_match.group(0)):].strip()
    
    # Also recognize the abbreviations directly
    known_abbreviations = set(street_type_map.values())
    
    # Initialize components
    pre_direction = ''
    street_name = ''
    street_type = ''
    post_direction = ''
    
    # Check for pre-direction at the beginning
    for dir_full, dir_abbr in direction_map.items():
        if street_part.upper().startswith(dir_full + ' '):
            pre_direction = dir_abbr
            street_part = street_part[len(dir_full):].strip()
            break
    
    # Check for post-direction at the end
    for dir_full, dir_abbr in direction_map.items():
        if street_part.upper().endswith(' ' + dir_full):
            post_direction = dir_abbr
            street_part = street_part[:-(len(dir_full) + 1)].strip()
            break
    
    # Check for street type - first try full names, then abbreviations
    found_type = False
    
    # Check for full street type names
    for type_full, type_abbr in street_type_map.items():
        pattern = r'\s{}$'.format(type_full)
        if re.search(pattern, street_part, re.IGNORECASE):
            street_type = type_abbr
            street_part = re.sub(pattern, '', street_part, flags=re.IGNORECASE).strip()
            found_type = True
            break
    
    # If no full name found, check for abbreviations
    if not found_type:
        for type_abbr in known_abbreviations:
            pattern = r'\s{}$'.format(type_abbr)
            if re.search(pattern, street_part, re.IGNORECASE):
                street_type = type_abbr
                street_part = re.sub(pattern, '', street_part, flags=re.IGNORECASE).strip()
                break
    
    # What's left is the street name
    street_name = street_part
    
    # Construct the full name according to enQuestra requirements
    components = []
    if pre_direction:
        components.append(pre_direction)
    if street_name:
        components.append(street_name)
    if street_type:
        components.append(street_type)
    if post_direction:
        components.append(post_direction)
    
    fullname = ' '.join(components)
    
    # Debug logging for parsed components
    if logging.getLogger().isEnabledFor(logging.DEBUG):
        logging.debug(f"Parsed components: pre_dir={pre_direction}, name={street_name}, " +
                     f"type={street_type}, post_dir={post_direction}, full={fullname}")
    
    return {
        'fullname': fullname,
        'preDirection': pre_direction,
        'properName': street_name,
        'abbreviation': street_type,
        'postDirection': post_direction
    }


def custom_quote(val):
    """
    Apply custom quoting to values
    """
    if val not in ["", None]:
        return f'"{val}"'
    return val


if __name__ == "__main__":
    main()