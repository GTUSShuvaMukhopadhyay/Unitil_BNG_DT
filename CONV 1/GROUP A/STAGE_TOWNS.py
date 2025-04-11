import os
import pandas as pd
import csv

# Define input file path
file_path = r"C:\Users\US97684\Downloads\documents_20250219\ZDM_PREMDETAILS.xlsx"

# Load the main dataset
df = pd.read_excel(file_path, sheet_name='Sheet1', engine='openpyxl')

# Define county mapping directly in the code
data_county = {
    "Town": ["Alton", "Argyle TWP", "Bangor", "Brewer", "Bucksport", "Chester", "Edinburg", "Frankfort", "Hampden", 
              "Hermon", "Howland", "Lincoln", "Mattamascontis TWP", "Mattawamkeag", "Old Town", "Orono", "Orrington", 
              "Prospect", "Searsport", "Veazie", "Winterport", "Woodville"],
    "Zip Code": ["04468", "04468", "04401", "04412", "04416", "04457", "04448", "04438", "04444", "04401", 
                  "04448", "04457", "04457", "04459", "04468", "04473", "04474", "04981", "04974", "04401", 
                  "04496", "04457"],
    "County": ["Penobscot", "Penobscot", "Penobscot", "Penobscot", "Hancock", "Penobscot", "Penobscot", "Waldo", "Penobscot", 
                "Penobscot", "Penobscot", "Lincoln", "Penobscot", "Penobscot", "Penobscot", "Penobscot", "Penobscot", 
                "Waldo", "Waldo", "Penobscot", "Waldo", "Penobscot"]
}

df_county = pd.DataFrame(data_county)

# Standardize Town and Zip Code formatting in df_county
df_county["Town"] = df_county["Town"].str.strip().str.lower()
df_county["Zip Code"] = df_county["Zip Code"].str.strip()

# Create an empty DataFrame for STAGE_TOWNS
df_stage_towns = pd.DataFrame()

# Extract TOWN: Everything before the first comma in "Service Address"
df_stage_towns["TOWN"] = df["Service Address"].astype(str).str.split(",").str[0].str.strip().str.lower()

# Hardcode STATE as "ME"
df_stage_towns["STATE"] = "ME"

# Clean the ZIPCODE: Remove everything after the first 5 digits
df_stage_towns['ZIPCODE'] = df["Zip Code"].astype(str).str.extract(r'(\d{5})', expand=False)

# Merge with county reference table to get COUNTY values (preserving all rows from df_stage_towns)
df_stage_towns = df_stage_towns.merge(df_county, left_on=["TOWN", "ZIPCODE"], right_on=["Town", "Zip Code"], how="left")

df_stage_towns.drop(columns=["Town", "Zip Code"], inplace=True)  # Drop redundant columns

# Replace missing values in columns with default values
df_stage_towns["TOWN"] = df_stage_towns["TOWN"].fillna("Unknown").str.upper()
df_stage_towns["STATE"] = df_stage_towns["STATE"].fillna("ME").str.upper()
df_stage_towns["ZIPCODE"] = df_stage_towns["ZIPCODE"].fillna("00000").str.strip()  # Default ZIP if missing
df_stage_towns["County"] = df_stage_towns["County"].fillna("Unknown").replace("Unknown", "")

# Ensure all values are non-empty
df_stage_towns["TOWN"] = df_stage_towns["TOWN"].apply(lambda x: x if x != "" else "Unknown")
df_stage_towns["STATE"] = df_stage_towns["STATE"].apply(lambda x: x if x != "" else "ME")
df_stage_towns["ZIPCODE"] = df_stage_towns["ZIPCODE"].apply(lambda x: x if x != "" else "00000")
df_stage_towns["County"] = df_stage_towns["County"].apply(lambda x: x if x != "" else "Unknown")

# Drop rows where any of the columns contain 'Unknown' or '00000'
df_stage_towns = df_stage_towns[~df_stage_towns["TOWN"].isin(["Unknown"]) & 
                                ~df_stage_towns["STATE"].isin(["Unknown"]) &
                                ~df_stage_towns["ZIPCODE"].isin(["00000"]) & 
                                ~df_stage_towns["County"].isin(["Unknown"])]

# Apply quotes to all non-empty string values (add double quotes around text fields)
df_stage_towns = df_stage_towns.applymap(lambda x: f'"{x}"' if isinstance(x, str) and x != "" else x)

# Remove duplicates based on combination of Town, State, and Zip Code
df_stage_towns = df_stage_towns.drop_duplicates(subset=["TOWN", "STATE", "ZIPCODE"])

# Reorder the columns to match the required output order
df_stage_towns = df_stage_towns[["TOWN", "STATE", "County", "ZIPCODE"]]

# Rename "County" column to "COUNTY"
df_stage_towns = df_stage_towns.rename(columns={"County": "COUNTY"})

# Add trailer row
trailer_row = pd.DataFrame([['TRAILER'] + [''] * (len(df_stage_towns.columns) - 1)], columns=df_stage_towns.columns)
df_stage_towns = pd.concat([df_stage_towns, trailer_row], ignore_index=True)

# Dynamically define output path to avoid permission issues
output_dir = r"C:\Users\US97684\OneDrive - Grant Thornton LLP\Desktop\Python_file\unitil"  # Ensure this path is correct

# Check if output directory exists, create it if it doesn't
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

output_csv = os.path.join(output_dir, "STAGE_TOWNS.csv")

# Save to CSV with quoting enabled (using csv.QUOTE_NONE)
df_stage_towns.to_csv(output_csv, index=False, quoting=csv.QUOTE_NONE, escapechar='\\')  # quoting=csv.QUOTE_NONE disables default quoting

print(f"Data has been saved to: {output_csv}")

# Print diagnostics
total_rows = len(df_stage_towns) - 1  # Excluding trailer row
missing_county = df_stage_towns[df_stage_towns['COUNTY'] == ""]
print(f"Total rows in output: {total_rows}")
print(f"Rows with missing COUNTY: {len(missing_county)}")