
import pandas as pd
import csv

# File paths
file_path1 = r"C:\Users\US97684\Downloads\documents_20250317_New\Premise_clean_final.xlsx"
config_path = r"C:\Users\US97684\Downloads\documents_20250317_New\Configuration.xlsx"
output_path = r"C:\Users\US97684\Downloads\documents_20250317_New\Outbound\STAGE_STREETS.csv"

# Load data
df_Premise = pd.read_excel(file_path1, sheet_name='Clean_Data', engine='openpyxl')
sheet1 = pd.read_excel(config_path, sheet_name='Street Abbreviation', engine='openpyxl')

# Initialize new DataFrame
df_new = pd.DataFrame()

# Direction mapping
direction_map = { 
    'N': 'N', 'S': 'S', 'E': 'E', 'W': 'W',
    'NE': 'NE', 'SE': 'SE', 'SW': 'SW', 'NW': 'NW',
    'NORTH': 'N', 'SOUTH': 'S', 'EAST': 'E', 'WEST': 'W',
    'NORTHEAST': 'NE', 'SOUTHEAST': 'SE', 'SOUTHWEST': 'SW', 'NORTHWEST': 'NW'  
}

# Build columns
df_new['FULLNAME'] = ""
df_new['PREDIRECTION'] = df_Premise.iloc[:, 4]
df_new['PROPERNAME'] = df_Premise.iloc[:, 5]
df_new['ABBREVIATION'] = df_Premise.iloc[:, 6]
df_new['POSTDIRECTION'] = df_Premise.iloc[:, 7]

# Normalize directions
df_new['PREDIRECTION'] = df_new['PREDIRECTION'].map(direction_map).fillna(df_new['PREDIRECTION'])
df_new['POSTDIRECTION'] = df_new['POSTDIRECTION'].map(direction_map).fillna(df_new['POSTDIRECTION'])

# Abbreviation mapping
abbreviation_lookup = dict(zip(sheet1.iloc[:, 0], sheet1.iloc[:, 1]))
df_new['ABBREVIATION'] = df_new['ABBREVIATION'].map(abbreviation_lookup).fillna(df_new['ABBREVIATION'])

# Compose FULLNAME
df_new['FULLNAME'] = (
    df_new['PREDIRECTION'].fillna(" ") + " " + 
    df_new['PROPERNAME'].fillna(" ") + " " + 
    df_new['ABBREVIATION'].fillna(" ") + " " + 
    df_new['POSTDIRECTION'].fillna(" ")
).str.strip()

# Drop rows missing required fields
required_columns = ['FULLNAME', 'PROPERNAME']
df_new = df_new.dropna(subset=required_columns)

# Drop duplicates
df_new = df_new.drop_duplicates(subset='FULLNAME')

# Replace NaNs with blanks
df_new = df_new.fillna("")

# Add trailer row
trailer_row = pd.DataFrame([['TRAILER'] + [''] * (len(df_new.columns) - 1)], columns=df_new.columns)
df_new = pd.concat([df_new, trailer_row], ignore_index=True)

# Custom quoting
def custom_quote(val, column):
    return f'"{val}"' if val not in ["", None] else val

df_new = df_new.apply(lambda col: col.apply(lambda val: custom_quote(val, col.name)))

# Export to CSV
df_new.to_csv(output_path, index=False, quoting=csv.QUOTE_NONE, escapechar='\\')

print(f"File successfully saved to: {output_path}")
