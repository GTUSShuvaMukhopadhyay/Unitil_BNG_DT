import pandas as pd
import os
import csv

# ✅ Checklist
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

print_checklist()

# ✅ File path
file_path = r"C:\Users\US82783\OneDrive - Grant Thornton LLP\Desktop\python\conv 2\Unbilled\Bill Parallel - Input_Output Files.xlsx"

print(f"\n🔄 Loading file: {file_path}")
df_Prem = pd.read_excel(file_path, sheet_name='ZDM_PREMDETAILS', engine='openpyxl')
print(f"✅ Loaded ZDM_PREMDETAILS with {len(df_Prem)} rows.")

df_EABL_After = pd.read_excel(file_path, sheet_name='EABL - After Conv', engine='openpyxl')
print(f"✅ Loaded EABL - After Conv with {len(df_EABL_After)} rows.")

df_EABL_Conv = pd.read_excel(file_path, sheet_name='EABL - Conv', engine='openpyxl')
print(f"✅ Loaded EABL - Conv with {len(df_EABL_Conv)} rows.")

# Output storage
output_rows = []

print(f"\n🔁 Processing {len(df_EABL_After)} rows from EABL - After Conv...")
for i in range(len(df_EABL_After)):
    INSTALLATION = df_EABL_After.iloc[i, 3]
    METERNUMBER = df_EABL_After.iloc[i, 6]
    CURRREADDATE = df_EABL_After.iloc[i, 4]
    CURRREADING = round(float(df_EABL_After.iloc[i, 8]),3)
    match_index = df_Prem[df_Prem.iloc[:, 3] == INSTALLATION].index
    conv_match_index = df_EABL_Conv[df_EABL_Conv.iloc[:, 3] == INSTALLATION].index

    if not match_index.empty:
        matched_row = match_index[0]
        CUSTOMERID = int(df_Prem.iloc[matched_row, 7])
        LOCATIONID = df_Prem.iloc[matched_row, 2]
        METERMULTIPLIER = round(float(df_Prem.iloc[matched_row, 22]),6)
        APPLICATION = "5"
        METERREGISTER = "1"
        READINGCODE = "2"
        READINGTYPE = "0"
        UNITOFMEASURE = "CF"
        READERID = ""
        UPDATEDATE = ""

    if not conv_match_index.empty:
        PREVREADDATE = df_EABL_Conv.iloc[conv_match_index[0], 4]
        PREVREADING = round(float(df_EABL_Conv.iloc[conv_match_index[0], 8]),3)
        RAWUSAGE = round(float(CURRREADING),3) - round(float(PREVREADING),3)
        BILLINGUSAGE = float(RAWUSAGE) * float(METERMULTIPLIER)
        output_rows.append([
            CUSTOMERID, LOCATIONID, APPLICATION, METERNUMBER, METERREGISTER, READINGCODE, READINGTYPE,
            CURRREADDATE, PREVREADDATE, CURRREADING, PREVREADING, UNITOFMEASURE,
            RAWUSAGE, BILLINGUSAGE, METERMULTIPLIER, READERID, UPDATEDATE
        ])

# Define final columns and numeric ones
columns = [
    "CUSTOMERID", "LOCATIONID", "APPLICATION", "METERNUMBER", "METERREGISTER",
    "READINGCODE", "READINGTYPE", "CURRREADDATE", "PREVREADDATE", "CURRREADING",
    "PREVREADING", "UNITOFMEASURE", "RAWUSAGE", "BILLINGUSAGE",
    "METERMULTIPLIER", "READERID", "UPDATEDATE"
]


numeric_columns = [
    "APPLICATION", "METERREGISTER", "READINGCODE", "READINGTYPE",
    "CURRREADING", "PREVREADING", "RAWUSAGE", "BILLINGUSAGE", "METERMULTIPLIER"
]

# Create DataFrame
df_output = pd.DataFrame(output_rows, columns=columns)

# Convert datetime to YYYY-MM-DD
for col in ["CURRREADDATE", "PREVREADDATE", "UPDATEDATE"]:
    df_output[col] = pd.to_datetime(df_output[col], errors='coerce').dt.date

# Add double quotes to non-numeric fields
def custom_quote(val, colname):
    if pd.isna(val):
        return ""
    if colname in numeric_columns:
        return val
    return f'"{val}"'

df_output = df_output.apply(lambda col: col.apply(lambda val: custom_quote(val, col.name)))

trailer_row = pd.DataFrame([['TRAILER'] + [''] * (len(df_output.columns) - 1)], columns=df_output.columns)
df_output = pd.concat([df_output, trailer_row], ignore_index=True)

# Save to CSV
output_path = os.path.splitext(file_path)[0] + "_output1.csv"
df_output.to_csv(output_path, index=False, header=True, quoting=csv.QUOTE_NONE)
print(f"\n📁 Output saved to: {output_path}")
