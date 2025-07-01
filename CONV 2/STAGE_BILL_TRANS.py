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

# ✅ File paths
file_paths = [
    r"C:\Users\US82783\OneDrive - Grant Thornton LLP\Desktop\python\conv 2\Bill\Bill Transactions CSV.xlsx"
]

rows = []

# ✅ Read & Process each file
for file_path in file_paths:
    print(f"\nLoading: {file_path}")
    df = pd.read_excel(file_path, sheet_name='ZMECON', engine='openpyxl')
    print(f"Loaded {len(df)} rows.")

    for idx, row in df.iterrows():
        customer_id = str(row.iloc[0])[:15]
        location_id = str(row.iloc[25])[:15]
        meternumber = str(row.iloc[20])[:20]
        rows.append({
            "CUSTOMERID": customer_id,
            "LOCATIONID": location_id,
            "CHARGECODE": "ServCharge",
            "CHARGEDESC": "",
            "BILLAMOUNT": round(float(row.iloc[8]), 2),
            "METERNUMBER": meternumber,
            "BILLCONSUMPTION":""
        })

        usecharges_amount = round(float(row.iloc[[9, 11, 13, 15]].sum()),2)
        rows.append({
            "CUSTOMERID": customer_id,
            "LOCATIONID": location_id,
            "CHARGECODE": "UseCharges",
            "CHARGEDESC": "",
            "BILLAMOUNT": usecharges_amount,
            "METERNUMBER": meternumber,
            "BILLCONSUMPTION": round(float(row.iloc[21]), 3)
        })

        rows.append({
            "CUSTOMERID": customer_id,
            "LOCATIONID": location_id,
            "CHARGECODE": "SaleTax",
            "CHARGEDESC": "",
            "BILLAMOUNT": round(float(row.iloc[17]),2),
            "METERNUMBER": meternumber,
            "BILLCONSUMPTION": ""
        })

print(f"\nTotal processed rows: {len(rows)}")

# ✅ Final DataFrame
df_new = pd.DataFrame(rows)

# ✅ Add trailer row
trailer_row = pd.DataFrame([{
    "CUSTOMERID": "TRAILER",
    "LOCATIONID": "MF",
    "CHARGECODE": "",
    "CHARGEDESC": "",
    "BILLAMOUNT": "",
    "BILLCONSUMPTION": ""
}])
df_new = pd.concat([df_new, trailer_row], ignore_index=True)

# ✅ Output paths
output_dir = r"C:\Users\US82783\OneDrive - Grant Thornton LLP\Desktop\python\conv 2\Bill"
main_output_path = os.path.join(output_dir, "BillTrans2.csv")

numeric_columns = [
    'BILLAMOUNT', 'BILLCONSUMPTION'
]

def custom_quote(val, column):
    # Check if the column is in the list of numeric columns
    if column in numeric_columns:
        return val  # No quotes for numeric fields
    # Otherwise, add quotes for non-numeric fields
    return f'"{val}"' if val not in ["", None] else val

df_new = df_new.apply(lambda col: col.apply(lambda val: custom_quote(val, col.name)))
df_new.to_csv(main_output_path, index=False, header=True, quoting=csv.QUOTE_NONE)
print(f"\nFull CSV file saved at: {main_output_path}")
