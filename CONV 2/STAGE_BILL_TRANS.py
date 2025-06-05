import pandas as pd
import os

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

        rows.append({
            "CUSTOMERID": customer_id,
            "LOCATIONID": location_id,
            "CHARGECODE": "ServCharge",
            "CHARGEDESC": "",
            "BILLAMOUNT": row.iloc[8],
            "BILLCONSUMPTION": ""
        })

        usecharges_amount = row.iloc[9:16].sum()
        rows.append({
            "CUSTOMERID": customer_id,
            "LOCATIONID": location_id,
            "CHARGECODE": "UseCharges",
            "CHARGEDESC": "",
            "BILLAMOUNT": usecharges_amount,
            "BILLCONSUMPTION": ""
        })

        rows.append({
            "CUSTOMERID": customer_id,
            "LOCATIONID": location_id,
            "CHARGECODE": "SaleTax",
            "CHARGEDESC": "",
            "BILLAMOUNT": row.iloc[17],
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
split_output_dir = os.path.join(output_dir, "SplitFiles2")
os.makedirs(split_output_dir, exist_ok=True)

# ✅ Save main full file
df_new.to_csv(main_output_path, index=False)
print(f"\nFull CSV file saved at: {main_output_path}")

# ✅ Split file logic
max_rows = 1000000
total_rows = len(df_new)
num_parts = (total_rows + max_rows - 1) // max_rows  # Ceiling division

print(f"Creating {num_parts} split file(s)...")

for i in range(num_parts):
    start = i * max_rows
    end = min(start + max_rows, total_rows)
    part_df = df_new.iloc[start:end]
    part_file_path = os.path.join(split_output_dir, f"BillTrans1_part{i+1}.csv")
    part_df.to_csv(part_file_path, index=False)
    print(f"Saved part {i+1}: rows {start} to {end - 1} ➜ {part_file_path}")
