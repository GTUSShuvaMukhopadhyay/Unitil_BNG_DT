import pandas as pd
import os
import csv
import Conversion_Utils as cu

# Setup logging
import sys
sys.stdout.reconfigure(encoding='utf-8')

# ✅ Print validation checklist
cu.print_checklist()

# 📂 Load source files
print('Loading source files...')
df_writeoff = cu.get_file("writeoff")
df_zmecon = cu.get_file("zmecon")
df_dfkkop = cu.get_file("dfkkop")

print(f"✅ Loaded ZWRITEOFF: {len(df_writeoff)} rows")
print(f"✅ Loaded ZMECON: {len(df_zmecon)} rows")
print(f"✅ Loaded DFKKOP: {len(df_dfkkop)} rows")

# 📤 Filter DFKKOP where column 10 (Status) is blank
df_filtered = df_dfkkop[df_dfkkop.iloc[:, 10].isna()]
print(f"✅ Filtered DFKKOP rows with blank status: {len(df_filtered)}")

# ⚙️ Load Configuration file
config_path = r"C:\\Users\\US82783\\OneDrive - Grant Thornton Advisors LLC\\Desktop\\python\\CONV 2B _ 2nd run\\DATA SOURCES\\Configuration 13.xlsx"
df_Config = pd.read_excel(config_path, sheet_name='RateCode', engine='openpyxl')
print(f"✅ Loaded Configuration RateCode: {len(df_Config)} rows")

# 🔧 Helpers
def normalize(val):
    try:
        return str(int(float(val))).zfill(4)
    except:
        return str(val).strip().upper()

def clean_id(val):
    try:
        return str(int(float(val))).strip()
    except:
        return None

# 🧹 Normalize Configuration
df_Config['Rate Category Norm'] = df_Config.iloc[:, 0].astype(str).str.strip().str.upper()
df_Config['MTrans Norm'] = df_Config.iloc[:, 1].apply(normalize)
df_Config['STrans Norm'] = df_Config.iloc[:, 2].apply(normalize)

# 🔁 Build ZMECON mapping
df_zmecon['CUSTOMERID_CLEAN'] = df_zmecon.iloc[:, 0].apply(clean_id)
df_zmecon['RATE'] = df_zmecon.iloc[:, 24].astype(str).str.strip().str.upper()
df_zmecon['LOCATION'] = df_zmecon.iloc[:, 25]
df_zmecon_clean = df_zmecon.dropna(subset=['CUSTOMERID_CLEAN']).drop_duplicates(subset=['CUSTOMERID_CLEAN'])
zmecon_full_mapping = df_zmecon_clean.set_index('CUSTOMERID_CLEAN')[['RATE', 'LOCATION']].to_dict('index')

# 🔁 Build DFKKOP mapping
dfkkop_mapping = dict(
    zip(
        df_filtered.iloc[:, 1].apply(clean_id),
        zip(
            df_filtered.iloc[:, 4].apply(normalize),
            df_filtered.iloc[:, 5].apply(normalize)
        )
    )
)

# 🧪 Show sample dfkkop_mapping
print("\n🔍 Sample dfkkop_mapping entries:")
for k, v in list(dfkkop_mapping.items())[:10]:
    print(f"CUSTOMERID: {k}, MTrans: {v[0]}, STrans: {v[1]}")

# 📦 Helper functions
def get_zmecon_data(cust_id):
    return zmecon_full_mapping.get(clean_id(cust_id), {})

def get_rate_from_zmecon(cust_id):
    return get_zmecon_data(cust_id).get('RATE', None)

def get_location_from_zmecon(cust_id):
    return get_zmecon_data(cust_id).get('LOCATION', "")

def get_iloc3_from_config(rate, mtrans, strans):
    if not rate or not mtrans or not strans:
        return None
    match = df_Config[
        (df_Config['Rate Category Norm'] == str(rate).strip().upper()) &
        (df_Config['MTrans Norm'] == normalize(mtrans)) &
        (df_Config['STrans Norm'] == normalize(strans))
    ]
    if not match.empty:
        return match.iloc[0, 3]
    return None

# 🛠️ Build Output DataFrame
df_new = pd.DataFrame()
df_new['CUSTOMERID'] = df_writeoff.iloc[:, 0].apply(lambda x: str(int(float(x))) if pd.notna(x) and str(x).replace('.', '', 1).isdigit() else x)
df_new['APPLICATION'] = 5
df_new['CHARGEDATE'] = pd.to_datetime(df_writeoff.iloc[:, 2], errors='coerce').dt.strftime("%Y-%m-%d")
df_new['WRITEOFFDATE'] = df_new['CHARGEDATE']
df_new['WRITEOFFAMOUNT'] = df_writeoff.iloc[:, 3]
df_new['AMOUNTREMAINING'] = df_writeoff.iloc[:, 3]
df_new['UPDATEDATE'] = ""

# 🔍 Compute LOCATIONID & RECEIVABLECODE
location_ids = []
codes = []

for cust_id in df_new['CUSTOMERID']:
    cust_id_str = clean_id(cust_id)
    zmecon_data = get_zmecon_data(cust_id)
    location = zmecon_data.get('LOCATION', "")
    rate = zmecon_data.get('RATE', None)
    mtrans, strans = dfkkop_mapping.get(cust_id_str, (None, None))
    code = get_iloc3_from_config(rate, mtrans, strans)

    if code is None:
        code = 8098  # default fallback

    location_ids.append(location)
    codes.append(int(code))

df_new['LOCATIONID'] = pd.Series(location_ids).apply(lambda x: str(int(float(x))) if pd.notna(x) and str(x).replace('.', '', 1).isdigit() else x)
df_new['RECEIVABLECODE'] = codes

# ✅ Reorder columns before export
df_new = df_new[['CUSTOMERID','LOCATIONID','APPLICATION','CHARGEDATE','WRITEOFFDATE','WRITEOFFAMOUNT','AMOUNTREMAINING','RECEIVABLECODE','UPDATEDATE']]

# ➕ Add trailer row
df_new = pd.concat(
    [df_new, pd.DataFrame([["TRAILER"] + [""] * (len(df_new.columns) - 1)], columns=df_new.columns)],
    ignore_index=True
)
cu.log_debug("✅ Trailer row added")

# 📤 Export to CSV
output_path = r"C:\Users\US82783\OneDrive - Grant Thornton Advisors LLC\Desktop\python\CONV 2B _ 2nd run\Extracts\STAGE_WRITEOFF.csv"
df_new.to_csv(output_path, index=False, quoting=csv.QUOTE_NONNUMERIC)
cu.log_info(f"✅ CSV file saved successfully at: {output_path}")
