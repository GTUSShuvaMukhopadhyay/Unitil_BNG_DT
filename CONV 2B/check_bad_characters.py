import pandas as pd
import re

# Step 1: Load CSV (skip bad lines)
file_path = r"C:\Users\US82783\OneDrive - Grant Thornton Advisors LLC\Desktop\python\CONV 2B _ 2nd run\Extracts\STAGE_CUST_NOTES.csv"
df = pd.read_csv(file_path, dtype=str, on_bad_lines="skip")  # Read all as string

# Step 2: Define emoji pattern and special character check
emoji_pattern = re.compile(
    "[" 
    "\U0001F600-\U0001F64F"  # emoticons
    "\U0001F300-\U0001F5FF"  # symbols & pictographs
    "\U0001F680-\U0001F6FF"  # transport & map symbols
    "\U0001F1E0-\U0001F1FF"  # flags
    "\U00002700-\U000027BF"  # dingbats
    "\U0001F900-\U0001F9FF"  # supplemental symbols
    "\U00002600-\U000026FF"  # misc symbols
    "]+", flags=re.UNICODE
)

def has_bad_characters(text):
    if pd.isna(text):
        return False
    text = str(text)
    return (
        bool(re.search(r'[^\x00-\x7F]', text)) or       # non-ASCII
        bool(emoji_pattern.search(text)) or             # emoji
        bool(re.search(r'[\x00-\x1F\x7F]', text))       # control characters
    )

# Step 3: Apply the function to each cell and flag rows
mask = df.applymap(has_bad_characters).any(axis=1)
bad_rows = df[mask]

# Step 4: Print offending rows
if not bad_rows.empty:
    print("🔍 Rows with bad characters, emojis, or control characters:\n")
    print(bad_rows.to_string(index=False))
else:
    print("✅ No bad characters found in the file.")
