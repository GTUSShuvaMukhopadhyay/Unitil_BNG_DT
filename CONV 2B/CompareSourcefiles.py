import pandas as pd
import os

def load_file(filepath):
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
    
    ext = os.path.splitext(filepath)[1].lower()

    if ext == '.csv':
        return pd.read_csv(filepath)
    elif ext == '.xlsx':
        return pd.read_excel(filepath)
    else:
        raise ValueError(f"Unsupported file format: {filepath}")

def compare_headers(file1_path, file2_path):
    df1 = load_file(file1_path)
    df2 = load_file(file2_path)

    cols1 = list(df1.columns)
    cols2 = list(df2.columns)

    print("=== Column Comparison Report ===")
    print(f"\nFile 1: {file1_path}")
    print(f"File 2: {file2_path}\n")

    # Side-by-side comparison
    max_len = max(len(cols1), len(cols2))
    cols1 += [''] * (max_len - len(cols1))
    cols2 += [''] * (max_len - len(cols2))

    comparison_df = pd.DataFrame({
        'File 1 Columns': cols1,
        'File 2 Columns': cols2
    })

    print("📋 Side-by-Side Column Comparison:\n")
    print(comparison_df.to_string(index=True))

    # Unique and common columns
    only_in_file1 = [col for col in df1.columns if col not in df2.columns]
    only_in_file2 = [col for col in df2.columns if col not in df1.columns]
    common_cols = [col for col in df1.columns if col in df2.columns]

    print("\n🔍 Differences:")

    if only_in_file1:
        print("\n🔴 Columns only in File 1:")
        for col in only_in_file1:
            print(f"  - {col}")
    else:
        print("✅ No extra columns in File 1")

    if only_in_file2:
        print("\n🔴 Columns only in File 2:")
        for col in only_in_file2:
            print(f"  - {col}")
    else:
        print("✅ No extra columns in File 2")

    print("\n🔍 Checking index positions for common columns:")
    mismatch_found = False
    for col in common_cols:
        idx1 = list(df1.columns).index(col)
        idx2 = list(df2.columns).index(col)
        if idx1 != idx2:
            mismatch_found = True
            print(f"⚠️  Column '{col}' at index {idx1} in File 1, index {idx2} in File 2")

    if not mismatch_found:
        print("✅ All common columns are in the same order")

if __name__ == "__main__":
    file1_path = r"C:\Users\US82783\Downloads\ZMECON 01012022 TO 12312024.XLSX"
    file2_path = r"C:\Users\US82783\OneDrive - Grant Thornton Advisors LLC\Desktop\python\CONV 2B _ 2nd run\DATA SOURCES\ZMECON 010115 to 12312020.xlsx"
    try:
        compare_headers(file1_path, file2_path)
    except Exception as e:
        print(f"\n❌ Error: {e}")
