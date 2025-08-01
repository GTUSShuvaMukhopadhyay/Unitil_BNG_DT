import pandas as pd
import os
import sys
import csv
import time
from datetime import datetime

 # Add the parent directory to sys.path
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
import Conversion_Utils as cu 

cu.print_checklist()

###########
# Output Column Definitions
#
# TAXYEAR	Numeric	4	N
# CUSTOMERID	Alpha	15	Y
# LOCATIONID	Alpha	15	Y
# TRANSACTIONDATE	Date	10	Y
# BILLINGDATE	Date	10	N
# DUEDATE	Date	10	N
# BILLORINVOICENUMBER	Numeric	8	N
# TRANSACTIONTYPE	Numeric	2	Y
# TRANSACTIONAMOUNT	Numeric	11,2	Y
# TRANSACTIONDESCRIPTION	Alpha	20	N
# APPLICATION	Numeric	1	Y
# BILLTYPE	Numeric	1	N
# TENDERTYPE	Numeric	2	N
# UPDATEDATE    " "

##########################
# Prepare ZMECON Data
df_zmecon  = cu.get_file("zmecon")
df_zmecon = df_zmecon[['Business Partner','Contract Account', 'Print Document No', 'Billing Key Date', 'Serv.Charge', 'Energy Charge', 'Past Gas Adj.Charges', 'Efficiency mine fund', 'Transp.Charge']]

########################
# Prepare DFKKOP Data
df_dfkkop = cu.get_file("dfkkop")
df_dfkkop = df_dfkkop[['BPartner', 'Cont.Account', 'Doc. Date', 'Pstng Date', 'Due', 'Reference', 'MTrans', 'STrans', 'Amount', 'Crcy', 'DT', 'Status']]
# Standardize STrans to 4-digit format with leading zeros
df_dfkkop["STrans"] = df_dfkkop["STrans"].apply(
    lambda x: "{:04d}".format(int(x)) if pd.notna(x) and isinstance(x, (int, float)) else str(x) if pd.notna(x) else x
)
df_dfkkop["STrans"] = df_dfkkop["STrans"].str.strip()

# Standardize MTrans to 4-digit format with leading zeros  
df_dfkkop["MTrans"] = df_dfkkop["MTrans"].apply(
    lambda x: "{:04d}".format(int(x)) if pd.notna(x) and isinstance(x, (int, float)) else str(x) if pd.notna(x) else x
)
df_dfkkop["MTrans"] = df_dfkkop["MTrans"].str.strip()

# Define the 19 valid combinations - Now both MTrans and STrans as standardized strings
# Removed 0100, 0002 as a valid combination
valid_combinations = {
    ("0015", "0010"), ("0015", "0020"), ("0015", "0021"), ("0015", "0030"),
    ("0015", "0040"), ("0015", "0070"), ("0015", "0230"), ("0015", "0231"),
    ("0015", "0300"), ("0015", "0370"), ("0015", "0371"), ("0025", "0010"),
    ("0070", "0010"), ("0080", "0005"), ("0080", "0010"), 
    ("0200", "0002"), ("0620", "0010"), ("0630", "0010")
}

# Apply filter - Now much simpler since formats are standardized
def check_valid_combination(row):
    try:
        return (row['MTrans'], row['STrans']) in valid_combinations
    except:
        return False

df_dfkkop = df_dfkkop[
    df_dfkkop.apply(check_valid_combination, axis=1)
]

#########
# Transformation logic goes here
#########
df_new = pd.DataFrame()

#########
# Write CSV File
##########

# Use QUOTE_NONNUMERIC to ensure all non-numeric fields (including dates) get quotes
cu.write_csv(df_new, "GROUP C/STAGE_TRANSACTIONAL_HIST.csv" )
