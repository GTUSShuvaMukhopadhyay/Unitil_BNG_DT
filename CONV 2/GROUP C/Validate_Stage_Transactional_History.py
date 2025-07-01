###########
# Validate test cases for STAGE_TRANSACTIONAL_HISTORY
#
# Case 1:  Ensure the sum of amounts in DFKKOP by customer number matches the sum in STAGE_TRANSACTIONAL_HISTORY
# Case 2:  Validate the Tender Type is correct
#
##########

import pandas as pd
import csv
import time
from datetime import datetime

 # Add the parent directory to sys.path
import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
import Conversion_Utils as cu 

cu.print_checklist()

##########
# Test Case 1: Validate the sum of final billing amounts in DFKKOP by customer matches the sum in STAGE_TRANSACTIONAL_HISTORY
##########
df_dfkkop = cu.get_file("dfkkop", columns=["BPartner", "Amount", "MTrans", "STrans"] )
df_dfkkop['BPartner'] = df_dfkkop['BPartner'].astype(str).str.strip()
df_dfkkop['BPartner'] = df_dfkkop['BPartner'].str.split('.').str[0]  # Remove any decimal part
df_dfkkop['Amount'] = df_dfkkop['Amount'].astype(float)
# Filter for final billing transactions
df_dfkkop = df_dfkkop[ (df_dfkkop['MTrans'] == '0200') & (df_dfkkop['STrans'] == '0002') ] 

df_sth = cu.get_file("stage_transactional_history", columns=["CUSTOMERID", "TRANSACTIONAMOUNT", "TRANSACTIONDESCRIPTION"], skip_cache=True)
df_sth['CUSTOMERID'] = df_sth['CUSTOMERID'].astype(str).str.strip()
df_sth['TRANSACTIONAMOUNT'] = df_sth['TRANSACTIONAMOUNT'].astype(float)
# Filter for final billing transactions
df_sth = df_sth[ df_sth['TRANSACTIONDESCRIPTION'] == 'Final Billing' ]

df_sth_sum = df_sth.groupby("CUSTOMERID")["TRANSACTIONAMOUNT"].sum().reset_index()
df_dfkkop_sum = df_dfkkop.groupby("BPartner")["Amount"].sum().reset_index()
# Merge the two dataframes on customer ID
df_merged = pd.merge(df_sth_sum, df_dfkkop_sum, left_on="CUSTOMERID", right_on="BPartner", how="outer", suffixes=('_STH', '_DFKKOP'))
print("Total dataset is {} rows.".format(len(df_merged)))
# Check for discrepancies
df_merged['Discrepancy'] = df_merged['TRANSACTIONAMOUNT'] - df_merged['Amount']
# Filter for discrepancies
df_discrepancies = df_merged[df_merged['Discrepancy'] != 0]

if not df_discrepancies.empty:
    print("Discrepancies found between STAGE_TRANSACTIONAL_HISTORY and DFKKOP:")
    print(df_discrepancies)
    print("Total discrepancies found: {}".format(len(df_discrepancies)))
    print("Total amount of discrepancies found: {}".format(df_discrepancies['Discrepancy'].sum()))
    pd.to_csv(df_discrepancies, "discrepancies_dfkkop_sth.csv", index=False)
    print("Discrepancies saved to discrepancies_dfkkop_sth.csv")
else:
    print("No discrepancies found between STAGE_TRANSACTIONAL_HISTORY and DFKKOP.")
