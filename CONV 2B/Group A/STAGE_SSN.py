import pandas as pd
import os
import csv  # For CSV saving
import pgpy
from io import BytesIO, StringIO

# fake imghdr requirement
def get_image_type(filepath):
    return None

# Load Unitil private key
with open("../../Downloads/0x27E36A82-sec.asc", "r") as key_file:
    private_key, _ = pgpy.PGPKey.from_file(key_file.name)

# Load S&S Public key
with open("../../Downloads/sns-public-key.asc", "r") as pub_key_file:
    public_key, _ = pgpy.PGPKey.from_file(pub_key_file.name)

# Unlock the key with your passphrase
with private_key.unlock("GT&Unitil2025"):

    # Load the encrypted message
    with open("../DATA/5302 - Identification details.XLSX.gpg", "r") as enc_file:
        encrypted_message = pgpy.PGPMessage.from_file(enc_file.name)

    # Decrypt the message
    decrypted_message = private_key.decrypt(encrypted_message)

    # Load the excel file into a dataframe
    excel_df = pd.read_excel(BytesIO(decrypted_message.message))

    # Create the new data frame
    df_new = excel_df[['Business Partner', 'IDType', 'Identification number']].copy()

    #Rename the columns to CUSTOMERID, SSNTINTYPE, SSNTIN
    df_new.columns = ['CUSTOMERID', 'SSNTINTYPENAME', 'SSNTIN']
    df_new['SSNTINTYPE'] = df_new['SSNTINTYPENAME'].apply(lambda x: 1 if x == 'Social Security Number' else 2)
    df_new['DRIVERSLICENSE'] = ''
    df_new['DLSTATE'] = ''

    # Remove non-numeric characters from SSNTIN
    df_new['SSNTIN'] = df_new['SSNTIN'].astype(str).str.replace(r'\D', '', regex=True)
    #df_new['SSNTIN'] = df_new['SSNTIN'].str.replace('-', '', regex=False)

    #Ensure the SSNTIN column is formatted as XXX-XX-XXXX for SSNTINTYPE 1, or XX-XXXXXXX for SSNTINTYPE 2
    #df_new['SSNTIN'] = df_new.apply(lambda x: f"{x['SSNTIN'][:3]}-{x['SSNTIN'][3:5]}-{x['SSNTIN'][5:]}" if x['SSNTINTYPE'] == 1 else f"{x['SSNTIN'][:2]}-{x['SSNTIN'][2:]}", axis=1)

    #Reorderfs
    df_new = df_new[['CUSTOMERID','SSNTINTYPE', 'SSNTIN', 'DRIVERSLICENSE', 'DLSTATE']]

    # Drop duplicate records
    df_new = df_new.drop_duplicates()
    
    # --------------------------
    # Add trailer row
    # --------------------------
    trailer_row = pd.DataFrame([["TRAILER"] + [''] * (len(df_new.columns) - 1)], columns=df_new.columns)
    df_new = pd.concat([df_new, trailer_row], ignore_index=True)
    print(f"Added trailer row. Final row count: {len(df_new)}")

   # Conver the dataframe to an in-memory CSV file
    csv_buffer = BytesIO()
    df_new.to_csv(csv_buffer, index=False, quoting=csv.QUOTE_NONNUMERIC)

    # Create an encrypted message from the CSV data
    csv_buffer.seek(0)
    encrypted_csv = public_key.encrypt(pgpy.PGPMessage.new(csv_buffer.getvalue()))
    encrypted_csv_file = "STAGE_SSN.csv.gpg"

    # Save the encrypted CSV file
    with open(encrypted_csv_file, "w") as enc_file:
        enc_file.write(str(encrypted_csv))
        enc_file.flush()
        enc_file.seek(0)
    ''' for testing only 
    # Load the encrypted CSV file to verify
    with open(encrypted_csv_file, "r") as enc_file:
        encrypted_csv_content = enc_file.read() 

    # Decrypt the CSV file to verify
    decrypted_csv = private_key.decrypt(pgpy.PGPMessage.from_file(encrypted_csv_file))

    print("Decrypted CSV content:", decrypted_csv.message)
    '''
    print(f"Encrypted CSV file saved as {encrypted_csv_file}")    


 