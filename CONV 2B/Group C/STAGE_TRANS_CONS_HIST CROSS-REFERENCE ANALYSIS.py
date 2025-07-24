# STAGE_TRANS_CONS_HIST CROSS-REFERENCE ANALYSIS
# =============================================================================
import pandas as pd
import os

def analyze_billing_batch_mismatch():
    # Load the debug files
    try:
        trans_debug_path = r"C:\Users\us85360\Desktop\CONV 2 B  - STAGE_TRANSACTIONAL_HIST\DEBUG_TRANSACTIONAL_TRACKING.csv"
        cons_debug_path = r"C:\Users\us85360\Desktop\CONV 2B - STAGE_CONSUMPTION_HIST\DEBUG_CONSUMPTION_TRACKING.csv"
        
        # ACTUALLY LOAD THE FILES:
        print("Loading debug files...")
        trans_debug = pd.read_csv(trans_debug_path)
        cons_debug = pd.read_csv(cons_debug_path)
        print(f"✅ Successfully loaded both debug files")
        print(f"   Transactional records: {len(trans_debug):,}")
        print(f"   Consumption records: {len(cons_debug):,}")
        
    except FileNotFoundError as e:
        print(f"Error: Could not find debug files. Make sure both scripts have run. {e}")
        return
    except Exception as e:
        print(f"Error loading debug files: {e}")
        return

    print("\nENHANCED CROSS-REFERENCE ANALYSIS")
    print("="*60)

    # FIXED: Clean the data first - remove quotes AND decimal points
    trans_debug['PROCESSED_BILLORINVOICE'] = trans_debug['PROCESSED_BILLORINVOICE'].astype(str).str.replace('"', '').str.replace('.0', '').str.strip()
    cons_debug['PROCESSED_BILLINGBATCH'] = cons_debug['PROCESSED_BILLINGBATCH'].astype(str).str.replace('"', '').str.replace('.0', '').str.strip()

    # 1. Check for common BILLORINVOICENUMBER/BILLINGBATCHNUMBER values
    trans_values = set(trans_debug[trans_debug['PROCESSED_BILLORINVOICE'] != '']['PROCESSED_BILLORINVOICE'].unique())
    cons_values = set(cons_debug[cons_debug['PROCESSED_BILLINGBATCH'] != '']['PROCESSED_BILLINGBATCH'].unique())

    # Remove empty strings and 'nan' values
    trans_values = {v for v in trans_values if v and v != 'nan' and v != ''}
    cons_values = {v for v in cons_values if v and v != 'nan' and v != ''}

    common_values = trans_values.intersection(cons_values)
    trans_only = trans_values - cons_values
    cons_only = cons_values - trans_values

    print(f"\n1. VALUE OVERLAP ANALYSIS:")
    print(f"   Unique BILLORINVOICENUMBER values: {len(trans_values):,}")
    print(f"   Unique BILLINGBATCHNUMBER values: {len(cons_values):,}")
    print(f"   Common values: {len(common_values):,}")
    print(f"   Only in transactional: {len(trans_only):,}")
    print(f"   Only in consumption: {len(cons_only):,}")
    
    if len(common_values) > 0:
        print(f"   Sample common values: {list(common_values)[:10]}")
        overlap_pct = len(common_values) / max(len(trans_values), len(cons_values)) * 100
        print(f"   📊 Overlap rate: {overlap_pct:.1f}%")
    else:
        print("   ❌ NO COMMON VALUES FOUND!")
        
    if len(trans_only) > 0:
        print(f"   Sample transactional-only: {list(trans_only)[:10]}")
    if len(cons_only) > 0:
        print(f"   Sample consumption-only: {list(cons_only)[:10]}")

    # 2. Check customer overlap
    trans_customers = set(trans_debug['CUSTOMERID'].astype(str).str.replace('"', '').str.strip().unique())
    cons_customers = set(cons_debug['CUSTOMERID'].astype(str).str.replace('"', '').str.strip().unique())
    common_customers = trans_customers.intersection(cons_customers)

    print(f"\n2. CUSTOMER OVERLAP:")
    print(f"   Customers in transactional: {len(trans_customers):,}")
    print(f"   Customers in consumption: {len(cons_customers):,}")
    print(f"   Common customers: {len(common_customers):,}")
    if len(trans_customers) > 0 and len(cons_customers) > 0:
        coverage = len(common_customers)/max(len(trans_customers), len(cons_customers))*100
        print(f"   Coverage: {coverage:.1f}%")

    # 3. Date range comparison
    try:
        trans_debug['DOC_DATE'] = pd.to_datetime(trans_debug['DOC_DATE'], errors='coerce')
        cons_debug['CURRREADDATE'] = pd.to_datetime(cons_debug['CURRREADDATE'].astype(str).str.replace('"', ''), errors='coerce')

        print(f"\n3. DATE RANGE COMPARISON:")
        trans_min, trans_max = trans_debug['DOC_DATE'].min(), trans_debug['DOC_DATE'].max()
        cons_min, cons_max = cons_debug['CURRREADDATE'].min(), cons_debug['CURRREADDATE'].max()
        
        print(f"   Transactional date range: {trans_min} to {trans_max}")
        print(f"   Consumption date range: {cons_min} to {cons_max}")
        
        # Check if date ranges overlap
        if pd.notna(trans_min) and pd.notna(cons_min):
            overlap_start = max(trans_min, cons_min)
            overlap_end = min(trans_max, cons_max)
            if overlap_start <= overlap_end:
                print(f"   ✅ Date overlap: {overlap_start} to {overlap_end}")
            else:
                print(f"   ⚠️  WARNING: No date overlap between datasets!")
        else:
            print(f"   ⚠️  WARNING: Could not determine date overlap!")
    except Exception as e:
        print(f"\n3. DATE RANGE COMPARISON: Error analyzing dates: {e}")

    # 4. Find records with matching batch numbers by customer
    print(f"\n4. MATCHING RECORDS BY CUSTOMER:")
    matches_found = 0
    customers_with_matches = 0
    
    # Clean customer IDs
    trans_debug['CUSTOMERID_CLEAN'] = trans_debug['CUSTOMERID'].astype(str).str.replace('"', '').str.strip()
    cons_debug['CUSTOMERID_CLEAN'] = cons_debug['CUSTOMERID'].astype(str).str.replace('"', '').str.strip()
    
    for customer in list(common_customers)[:10]:  # Check first 10 common customers
        customer_trans = trans_debug[trans_debug['CUSTOMERID_CLEAN'] == customer]
        customer_cons = cons_debug[cons_debug['CUSTOMERID_CLEAN'] == customer]
        
        trans_batches = set(customer_trans[customer_trans['PROCESSED_BILLORINVOICE'] != '']['PROCESSED_BILLORINVOICE'])
        trans_batches = {v for v in trans_batches if v and v != 'nan' and v != ''}
        
        cons_batches = set(customer_cons[customer_cons['PROCESSED_BILLINGBATCH'] != '']['PROCESSED_BILLINGBATCH'])
        cons_batches = {v for v in cons_batches if v and v != 'nan' and v != ''}
        
        customer_matches = trans_batches.intersection(cons_batches)
        if len(customer_matches) > 0:
            matches_found += len(customer_matches)
            customers_with_matches += 1
            print(f"   Customer {customer}: {len(customer_matches)} matching batch numbers")
            if len(customer_matches) <= 3:  # Show actual matches for small numbers
                print(f"     Matches: {list(customer_matches)}")
        
    print(f"   📊 Summary: {customers_with_matches} customers with matches, {matches_found} total matching batch numbers")

    # 5. Raw value comparison
    print(f"\n5. RAW VALUE ANALYSIS:")
    
    # Sample raw values from both sources
    trans_raw_sample = trans_debug['RAW_REFERENCE'].dropna().head(10)
    cons_raw_sample = cons_debug['RAW_PRINT_DOC'].dropna().head(10)
    
    print(f"   Sample transactional raw values: {trans_raw_sample.tolist()}")
    print(f"   Sample consumption raw values: {cons_raw_sample.tolist()}")
    
    # Check if any raw values are identical
    trans_raw_set = set(trans_debug['RAW_REFERENCE'].dropna().astype(str))
    cons_raw_set = set(cons_debug['RAW_PRINT_DOC'].dropna().astype(str))
    raw_overlap = trans_raw_set.intersection(cons_raw_set)
    
    print(f"   Raw value overlap: {len(raw_overlap):,} identical raw values")
    if len(raw_overlap) > 0:
        print(f"   Sample identical raw values: {list(raw_overlap)[:10]}")

    # 6. Processing consistency check
    print(f"\n6. PROCESSING CONSISTENCY:")
    
    # Check if the same raw values produce the same processed values
    def check_processing_consistency(raw_val):
        try:
            if pd.notna(raw_val):
                # Handle both string and numeric inputs
                if isinstance(raw_val, str):
                    try:
                        raw_val = float(raw_val)
                    except:
                        return ""
                if isinstance(raw_val, (int, float)):
                    return str(int(raw_val))[2:10]
            return ""
        except:
            return ""
    
    # Test with actual values from the data
    if len(trans_raw_sample) > 0:
        print(f"   Processing test on actual data:")
        for i, raw_val in enumerate(trans_raw_sample.head(3)):
            processed = check_processing_consistency(raw_val)
            print(f"     {raw_val} -> '{processed}' (length: {len(processed)})")

    # FIXED: Add the test code here in the right place
    print(f"\n   🔬 DETAILED PROCESSING TEST:")
    test_values = [20000009778, 20000009778.0, "20000009778"]
    for val in test_values:
        try:
            full_str = str(int(val))
            processed = str(int(val))[2:10]
            print(f"     Input: {val} ({type(val).__name__}) -> Full: '{full_str}' -> Slice[2:10]: '{processed}' (len: {len(processed)})")
        except Exception as e:
            print(f"     Input: {val} ({type(val).__name__}) -> ERROR: {e}")
    
    # Test with actual values from both datasets
    print(f"\n   🔬 ACTUAL DATA COMPARISON:")
    if len(trans_raw_sample) > 0 and len(cons_raw_sample) > 0:
        print(f"   Transactional sample processing:")
        for val in trans_raw_sample.head(3):
            try:
                full_str = str(int(val))
                processed = str(int(val))[2:10]
                print(f"     Trans: {val} -> Full: '{full_str}' -> Processed: '{processed}'")
            except Exception as e:
                print(f"     Trans: {val} -> ERROR: {e}")
        
        print(f"   Consumption sample processing:")
        for val in cons_raw_sample.head(3):
            try:
                full_str = str(int(val))
                processed = str(int(val))[2:10]
                print(f"     Cons: {val} -> Full: '{full_str}' -> Processed: '{processed}'")
            except Exception as e:
                print(f"     Cons: {val} -> ERROR: {e}")

    # 7. Generate recommendations
    print(f"\n7. RECOMMENDATIONS:")
    
    if len(common_values) == 0:
        print("   ❌ No matching batch numbers found!")
        print("   🔍 Root causes to investigate:")
        print("     • DFKKOP 'Reference' and ZMECON 'Print Document No.' may not be related fields")
        print("     • Different date ranges between datasets")
        print("     • Different processing logic or data sources")
        print("     • Consider using Customer ID + Date proximity for linking instead")
    elif len(common_values) < min(len(trans_values), len(cons_values)) * 0.1:
        print("   ⚠️  Very low match rate - investigate data relationship")
        print(f"   📈 Match rate: {len(common_values) / min(len(trans_values), len(cons_values)) * 100:.2f}%")
    else:
        print("   ✅ Found meaningful matching batch numbers!")
        print(f"   📈 Match rate: {len(common_values) / min(len(trans_values), len(cons_values)) * 100:.2f}%")
        print("   🔍 Next steps: Investigate why some values don't match")

    # Save detailed comparison
    try:
        # Use the transactional directory for saving files
        save_dir = os.path.dirname(trans_debug_path)  # Gets the directory path
        
        comparison_results = {
            'trans_unique_values': len(trans_values),
            'cons_unique_values': len(cons_values), 
            'common_values': len(common_values),
            'trans_only_values': len(trans_only),
            'cons_only_values': len(cons_only),
            'common_customers': len(common_customers),
            'customers_with_batch_matches': customers_with_matches,
            'total_batch_matches': matches_found
        }
        
        comparison_df = pd.DataFrame([comparison_results])
        summary_path = os.path.join(save_dir, 'BATCH_NUMBER_COMPARISON_SUMMARY.csv')
        comparison_df.to_csv(summary_path, index=False)
        print(f"\n   📊 Detailed comparison saved to: {summary_path}")
        
        # Also save sample matches for further analysis
        if len(common_values) > 0:
            matches_df = pd.DataFrame({'MATCHING_BATCH_NUMBERS': list(common_values)})
            matches_path = os.path.join(save_dir, 'MATCHING_BATCH_NUMBERS.csv')
            matches_df.to_csv(matches_path, index=False)
            print(f"   📋 Matching batch numbers saved to: {matches_path}")
        
        # ADDED: Save detailed analysis of why values don't match
        analysis_results = []
        
        # Compare first 100 values from each dataset
        trans_sample = list(trans_values)[:100] if trans_values else []
        cons_sample = list(cons_values)[:100] if cons_values else []
        
        for i, trans_val in enumerate(trans_sample):
            analysis_results.append({
                'Source': 'Transactional',
                'Index': i,
                'Processed_Value': trans_val,
                'Length': len(str(trans_val)),
                'Has_Decimal': '.0' in str(trans_val),
                'Clean_Value': str(trans_val).replace('.0', '')
            })
        
        for i, cons_val in enumerate(cons_sample):
            analysis_results.append({
                'Source': 'Consumption',
                'Index': i,
                'Processed_Value': cons_val,
                'Length': len(str(cons_val)),
                'Has_Decimal': '.0' in str(cons_val),
                'Clean_Value': str(cons_val).replace('.0', '')
            })
        
        if analysis_results:
            analysis_df = pd.DataFrame(analysis_results)
            analysis_path = os.path.join(save_dir, 'DETAILED_VALUE_ANALYSIS.csv')
            analysis_df.to_csv(analysis_path, index=False)
            print(f"   🔍 Detailed value analysis saved to: {analysis_path}")
            
    except Exception as e:
        print(f"   ⚠️  Could not save comparison files: {e}")
        print(f"   Attempted to save to: {save_dir}")

    print("="*60)
    print("🎯 ANALYSIS COMPLETE!")

if __name__ == "__main__":
    analyze_billing_batch_mismatch()