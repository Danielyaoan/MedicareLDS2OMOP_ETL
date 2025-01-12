import os, os.path, subprocess
from constants import MEDI_FILE_TOKENS
import pandas as pd
import dask.dataframe as dd



# -----------------------------------
# - Combine 2 beneficiary files into 1.
#  This assumes the Medicare data is always 2018
# -----------------------------------
def combine_beneficiary_files(directory_name, year_number, output_bene_filename):
    print('-'*80)
    print('combine_beneficiary_files: year_number=' + str(year_number))
    print('Writing to ->' + output_bene_filename)


    input_bene_filename_1 = os.path.join(directory_name, str(year_number), 'mbsf_abcd_summary.csv')
    input_bene_filename_2 = os.path.join(directory_name, str(year_number), 'mbsf_cc_summary.csv')
    file_name = [input_bene_filename_1, input_bene_filename_2]
    for input_bene_filename in file_name:
        print('Reading    ->' + input_bene_filename)
        if not os.path.exists(input_bene_filename):
            print('.....not found, looking for zip')
            zipped_file = input_bene_filename.replace('.csv','.zip')
            if os.path.exists(zipped_file):
                subprocess.call(["unzip", "-d", os.path.join(directory_name, str(year_number)), zipped_file])
            if not os.path.exists(input_bene_filename):
                print('** File not found !! ', input_bene_filename)
                raise Exception()

    # dtypes = pd.read_csv(input_bene_filename_1, nrows=1000).dtypes.to_dict()
    # for col, dtype in dtypes.items():
    #     if pd.api.types.is_integer_dtype(dtype):
    #         dtypes[col]='float64'
    bene_base = dd.read_csv(input_bene_filename_1, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_bene_filename_2, nrows=1000).dtypes.to_dict()
    bene_ccs = dd.read_csv(input_bene_filename_2, dtype=object, assume_missing=True)
    bene_summary = dd.merge(bene_base, bene_ccs, on=['BENE_ID', 'BENE_ENROLLMT_REF_YR', 'ENRL_SRC'], how='left')
    
    bene_summary.to_csv(output_bene_filename, index=False, single_file=True)
    del bene_base, bene_ccs, bene_summary

    print('Done')

# -----------------------------------
# - Combine 2 carrier files into 1, dropping the headers beneficiary files into 1, dropping th eheaders
#
# -----------------------------------
def combine_carrier_files(directory_name, year_number, output_carrier_filename):
    print('-'*80)
    print('combine_carier_files: year_number=' + str(year_number))
    print('Writing to ->' + output_carrier_filename)

    input_carier_filename_1 = os.path.join(directory_name, str(year_number), 'bcarrier_claims_k.csv')
    input_carier_filename_2 = os.path.join(directory_name, str(year_number), 'bcarrier_line_k.csv')
    input_carier_filename_3 = os.path.join(directory_name, str(year_number), 'bcarrier_demo_codes.csv')
    file_name = [input_carier_filename_1, input_carier_filename_2, input_carier_filename_3]
    for input_carier_filename in file_name:
        print('Reading    ->' + input_carier_filename)
        if not os.path.exists(input_carier_filename):
            print('.....not found, looking for zip')
            zipped_file = input_carier_filename.replace('.csv','.zip')
            if os.path.exists(zipped_file):
                subprocess.call(["unzip", "-d", os.path.join(directory_name, str(year_number)), zipped_file])
            if not os.path.exists(input_carier_filename):
                print('** File not found !! ', input_carier_filename)
                raise Exception()
    # dtypes = pd.read_csv(input_carier_filename_1, nrows=1000).dtypes.to_dict()
    carier_claim = dd.read_csv(input_carier_filename_1, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_carier_filename_2, nrows=1000).dtypes.to_dict()
    carier_line = dd.read_csv(input_carier_filename_2, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_carier_filename_3, nrows=1000).dtypes.to_dict()
    carier_demo = dd.read_csv(input_carier_filename_3, dtype=object, assume_missing=True)
    
    
    merged_1 = dd.merge(carier_claim, carier_demo, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    carier_line = carier_line.rename(columns={'CML_THRU_DT':'LINE_CLM_THRU_DT'})
    carier_summary = dd.merge(merged_1, carier_line, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    
    carier_summary.to_csv(output_carrier_filename, index=False, single_file=True)
    del carier_claim, carier_demo, merged_1, carier_line, carier_summary

    print('Done')


def combine_inpatient_files(directory_name, year_number, output_inpt_filename):
    print('-'*80)
    print('combine_inpatient_files: year_number=' + str(year_number))
    print('Writing to ->' + output_inpt_filename)


    input_inpt_filename_1 = os.path.join(directory_name, str(year_number), 'inpatient_base_claims_k.csv')
    input_inpt_filename_2 = os.path.join(directory_name, str(year_number), 'inpatient_revenue_center_k.csv')
    input_inpt_filename_3 = os.path.join(directory_name, str(year_number), 'inpatient_condition_codes.csv')
    input_inpt_filename_4 = os.path.join(directory_name, str(year_number), 'inpatient_occurrnce_codes.csv')
    input_inpt_filename_5 = os.path.join(directory_name, str(year_number), 'inpatient_span_codes.csv')
    input_inpt_filename_6 = os.path.join(directory_name, str(year_number), 'inpatient_value_codes.csv')
    input_inpt_filename_7 = os.path.join(directory_name, str(year_number), 'inpatient_demo_codes.csv')
    file_name = [input_inpt_filename_1, input_inpt_filename_2, input_inpt_filename_3, input_inpt_filename_4, input_inpt_filename_5, input_inpt_filename_6, input_inpt_filename_7]
    
    for input_inpt_filename in file_name:
        print('Reading    ->' + input_inpt_filename)
        if not os.path.exists(input_inpt_filename):
            print('.....not found, looking for zip')
            zipped_file = input_inpt_filename.replace('.csv','.zip')
            if os.path.exists(zipped_file):
                subprocess.call(["unzip", "-d", os.path.join(directory_name, str(year_number)), zipped_file])
            if not os.path.exists(input_inpt_filename):
                print('** File not found !! ', input_inpt_filename)
                raise Exception()
    # dtypes = pd.read_csv(input_inpt_filename_1, nrows=1000).dtypes.to_dict()
    inpt_base = dd.read_csv(input_inpt_filename_1, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_inpt_filename_2, nrows=1000).dtypes.to_dict()
    inpt_revenue = dd.read_csv(input_inpt_filename_2, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_inpt_filename_3, nrows=1000).dtypes.to_dict()
    inpt_condition = dd.read_csv(input_inpt_filename_3, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_inpt_filename_4, nrows=1000).dtypes.to_dict()
    inpt_occurrence = dd.read_csv(input_inpt_filename_4, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_inpt_filename_5, nrows=1000).dtypes.to_dict()
    inpt_span = dd.read_csv(input_inpt_filename_5, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_inpt_filename_6, nrows=1000).dtypes.to_dict()
    inpt_value = dd.read_csv(input_inpt_filename_6, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_inpt_filename_7, nrows=1000).dtypes.to_dict()
    inpt_demo = dd.read_csv(input_inpt_filename_7, dtype=object, assume_missing=True)
    
    merged_1 = dd.merge(inpt_base, inpt_revenue, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    merged_2 = dd.merge(merged_1, inpt_condition, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    merged_3 = dd.merge(merged_2, inpt_occurrence, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    merged_4 = dd.merge(merged_3, inpt_span, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    merged_5 = dd.merge(merged_4, inpt_value, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    inpt_summary = dd.merge(merged_5, inpt_demo, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    
    inpt_summary.to_csv(output_inpt_filename, index=False, single_file=True)
    del inpt_base, inpt_revenue, inpt_condition, inpt_occurrence, inpt_span, inpt_value, inpt_demo, merged_1, merged_2, merged_3, merged_4, merged_5, inpt_summary

    print('Done')
    
                
        
        
        
def combine_outpatient_files(directory_name, year_number, output_outpt_filename):
    print('-'*80)
    print('combine_outpatient_files: year_number=' + str(year_number))
    print('Writing to ->' + output_outpt_filename)


    input_outpt_filename_1 = os.path.join(directory_name, str(year_number), 'outpatient_base_claims_k.csv')
    input_outpt_filename_2 = os.path.join(directory_name, str(year_number), 'outpatient_revenue_center_k.csv')
    input_outpt_filename_3 = os.path.join(directory_name, str(year_number), 'outpatient_condition_codes.csv')
    input_outpt_filename_4 = os.path.join(directory_name, str(year_number), 'outpatient_occurrnce_codes.csv')
    input_outpt_filename_5 = os.path.join(directory_name, str(year_number), 'outpatient_span_codes.csv')
    input_outpt_filename_6 = os.path.join(directory_name, str(year_number), 'outpatient_value_codes.csv')
    input_outpt_filename_7 = os.path.join(directory_name, str(year_number), 'outpatient_demo_codes.csv')
    file_name = [input_outpt_filename_1, input_outpt_filename_2, input_outpt_filename_3, input_outpt_filename_4, input_outpt_filename_5, input_outpt_filename_6, input_outpt_filename_7]
    
    for input_outpt_filename in file_name:
        print('Reading    ->' + input_outpt_filename)
        if not os.path.exists(input_outpt_filename):
            print('.....not found, looking for zip')
            zipped_file = input_outpt_filename.replace('.csv','.zip')
            if os.path.exists(zipped_file):
                subprocess.call(["unzip", "-d", os.path.join(directory_name, str(year_number)), zipped_file])
            if not os.path.exists(input_outpt_filename):
                print('** File not found !! ', input_outpt_filename)
                raise Exception()
    # dtypes = pd.read_csv(input_outpt_filename_1, nrows=1000).dtypes.to_dict()
    outpt_base = dd.read_csv(input_outpt_filename_1, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_outpt_filename_2, nrows=1000).dtypes.to_dict()
    outpt_revenue = dd.read_csv(input_outpt_filename_2, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_outpt_filename_3, nrows=1000).dtypes.to_dict()
    outpt_condition = dd.read_csv(input_outpt_filename_3, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_outpt_filename_4, nrows=1000).dtypes.to_dict()
    outpt_occurrence = dd.read_csv(input_outpt_filename_4, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_outpt_filename_5, nrows=1000).dtypes.to_dict()
    outpt_span = dd.read_csv(input_outpt_filename_5, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_outpt_filename_6, nrows=1000).dtypes.to_dict()
    outpt_value = dd.read_csv(input_outpt_filename_6, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_outpt_filename_7, nrows=1000).dtypes.to_dict()
    outpt_demo = dd.read_csv(input_outpt_filename_7, dtype=object, assume_missing=True)
    
    merged_1 = dd.merge(outpt_base, outpt_revenue, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    merged_2 = dd.merge(merged_1, outpt_condition, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    merged_3 = dd.merge(merged_2, outpt_occurrence, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    merged_4 = dd.merge(merged_3, outpt_span, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    merged_5 = dd.merge(merged_4, outpt_value, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    outpt_summary = dd.merge(merged_5, outpt_demo, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    
    outpt_summary.to_csv(output_outpt_filename, index=False, single_file=True)

    del outpt_base, outpt_revenue, outpt_condition, outpt_occurrence, outpt_span, outpt_value, outpt_demo, merged_1, merged_2, merged_3, merged_4, merged_5, outpt_summary

    print('Done')
    
                    
        
                
        
        
def combine_snf_files(directory_name, year_number, output_snf_filename):
    print('-'*80)
    print('combine_skilled_nursing_facility_files: year_number=' + str(year_number))
    print('Writing to ->' + output_snf_filename)


    input_snf_filename_1 = os.path.join(directory_name, str(year_number), 'snf_base_claims_k.csv')
    input_snf_filename_2 = os.path.join(directory_name, str(year_number), 'snf_revenue_center_k.csv')
    input_snf_filename_3 = os.path.join(directory_name, str(year_number), 'snf_condition_codes.csv')
    input_snf_filename_4 = os.path.join(directory_name, str(year_number), 'snf_occurrnce_codes.csv')
    input_snf_filename_5 = os.path.join(directory_name, str(year_number), 'snf_span_codes.csv')
    input_snf_filename_6 = os.path.join(directory_name, str(year_number), 'snf_value_codes.csv')
    input_snf_filename_7 = os.path.join(directory_name, str(year_number), 'snf_demo_codes.csv')
    file_name = [input_snf_filename_1, input_snf_filename_2, input_snf_filename_3, input_snf_filename_4, input_snf_filename_5, input_snf_filename_6, input_snf_filename_7]
    
    for input_snf_filename in file_name:
        print('Reading    ->' + input_snf_filename)
        if not os.path.exists(input_snf_filename):
            print('.....not found, looking for zip')
            zipped_file = input_snf_filename.replace('.csv','.zip')
            if os.path.exists(zipped_file):
                subprocess.call(["unzip", "-d", os.path.join(directory_name, str(year_number)), zipped_file])
            if not os.path.exists(input_snf_filename):
                print('** File not found !! ', input_snf_filename)
                raise Exception()
    # dtypes = pd.read_csv(input_snf_filename_1, nrows=1000).dtypes.to_dict()
    snf_base = dd.read_csv(input_snf_filename_1, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_snf_filename_2, nrows=1000).dtypes.to_dict()
    snf_revenue = dd.read_csv(input_snf_filename_2, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_snf_filename_3, nrows=1000).dtypes.to_dict()
    snf_condition = dd.read_csv(input_snf_filename_3, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_snf_filename_4, nrows=1000).dtypes.to_dict()
    snf_occurrence = dd.read_csv(input_snf_filename_4, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_snf_filename_5, nrows=1000).dtypes.to_dict()
    snf_span = dd.read_csv(input_snf_filename_5, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_snf_filename_6, nrows=1000).dtypes.to_dict()
    snf_value = dd.read_csv(input_snf_filename_6, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_snf_filename_7, nrows=1000).dtypes.to_dict()
    snf_demo = dd.read_csv(input_snf_filename_7, dtype=object, assume_missing=True)
    
    merged_1 = dd.merge(snf_base, snf_revenue, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    merged_2 = dd.merge(merged_1, snf_condition, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    merged_3 = dd.merge(merged_2, snf_occurrence, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    merged_4 = dd.merge(merged_3, snf_span, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    merged_5 = dd.merge(merged_4, snf_value, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    snf_summary = dd.merge(merged_5, snf_demo, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    
    snf_summary.to_csv(output_snf_filename, index=False, single_file=True)

    del snf_base, snf_revenue, snf_condition, snf_occurrence, snf_span, snf_value, snf_demo, merged_1, merged_2, merged_3, merged_4, merged_5, snf_summary

    print('Done')
                    
        
        
        
def combine_dme_files(directory_name, year_number, output_dme_filename):
    print('-'*80)
    print('combine_durable_medical_equipment_files: year_number=' + str(year_number))
    print('Writing to ->' + output_dme_filename)

    input_dme_filename_1 = os.path.join(directory_name, str(year_number), 'dme_claims_k.csv')
    input_dme_filename_2 = os.path.join(directory_name, str(year_number), 'dme_line_k.csv')
    input_dme_filename_3 = os.path.join(directory_name, str(year_number), 'dme_demo_codes.csv')
    file_name = [input_dme_filename_1, input_dme_filename_2, input_dme_filename_3]
    for input_dme_filename in file_name:
        print('Reading    ->' + input_dme_filename)
        if not os.path.exists(input_dme_filename):
            print('.....not found, looking for zip')
            zipped_file = input_dme_filename.replace('.csv','.zip')
            if os.path.exists(zipped_file):
                subprocess.call(["unzip", "-d", os.path.join(directory_name, str(year_number)), zipped_file])
            if not os.path.exists(input_dme_filename):
                print('** File not found !! ', input_dme_filename)
                raise Exception()
    # dtypes = pd.read_csv(input_dme_filename_1, nrows=1000).dtypes.to_dict()
    dme_claim = dd.read_csv(input_dme_filename_1, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_dme_filename_2, nrows=1000).dtypes.to_dict()
    dme_line = dd.read_csv(input_dme_filename_2, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_dme_filename_3, nrows=1000).dtypes.to_dict()
    dme_demo = dd.read_csv(input_dme_filename_3, dtype=object, assume_missing=True)
    
    
    merged_1 = dd.merge(dme_claim, dme_demo, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    dme_line.rename(columns={'CML_THRU_DT':'LINE_CLM_THRU_DT'}, inplace=True)
    dme_summary = dd.merge(merged_1, dme_line, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    
    dme_summary.to_csv(output_dme_filename, index=False, single_file=True)

    del dme_claim, dme_line, dme_demo, merged_1, dme_summary

    print('Done')
    
    

    
    
def combine_hha_files(directory_name, year_number, output_hha_filename):
    print('-'*80)
    print('combine_home_health_agency_files: year_number=' + str(year_number))
    print('Writing to ->' + output_hha_filename)


    input_hha_filename_1 = os.path.join(directory_name, str(year_number), 'hha_base_claims_k.csv')
    input_hha_filename_2 = os.path.join(directory_name, str(year_number), 'hha_revenue_center_k.csv')
    input_hha_filename_3 = os.path.join(directory_name, str(year_number), 'hha_condition_codes.csv')
    input_hha_filename_4 = os.path.join(directory_name, str(year_number), 'hha_occurrnce_codes.csv')
    input_hha_filename_5 = os.path.join(directory_name, str(year_number), 'hha_span_codes.csv')
    input_hha_filename_6 = os.path.join(directory_name, str(year_number), 'hha_value_codes.csv')
    input_hha_filename_7 = os.path.join(directory_name, str(year_number), 'hha_demo_codes.csv')
    file_name = [input_hha_filename_1, input_hha_filename_2, input_hha_filename_3, input_hha_filename_4, input_hha_filename_5, input_hha_filename_6, input_hha_filename_7]
    
    for input_hha_filename in file_name:
        print('Reading    ->' + input_hha_filename)
        if not os.path.exists(input_hha_filename):
            print('.....not found, looking for zip')
            zipped_file = input_hha_filename.replace('.csv','.zip')
            if os.path.exists(zipped_file):
                subprocess.call(["unzip", "-d", os.path.join(directory_name, str(year_number)), zipped_file])
            if not os.path.exists(input_hha_filename):
                print('** File not found !! ', input_hha_filename)
                raise Exception()
    # dtypes = pd.read_csv(input_hha_filename_1, nrows=1000).dtypes.to_dict()
    hha_base = dd.read_csv(input_hha_filename_1, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_hha_filename_2, nrows=1000).dtypes.to_dict()
    hha_revenue = dd.read_csv(input_hha_filename_2, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_hha_filename_3, nrows=1000).dtypes.to_dict()
    hha_condition = dd.read_csv(input_hha_filename_3, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_hha_filename_4, nrows=1000).dtypes.to_dict()
    hha_occurrence = dd.read_csv(input_hha_filename_4, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_hha_filename_5, nrows=1000).dtypes.to_dict()
    hha_span = dd.read_csv(input_hha_filename_5, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_hha_filename_6, nrows=1000).dtypes.to_dict()
    hha_value = dd.read_csv(input_hha_filename_6, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_hha_filename_7, nrows=1000).dtypes.to_dict()
    hha_demo = dd.read_csv(input_hha_filename_7, dtype=object, assume_missing=True)
    
    merged_1 = dd.merge(hha_base, hha_revenue, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    merged_2 = dd.merge(merged_1, hha_condition, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    merged_3 = dd.merge(merged_2, hha_occurrence, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    merged_4 = dd.merge(merged_3, hha_span, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    merged_5 = dd.merge(merged_4, hha_value, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    hha_summary = dd.merge(merged_5, hha_demo, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    
    hha_summary.to_csv(output_hha_filename, index=False, single_file=True)

    del hha_base, hha_revenue, hha_condition, hha_occurrence, hha_span, hha_value, hha_demo, merged_1, merged_2, merged_3, merged_4, merged_5, hha_summary

    print('Done')
    
                    
        
        
        
def combine_hospice_files(directory_name, year_number, output_hospice_filename):
    print('-'*80)
    print('combine_hospice_files: year_number=' + str(year_number))
    print('Writing to ->' + output_hospice_filename)


    input_hospice_filename_1 = os.path.join(directory_name, str(year_number), 'hospice_base_claims_k.csv')
    input_hospice_filename_2 = os.path.join(directory_name, str(year_number), 'hospice_revenue_center_k.csv')
    input_hospice_filename_3 = os.path.join(directory_name, str(year_number), 'hospice_condition_codes.csv')
    input_hospice_filename_4 = os.path.join(directory_name, str(year_number), 'hospice_occurrnce_codes.csv')
    input_hospice_filename_5 = os.path.join(directory_name, str(year_number), 'hospice_span_codes.csv')
    input_hospice_filename_6 = os.path.join(directory_name, str(year_number), 'hospice_value_codes.csv')
    input_hospice_filename_7 = os.path.join(directory_name, str(year_number), 'hospice_demo_codes.csv')
    file_name = [input_hospice_filename_1, input_hospice_filename_2, input_hospice_filename_3, input_hospice_filename_4, input_hospice_filename_5, input_hospice_filename_6, input_hospice_filename_7]
    
    for input_hospice_filename in file_name:
        print('Reading    ->' + input_hospice_filename)
        if not os.path.exists(input_hospice_filename):
            print('.....not found, looking for zip')
            zipped_file = input_hospice_filename.replace('.csv','.zip')
            if os.path.exists(zipped_file):
                subprocess.call(["unzip", "-d", os.path.join(directory_name, str(year_number)), zipped_file])
            if not os.path.exists(input_hospice_filename):
                print('** File not found !! ', input_hospice_filename)
                raise Exception()
    # dtypes = pd.read_csv(input_hospice_filename_1, nrows=1000).dtypes.to_dict()
    hospice_base = dd.read_csv(input_hospice_filename_1, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_hospice_filename_2, nrows=1000).dtypes.to_dict()
    hospice_revenue = dd.read_csv(input_hospice_filename_2, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_hospice_filename_3, nrows=1000).dtypes.to_dict()
    hospice_condition = dd.read_csv(input_hospice_filename_3, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_hospice_filename_4, nrows=1000).dtypes.to_dict()
    hospice_occurrence = dd.read_csv(input_hospice_filename_4, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_hospice_filename_5, nrows=1000).dtypes.to_dict()
    hospice_span = dd.read_csv(input_hospice_filename_5, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_hospice_filename_6, nrows=1000).dtypes.to_dict()
    hospice_value = dd.read_csv(input_hospice_filename_6, dtype=object, assume_missing=True)
    # dtypes = pd.read_csv(input_hospice_filename_7, nrows=1000).dtypes.to_dict()
    hospice_demo = dd.read_csv(input_hospice_filename_7, dtype=object, assume_missing=True)
    
    merged_1 = dd.merge(hospice_base, hospice_revenue, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    merged_2 = dd.merge(merged_1, hospice_condition, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    merged_3 = dd.merge(merged_2, hospice_occurrence, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    merged_4 = dd.merge(merged_3, hospice_span, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    merged_5 = dd.merge(merged_4, hospice_value, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    hospice_summary = dd.merge(merged_5, hospice_demo, on=['BENE_ID', 'CLM_ID', 'NCH_CLM_TYPE_CD'], how='left')
    
    hospice_summary.to_csv(output_hospice_filename, index=False, single_file=True)

    del hospice_base, hospice_revenue, hospice_condition, hospice_occurrence, hospice_span, hospice_value, hospice_demo, merged_1, merged_2, merged_3, merged_4, merged_5, hospice_summary

    print('Done')
                    
        

    
    
    
class FileDescriptor(object):
    def __init__(self, token, mode, directory_name, filename, year_number, verify_exists=False, sort_required=False):
        self.token = token
        self.mode = mode
        self.directory_name = directory_name
        self.filename = filename
        self.year_number = year_number
        self.verify_exists = verify_exists
        self.complete_pathname = os.path.join(directory_name, filename)
        self.fd = None
        self._at_eof = False
        self._records_read = 0
        self._records_written = 0

        print('--FileDescriptor--')
        print('...token                 =', token)
        print('...mode                  =', mode)
        print('...complete_pathname     =', self.complete_pathname)

        ## TODO:
        # should be able to handle:
        #   .zip -> .csv
        #   combine beneficiary (208, 2009, 2010) . csv ->  comb
        #   sort .csv -> .srt
        #---------------
        if verify_exists:
            # handle carrier claims
            files = [self.complete_pathname]
            #if self.token == 'carrier':
            #    files = [ self.complete_pathname.replace('.csv', 'A.csv'),
            #              self.complete_pathname.replace('.csv', 'B.csv')]
            for f in files:
                print('.....verifying ->', f)
                if not os.path.exists(f):
                    # handle beneficiary
                    if self.token == MEDI_FILE_TOKENS.BENEFICARY:
                        combine_beneficiary_files(directory_name, year_number, output_bene_filename=f)
                    elif self.token == MEDI_FILE_TOKENS.INPATIENT:
                        combine_inpatient_files(directory_name, year_number, output_inpt_filename=f)
                    elif self.token == MEDI_FILE_TOKENS.OUTPATIENT:
                        combine_outpatient_files(directory_name, year_number, output_outpt_filename=f)
                    elif self.token == MEDI_FILE_TOKENS.CARRIER:
                        combine_carrier_files(directory_name, year_number, output_carrier_filename=f)
                    elif self.token == MEDI_FILE_TOKENS.SNF:
                        combine_snf_files(directory_name, year_number, output_snf_filename=f)
                    elif self.token == MEDI_FILE_TOKENS.DME:
                        combine_dme_files(directory_name, year_number, output_dme_filename=f)
                    elif self.token == MEDI_FILE_TOKENS.HHA:
                        combine_hha_files(directory_name, year_number, output_hha_filename=f)
                    elif self.token == MEDI_FILE_TOKENS.HOSPICE:
                        combine_hospice_files(directory_name, year_number, output_hospice_filename=f)
                    else:
                        #  unzip it if it's there
                        print('.....not found, looking for zip')
                        zipped_file = f.replace('.csv','.zip')
                        if os.path.exists(zipped_file):
                            subprocess.call(["unzip", "-d", directory_name, zipped_file])
                        if not os.path.exists(f):
                            print('** File not found !! ', f)
                            raise Exception()
       

        if sort_required:
            print('** Sorted file required')
            sorted_path = self.complete_pathname + '.srt'
            print('.....verifying ->', sorted_path)
            if not os.path.exists(sorted_path):
                zargs = []
                '''
                if self.token == MEDI_FILE_TOKENS.BENEFICARY:  #Sort on second field
                    fin = open(self.complete_pathname,"r")
                    firstline = fin.readline()
                    textfile = []
                    for line in fin:
                        v = line.split(",")
                        textfile.append([v[1],line])
                    fin.close()
                    textfile.sort()
                    fout = open(sorted_path,"w")
                    fout.write(firstline)
                    for (key,line) in textfile:
                        fout.write(line)
                    fout.close()
                else:
                '''
                fin = open(self.complete_pathname,"r")
                firstline = fin.readline()
                textfile = []
                for line in fin:
                    textfile.append(line)
                fin.close()
                textfile.sort()
                fout = open(sorted_path,"w")
                fout.write(firstline)
                for line in textfile:
                    fout.write(line)
                fout.close()

            self.complete_pathname = sorted_path


    def __str__(self):
        return 'token={0:25}, mode={1:10}\n\t filename={2:50} \n\t complete_pathname={3:50} \n\t fd={4}\n\t '.format(self.token, self.mode, self.filename, self.complete_pathname, self.fd)

    def get_patient_records(self, BENE_ID, record_list):
        ## assumes records are in DESYNPUF_ID order
        ## we will table all the records for an ID
        ## when that ID is requested, return them and table the next set
        ## if requested ID is less than buffer-ID, return empty list

        if self._at_eof:
            return

        # done = False
        # print 'get_patient_records->',DESYNPUF_ID
        rec = ''
        skip_count = 0

        if self.fd is None:
            self.fd = open(self.complete_pathname,'rb')
            # 2016-05-26 Skip header which is now at the top of every sorted file
            rec = self.fd.readline()

        # save_DESYNPUF_ID = ''
        recs_in = 0
        while True:
            try:
                rec = self.fd.readline()
                if not rec:
                    self._at_eof = True
                    break
                recs_in += 1
                if recs_in % 1000 == 0:
                    print('get_patient_records for ',BENE_ID, ', recs_in=', recs_in, ', file: ', self.complete_pathname)
                if recs_in > 1000000 :
                    raise

                
                if rec[0:len(BENE_ID)].decode('utf-8') == BENE_ID:
                    # print '\t ** keep'
                    # store array of fields instead of raw rec
                    record_list.append((rec[2:-1]).decode('utf-8').split(','))
                elif rec[0:len(BENE_ID)].decode('utf-8') < BENE_ID:
                    skip_count += 1
                else:
                    self.fd.seek(-len(rec),1)
                    # /done = True
                    break

            except IOError as ex:
                print('*** IO Error on file ', self.complete_pathname)
                print(ex)
                print('Record number :', self._records_read + len(record_list) - 1, ' recs_in=', recs_in)
                print('*** IO error \n..current record=', rec)
                raise BaseException
                done = True

        self.increment_recs_read(recs_in)
        # return records

    def open(self):
        if self.fd is None:
            open_mode = 'r'
            if self.mode == 'write': open_mode = 'w'
            self.fd = open(self.complete_pathname, open_mode)
        return self.fd

    def write(self, record):
        if self.mode == 'read':
            print('** Attempt to write to read-only file')
            raise Exception()
        if self.fd is None:
            open_mode = 'a'
            if self.mode == 'write': open_mode = 'w'
            self.fd = open(self.complete_pathname, open_mode)
        self.fd.write(record)

    def increment_recs_read(self, count=1):
        self._records_read += count

    def increment_recs_written(self, count=1):
        self._records_written += count

    def flush(self):
        if self.mode == 'read' or self.fd is None: return
        self.fd.flush()

    def close(self):
        if self.fd is None: return
        self.fd.close()

    @property
    def records_read(self):
        return self._records_read

    @property
    def records_written(self):
        return self._records_written


class FileControl(object):
    def __init__(self, base_medi_input_directory, base_output_directory, medi_dir_format, year_number, verify_exists = True):
        self.base_medi_input_directory = base_medi_input_directory
        self.base_output_directory = base_output_directory
        self.year_number = year_number

        #sample_input_directory = os.path.join(base_medi_input_directory, year_number)

        print('FileControl starting....')
        print('...base_medi_input_directory        = ', base_medi_input_directory)
        print('...year_number                      = ', year_number)
        #print('...sample_input_directory          = ', sample_input_directory)
        print('...base_output_directory            = ', base_output_directory)

        self.files = {}

        # input files
        input_files = [
            (MEDI_FILE_TOKENS.BENEFICARY,    'Beneficiary_Summary_File'),
            (MEDI_FILE_TOKENS.INPATIENT,     'Inpatient_Claims'),
            (MEDI_FILE_TOKENS.OUTPATIENT,    'Outpatient_Claims'),
            (MEDI_FILE_TOKENS.CARRIER,       'Carrier_Claims'),
            (MEDI_FILE_TOKENS.PARTD,         'pde_file'),
            (MEDI_FILE_TOKENS.SNF,           'SkilledNursingFacility_Claims'),
            (MEDI_FILE_TOKENS.DME,           'DurableMedicalEquipment_Claims'),
            (MEDI_FILE_TOKENS.HHA,           'HomeHealthAgency_Claims'),
            (MEDI_FILE_TOKENS.HOSPICE,       'Hospice_Claims')]
        for token, base_filename in input_files:
            self.files[token] = FileDescriptor(token, mode='read',
                                               directory_name=base_medi_input_directory,
                                               filename='{0}\{1}.csv'.format(year_number, base_filename),
                                               year_number=year_number,
                                               verify_exists=True,
                                               sort_required=True)

        # output files
        output_files = [
                'person',
                'observation',
                'observation_period',
                'specimen',
                'death',
                'visit_occurrence',
                'visit_cost',
                'condition_occurrence',
                'procedure_occurrence',
                'procedure_cost',
                'drug_exposure',
                'drug_cost',
                'device_exposure',
                'device_cost',
                'measurement',
                'location',
                'care_site',
                'provider',
                'payer_plan_period']

        for token in output_files:
            self.files[token] = FileDescriptor(token, mode='append',
                                               directory_name=self.base_output_directory,
                                               filename='{0}_{1}.csv'.format(token, year_number),
                                               year_number=year_number,
                                               verify_exists=False,
                                               sort_required=False)


        print('FileControl files:')
        print('-'*30)
        for ix, filedesc in enumerate(self.files):
            print('[{0}] {1}'.format(ix, self.files[filedesc]))

    def descriptor_list(self, which='all'):
        # someday I'll learn list comprehension
        l = {}
        if which == 'all':
            return self.files
        elif which == 'input':
            for fd in self.files.values():
                if fd.mode == 'read': l[fd.token] = fd
            return l
        elif which == 'output':
            for fd in self.files.values():
                if fd.mode != 'read': l[fd.token] = fd
            return l
        else:
            return {}

    def get_Descriptor(self, token):
        return self.files[token]

    def close_all(self):
        for token in self.files:
            filedesc = self.files[token]
            if filedesc.fd is not None:
                try:
                    filedesc.close()
                except:
                    pass

    def flush_all(self):
        for token in self.files:
            filedesc = self.files[token]
            if filedesc.fd is not None:
                try:
                    filedesc.flush()
                except:
                    pass

    def delete_all_output(self):
        for token in self.files:
            filedesc = self.files[token]
            if filedesc.mode != 'read':
                try:
                    os.unlink(filedesc.complete_pathname)
                except:
                    pass
