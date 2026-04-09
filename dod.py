from imports import *
from tat_calculator import run_tat_calculation as tat_cal


def main(final_df, dod_data):
    # dod_df = tat_cal.main(dod_data)

    ## this is where join with final df will happen
    ## If dod_df returns days count column for each stage then a combined days_bucket will be required to be calculated before joining
    # final_df['Days Bucket'] = final_df['po_razin_id'].map(dod_df.set_index('po_razin_id')['days_bucket']).fillna(0)
    
    return final_df