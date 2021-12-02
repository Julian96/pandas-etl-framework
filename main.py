import os
import shutil
import time
import pandas as pd
import pandasql as ps
import src.PandasETLHelpers.MetaColumnHelpers as mch
import src.PandasETLHelpers.SCDHelpers as scd

key_columns = ['Lastname','Firstname']
current_path = './data/current/current.parquet'
    
delta_sql_string = 'select a.* from new_data_df a LEFT JOIN current_df b ON a.KEY_HASH = b.KEY_HASH AND a.RECORD_HASH = b.RECORD_HASH WHERE b.KEY_HASH IS NULL'

def meta_column_historization(file_path):
    currents = mch.create_currents()
    new_data_df = pd.read_csv(file_path)
    new_data_df = mch.add_meta_columns(new_data_df,currents,key_columns)

    current_df = scd.read_parquet_df(current_path)
    if current_df is None:
        current_df = pd.DataFrame(columns=new_data_df.columns)
    current_df_delta = ps.sqldf(delta_sql_string)
    current_df = pd.concat([current_df,current_df_delta])
    current_df.to_parquet(current_path,partition_cols=key_columns,engine='fastparquet')

def simulate_runs(run_data_dict):
    if os.path.exists(current_path):
        shutil.rmtree(current_path)
    for run in run_data_dict:
        meta_column_historization(run_data_dict[run])
        time.sleep(2)

    final_df = scd.read_parquet_df(current_path)
    return final_df

if __name__ == '__main__':

    first_run_delta_path = './data/grades_delta_old.csv'
    second_run_delta_path = './data/grades_delta_new.csv'
    delta_run_data_dict = {
        'first_run' : first_run_delta_path,
        'second_run' : second_run_delta_path
    }
    delta_final_df = simulate_runs(delta_run_data_dict)
    print(delta_final_df)

    
    first_run_full_path = './data/grades_full_old.csv'
    second_run_full_path = './data/grades_full_new.csv'
    full_run_data_dict = {
        'first_run' : first_run_full_path,
        'second_run' : second_run_full_path
    }
    full_final_df = simulate_runs(full_run_data_dict)
    print(delta_final_df)