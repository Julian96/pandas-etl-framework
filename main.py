import os
import shutil
import pandas as pd
import pandasql as ps
import src.PandasETLHelpers.MetaColumnHelpers as mch
import src.PandasETLHelpers.SCDHelpers as scd

key_columns = ['Lastname','Firstname']
current_path = './data/current/current.parquet'
first_run_delta_path = './data/grades_delta_old.csv'
second_run_delta_path = './data/grades_delta_new.csv'

first_run_full_path = './data/grades_full_old.csv'
second_run_full_path = './data/grades_full_new.csv'

def meta_columns_delta(first_run_file_path, second_run_file_path):
    if os.path.exists(current_path):
        shutil.rmtree(current_path)
    first_run_data_df = pd.read_csv(first_run_file_path)
    first_run_ts = '2020-03-20 13:00:00'
    first_run_currents = mch.create_currents(first_run_ts)
    first_run_data_df = mch.add_meta_columns(first_run_data_df,first_run_currents,key_columns)

    first_run_current_df = scd.read_parquet_df(current_path)
    if first_run_current_df is None:
        first_run_current_df = pd.DataFrame(columns=first_run_data_df.columns)
    current_df = ps.sqldf('select a.* from first_run_data_df a LEFT JOIN first_run_current_df b ON a.KEY_HASH = b.KEY_HASH AND a.RECORD_HASH = b.RECORD_HASH WHERE b.KEY_HASH IS NULL')
    current_df = pd.concat([current_df,first_run_current_df])
    current_df.to_parquet(current_path,partition_cols=key_columns,engine='fastparquet')
    
    second_run_data_df = pd.read_csv(second_run_file_path)
    second_run_ts = '2021-04-15 11:12:30'
    second_run_currents = mch.create_currents(second_run_ts)
    second_run_data_df = mch.add_meta_columns(second_run_data_df,second_run_currents,key_columns)

    second_run_current_df = scd.read_parquet_df(current_path)
    if second_run_current_df is None:
        second_run_current_df = pd.DataFrame(columns=second_run_data_df.columns)
    current_df = ps.sqldf('select a.* from second_run_data_df a LEFT JOIN second_run_current_df b ON a.KEY_HASH = b.KEY_HASH AND a.RECORD_HASH = b.RECORD_HASH WHERE b.KEY_HASH IS NULL')
    current_df = pd.concat([current_df,second_run_current_df])
    current_df.to_parquet(current_path,partition_cols=key_columns,engine='fastparquet')

    final_df = scd.read_parquet_df(current_path)
    return final_df

    
    


if __name__ == '__main__':
    delta_final_df = meta_columns_delta(first_run_delta_path,second_run_delta_path)
    full_final_df = meta_columns_delta(first_run_full_path,second_run_full_path)
    print(delta_final_df)
    print(full_final_df)
    print(delta_final_df.equals(full_final_df))