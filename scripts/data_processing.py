# data_processing.py

import pandas as pd
import os

PROCESSED_DATA_DIR = 'data/processed'

def combine_data(*data_frames):
    combined_data = pd.concat(data_frames, ignore_index=True)
    duplicates_count = len(combined_data) - len(combined_data.drop_duplicates())
    combined_data.drop_duplicates(inplace=True)
    combined_data['id'] = range(1, len(combined_data) + 1)
    combined_data['id'] = combined_data['id'].astype(int)
    return combined_data, duplicates_count

def save_combined_data_to_csv(combined_data, file_name, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    file_path = os.path.join(output_dir, file_name)
    combined_data.to_csv(file_path, index=False, encoding='utf-8-sig')
    print(f"Datos combinados guardados en '{file_path}'.")
    return file_path