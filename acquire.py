from zipfile import ZipFile
import os
import pandas as pd

def extract_zip(zip_path, extract_to):
    """Extracts a zip file to the specific directory"""

    if not os.path.exists(zip_path):
        raise FileNotFoundError(f'Zip file not found at: {zip_path}')
    
    try:
        os.makedirs(extract_to, exist_ok=True)
    except Exception as e:
        raise Exception(f'Error creating extraction directory: {e}')

    with ZipFile(zip_path, 'r') as ref:
        ref.extractall(extract_to)
        return ref.namelist()

def meta():
    users_cols = ['user_id', 'age', 'sex', 'occupation', 'zip_code']
    users = pd.read_csv('data/ml-100k/u.user', sep='|', names=users_cols, encoding='latin-1')
    print(users.head())
    print(users.info())
    print(users.describe())
    
if __name__=='__main__':
    zip_path=r'C:\Users\USER\recomendation_system\zipped_data\ml-100k.zip'
    extract_to=r'C:\Users\USER\recomendation_system\data'

    try:
        extract_zip(zip_path,extract_to)
        print('✅ Data extracted successfully.')
    except Exception as e:
        raise Exception(f'Error during extraction: {e}')