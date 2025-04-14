import datetime
import pandas as pd
import numpy as np
import geopandas as gpd
from azure.storage.blob import BlobServiceClient, ContainerClient
from io import BytesIO


def arr_data_read(start_year = 2018, full_dataset = True):
    
    """
    Pulling in City of Chicago Arrest data; returns pandas dataframe of arrests.
    start_year: input starting year for requested data
    full_dataset: choose to pull in full dataset or small subset of data
    """

    

    if full_dataset == True:
        print("Pulling full dataset")
        limit = 20000000
    else:
        print("Small subset")
        limit = 200
    
    # initialize datatime object
    today = datetime.date.today()
    
    # getting current year
    current_yr = today.year
    # initialize a list
    arr_df_list = []
    
    # for each year between 2018 and current year, pull in arrest data    
    for year in range(start_year, current_yr + 1):
        arr_data_yr = pd.read_csv(
            f'https://data.cityofchicago.org/resource/dpt3-jri9.csv?$limit={limit}&$where=arrest_date%20between%20%27{year}-01-01T00:00:00%27%20and%20%27{year}-12-31T23:59:59%27'
    )
        arr_df_list.append(arr_data_yr)
        
    # concat lists of data from each list (dataframe of yearly arrests)
    arr_df = pd.concat(arr_df_list, ignore_index=True)
    print(arr_df.shape)

    return arr_df


import geopandas
df = arr_data_read(start_year = 2018, full_dataset = True)
df.columns
df.head()
df['FelonyMisdemeanorSort'] = np.nan

df.loc[df['charge_1_type'] == 'F', 'FelonyMisdemeanorSort'] = 0
df.loc[df['charge_1_type'] == 'M', 'FelonyMisdemeanorSort'] = 1

felony_classes = ['1', '2', '3', '4', 'M']
misdemeanor_classes = ['A', 'B', 'C']

df.loc[df['FelonyMisdemeanorSort'].isna() & df['charge_1_class'].isin(felony_classes), 'FelonyMisdemeanorSort'] = 0
df.loc[df['FelonyMisdemeanorSort'].isna() & df['charge_1_class'].isin(misdemeanor_classes), 'FelonyMisdemeanorSort'] = 1

warrant_descs = [
    'FUGITIVE FROM JUSTICE - OUT OF STATE WARRANT',
    'ISSUANCE OF WARRANT',
    'ISSUANCE OF WARRANT (ATTEMPT)',
    'ISSUANCE OF WARRANT (CONSPIRACY)',
    'ISSUANCE OF WARRANT (SOLICITATION)'
]
df.loc[df['FelonyMisdemeanorSort'].isna() & df['charge_1_description'].isin(warrant_descs), 'FelonyMisdemeanorSort'] = 2
df.loc[df['FelonyMisdemeanorSort'].isna(), 'FelonyMisdemeanorSort'] = 3

felony_map = {
    0: 'Felony',
    1: 'Misdemeanor',
    2: 'Warrant',
    3: 'Other'
}
df['FelonyMisdemeanor'] = df['FelonyMisdemeanorSort'].map(felony_map)
charge_map = {
    (0, 'M'): 0,
    (0, 'X'): 1,
    (0, '1'): 2,
    (0, '2'): 3,
    (0, '3'): 4,
    (0, '4'): 5,
    (1, 'A'): 6,
    (1, 'B'): 7,
    (1, 'C'): 8,
    (2, None): 9,
    (3, None): 10,
}

df['ChargeClassSort'] = np.nan
for (fms_val, class_val), sort_val in charge_map.items():
    if class_val:
        df.loc[(df['FelonyMisdemeanorSort'] == fms_val) & (df['charge_1_class'] == class_val), 'ChargeClassSort'] = sort_val
    else:
        df.loc[df['FelonyMisdemeanorSort'] == fms_val, 'ChargeClassSort'] = sort_val


df.loc[df['ChargeClassSort'].isna() & (df['FelonyMisdemeanorSort'] == 0), 'ChargeClassSort'] = 5
df.loc[df['ChargeClassSort'].isna() & (df['FelonyMisdemeanorSort'] == 1), 'ChargeClassSort'] = 8

charge_class_map = {
    0: 'Murder',
    1: 'Class X Felony',
    2: 'Class 1 Felony',
    3: 'Class 2 Felony',
    4: 'Class 3 Felony',
    5: 'Class 4 Felony',
    6: 'Class A Misd.',
    7: 'Class B Misd.',
    8: 'Class C Misd.',
    9: 'Warrant',
    10: 'Other'
}
df['ChargeClass'] = df['ChargeClassSort'].map(charge_class_map)

race_map = {
    'AMER INDIAN / ALASKAN NATIVE': 4,
    'ASIAN / PACIFIC ISLANDER': 3,
    'BLACK': 1,
    'BLACK HISPANIC': 2,
    'UNKNOWN / REFUSED': 4,
    'WHITE': 0,
    'WHITE HISPANIC': 2
}
df['DefendantRaceSort'] = df['race'].map(race_map)

defendant_race_map = {
    0: 'White',
    1: 'Black',
    2: 'Hispanic',
    3: 'Asian',
    4: 'Another Race/Ethnicity'
}
df['DefendantRace'] = df['DefendantRaceSort'].map(defendant_race_map)

lookup_path = "data/CPD offense lookup - Arrests.csv"
lookup = pd.read_csv(lookup_path)

lookup.head()
df = df.merge(lookup,left_on=['charge_1_statute','charge_1_description'],right_on=['CHARGE1STATUTE', 'CHARGE1DESCRIPTION'], how='left')
"data/CPD offense lookup - Arrests.csv"
df.head()
df.info()
df['ARRESTDATE2'] = df['arrest_date']
df['arrest_date'] = pd.to_datetime(df['ARRESTDATE2'], format='%m/%d/%Y', errors='coerce')
df.drop(columns=['ARRESTDATE2'], inplace=True)
df['ArrestSort'] = 1
df['Arrest'] = 'All Arrests'
df['ArrestYear'] = df['arrest_date'].dt.year
output_csv="arrest.csv"
## df.to_csv(output_csv, index=False, encoding='utf-8')
