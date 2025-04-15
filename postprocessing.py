import datetime
import pandas as pd
import numpy as np
import geopandas as gpd
from azure.storage.blob import BlobServiceClient, ContainerClient
from io import BytesIO
import os

API_KEY = os.environ['AZURE_BLOB']

def azure_upload_df(container, df, filename, con, filepath=None ):
    
    """
    Upload DataFrame to Azure Blob Storage for given container
    Keyword arguments:
    container -- the container folder name 
    df -- the dataframe(df) object
    filename -- name of the file
    filepath -- the filename to use for the blob 
    con -- azure connection string
    """
    
    if filepath != None:
        blob_path = filepath + filename
    else:
        blob_path = filename
        
    # initialize client
    blob_service_client = BlobServiceClient(account_url=con)
    
    #specify file path
    blob_client = blob_service_client.get_blob_client(
    container=container, blob=blob_path
        )
    
    #convert dataframe to a string object
    output = df.to_csv(index=False, encoding="utf-8")
    
    #upload file
    # overwrite the data
    blob_client.upload_blob(data=output, blob_type="BlockBlob", overwrite=True)
    
    #close the client; we're done with it!
    blob_service_client.close()


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



df = arr_data_read(start_year = 2018, full_dataset = True)
df.columns
df.head()


# 2. Map charge type to numeric (felony=0, misdemeanor=1)
df["FelonyMisdemeanorSort"] = df["charge_1_type"].map({"F": 0, "M": 1})

# 3. Fill based on CHARGE1CLASS
felony_classes = ["1", "2", "3", "4", "M"]
misdemeanor_classes = ["A", "B", "C"]
warrant_descriptions = [
    "FUGITIVE FROM JUSTICE - OUT OF STATE WARRANT",
    "ISSUANCE OF WARRANT",
    "ISSUANCE OF WARRANT (ATTEMPT)",
    "ISSUANCE OF WARRANT (CONSPIRACY)",
    "ISSUANCE OF WARRANT (SOLICITATION)"
]

df.loc[df["FelonyMisdemeanorSort"].isna() & df["charge_1_class"].isin(felony_classes), "FelonyMisdemeanorSort"] = 0
df.loc[df["FelonyMisdemeanorSort"].isna() & df["charge_1_class"].isin(misdemeanor_classes), "FelonyMisdemeanorSort"] = 1
df.loc[df["FelonyMisdemeanorSort"].isna() & df["charge_1_description"].isin(warrant_descriptions), "FelonyMisdemeanorSort"] = 2
df["FelonyMisdemeanorSort"] = df["FelonyMisdemeanorSort"].fillna(3)

# 4. Label Felony/Misdemeanor
charge_map = {0: "Felony", 1: "Misdemeanor", 2: "Warrant", 3: "Other"}
df["FelonyMisdemeanor"] = df["FelonyMisdemeanorSort"].map(charge_map)

# 5. Class sort
# Custom mapping depending on class and FelonyMisdemeanorSort
# (same logic as IF blocks in SPSS)

# 6. Race recode
race_map = {
    "AMER INDIAN / ALASKAN NATIVE": 4,
    "ASIAN / PACIFIC ISLANDER": 3,
    "BLACK": 1,
    "BLACK HISPANIC": 2,
    "UNKNOWN / REFUSED": 4,
    "WHITE": 0,
    "WHITE HISPANIC": 2
}
df["DefendantRaceSort"] = df["race"].map(race_map)
race_label_map = {
    0: "White", 1: "Black", 2: "Hispanic", 3: "Asian", 4: "Another Race/Ethnicity"
}
df["DefendantRace"] = df["DefendantRaceSort"].map(race_label_map)

lookup_path = "data/CPD offense lookup - Arrests.csv"
lookup = pd.read_csv(lookup_path)

lookup.head()
df = df.merge(lookup,left_on=['charge_1_statute','charge_1_description'],right_on=['CHARGE1STATUTE', 'CHARGE1DESCRIPTION'], how='left')

df['ARRESTDATE'] = pd.to_datetime(df['arrest_date'], errors='coerce')



df['ArrestSort'] = 1
df['Arrest'] = 'All Arrests'
df['ArrestYear'] = df['ARRESTDATE'].dt.year
df['ARRESTDATE'] = df['ARRESTDATE'].dt.date

df.head()
df.info()
output_csv="arrest.csv"

azure_upload_df(container='data', df=df, filepath='/',\
                filename= output_csv, con=API_KEY)
