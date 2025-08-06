from dagster import Definitions, load_assets_from_modules

from kaggle_pipeline import assets  # noqa: TID252

# all_assets = load_assets_from_modules([assets])

# defs = Definitions(
#     assets=all_assets,
# )



from dagster import Definitions, job, op
import pandas as pd
from sqlalchemy import create_engine
import kaggle

@op
def download_data() -> pd.DataFrame:
    # need to authenticate with Kaggle json file
    kaggle.api.authenticate()
    kaggle.api.dataset_download_files('zynicide/wine-reviews', path = "data/", unzip=True)
    df = pd.read_csv("data/winemag-data-130k-v2.csv", index_col=0)
    return df

@op
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates() 
    df = df.dropna(subset=['country','points'])
    df['price'] = df['price'].fillna(df['price'].mean())
    return df

@op
def upload_to_neon(df: pd.DataFrame):
    with open("connection_string.txt", "r") as file_object:
        key = file_object.read()
        
    engine = create_engine(key)
    df.to_sql("wine_data", engine, if_exists='replace', index=False)

@job
def etl_job():
    upload_to_neon(clean_data(download_data()))

defs = Definitions(jobs=[etl_job])

