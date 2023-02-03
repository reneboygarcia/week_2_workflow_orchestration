# Week 2 | Homework
# imports
import os
from pathlib import Path
import urllib.request
import pandas as pd
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket

print("Setup Complete")

# read data from the web into Dataframe
@task(log_prints=True, name="read-df")
def fetch(dataset_url: str) -> pd.DataFrame:
    filename, _ = urllib.request.urlretrieve(dataset_url)
    df = pd.read_parquet(filename)
    return df


# Tweak DataFrame,
@task(log_prints=True, name="tweak-dataframe")
def tweak(df: pd.DataFrame) -> pd.DataFrame:
    df_tweak = df
    print(df_tweak.head(n=2))
    print(f"Columns Dtype: {df_tweak.dtypes}")
    print(f"No. of row: {df_tweak.shape[0]}")
    return df_tweak


# Write DataFrame to a specific folder after tweaking the DataFrame
@task(log_prints=True, name="write-to-local-file")
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    directory = Path(f"data/data/{color}")
    path_name = directory / f"{dataset_file}.parquet"
    try:
        os.makedirs(directory, exist_ok=True)
        df.to_parquet(path_name, compression="gzip")
    except OSError as error:
        print(error)
    return path_name


# Upload local parquet file to GCS
@task(log_prints=True)
def write_gcs(path: Path) -> None:
    gcp_block = GcsBucket.load("prefect-gcs-2023")
    gcp_block.upload_from_path(from_path=path, to_path=path)
    return


# main ETL function
@flow(log_prints=True, name="etl-web-to-gcs")
def etl_web_to_gcs() -> None:
    color = "green"
    year = 2020
    month = 11
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = (
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}.parquet"
    )

    # execution
    df = fetch(dataset_url)
    df_tweak = tweak(df)
    path = write_local(df_tweak, color, dataset_file)
    write_gcs(path)
    print("Loaded data to GCS...Hooray!")


# run
if __name__ == "__main__":
    etl_web_to_gcs()

# Source: https://prefecthq.github.io/prefect-gcp/

