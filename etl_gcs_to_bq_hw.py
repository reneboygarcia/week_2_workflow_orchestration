# imports
from pathlib import Path
import urllib.request
import pandas as pd
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

print("Setup Complete")

# Download trip data from GCS
@task(log_prints=True, name="extract_from_gcs")
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("prefect-gcs-2023")
    gcs_block.get_directory(
        from_path=gcs_path, local_path="./data/"
    )  # the .. indicates the folder level
    return Path(f"./data/{gcs_path}")


# Data cleaning example
@task(log_prints=True, name="transform")
def transform(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    print(f"Pre: Missing passenger count: {df.passenger_count.isna().sum()}")
    print(f"Dtypes list: {df.dtypes}")
    return df


# Write DataFrame to BigQuery
@task(log_prints=True, name="write_bq")
def write_bq(df: pd.DataFrame) -> None:
    gcp_credentials_block = GcpCredentials.load("prefect-gcs-2023-creds")
    df.to_gbq(
        destination_table="prefect_de_2023_datasetid.yellow_ny_taxi_trips_table_2019",
        project_id="dtc-de-2023",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


# Child ETL flow to load data to BigQuery
@flow(log_prints=True, name="etl_gcs_to_bq")
def etl_gcs_to_bq(color: str = "yellow", year: int = 2019, month: int = 2):
    # step-by-step execution
    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)


# Main ETL flow
@flow(log_prints=True, name="parent_etl_gcs_to_bq")
def parent_etl_gcs_to_bq(
    color: str = "yellow", year: int = 2019, months: list[int] = [2, 3]
):
    for month in months:
        etl_gcs_to_bq(color, year, month)


# run main
if __name__ == "__main__":
    parent_etl_gcs_to_bq()
