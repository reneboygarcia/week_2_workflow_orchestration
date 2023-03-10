from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

# Load GCP Credentials block 
gcp_cred_block = GcpCredentials.load("prefect-gcs-2023-creds")

# Define the GcsBucket 

gcs_bucket = GcsBucket(bucket="prefect-de-2023", 
    gcp_credentials=gcp_cred_block)

gcs_bucket.save("prefect-gcs-block", overwrite=True)
