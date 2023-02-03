# Week 2 | Homework
# from prefect orion
# import
from prefect_gcp.cloud_storage import GcsBucket
from prefect.deployments import Deployment
from etl_gcs_to_bq_hw import parent_etl_gcs_to_bq

# Set-up Infrastructure
gcs_block = GcsBucket.load("prefect-gcs-2023")

# https://docs.prefect.io/api-ref/prefect/deployments/#prefect.deployments.Deployment.build_from_flow

gcs_dep = Deployment.build_from_flow(
    flow=parent_etl_gcs_to_bq,
    name="docker-flow-hw-q3",
    infrastructure=gcs_block,
)

print("Successfully deployed. Check Prefect Orion [127.0.0.1:4200] then Deployments")

# execute
if __name__ == "__main__":
    gcs_dep.apply()

# prefect deployment run parent_etl_gcs_to_bq/docker-flow-hw-q3 --params '{"color":"yellow", "year":2019, "months":[2,3]}'