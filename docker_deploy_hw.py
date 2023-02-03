# Week 2 | Homework
# from prefect orion
# import
from prefect.infrastructure.docker import DockerContainer
from prefect.deployments import Deployment
from parametric_web_to_gcs_hw import etl_parent_flow

# Set-up Infrastructure
docker_block = DockerContainer.load("prefectdockerblock")

# Deploy the docker
# https://docs.prefect.io/api-ref/prefect/deployments/#prefect.deployments.Deployment.build_from_flow

docker_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="docker-flow-hw-q3",
    infrastructure=docker_block,
)

print("Successfully deployed. Check Prefect Orion [127.0.0.1:4200] then Deployments")

# execute
if __name__ == "__main__":
    docker_dep.apply()

# prefect deployment run etl-parent/docker-flow-hw-q3 --params '{"color":"green", "year":2020, "months":[1]}'
# prefect deployment run etl-parent/docker-flow-hw-q3 --params '{"color":"yellow", "year":2019, "months":[2,3]}'