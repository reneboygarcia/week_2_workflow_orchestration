# Week 2 | Homework
# from prefect orion
# import
from prefect.infrastructure.docker import DockerContainer
from prefect.deployments import Deployment
from parametric_web_to_gcs_hw import etl_parent_flow
from prefect.filesystems import GitHub

# Set-up Infrastructure
github_block = GitHub.load("prefect-github-block")


# https://docs.prefect.io/api-ref/prefect/deployments/#prefect.deployments.Deployment.build_from_flow

gsc_git_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="github-flow-hw-q4",
    storage=github_block,
)

print(
    "Successfully deployed Prefect Github Block. Check Prefect Orion [127.0.0.1:4200] then Deployments"
)

# execute
if __name__ == "__main__":
    gsc_git_dep.apply()

# prefect deployment run etl-parent/github-flow-hw-q4 --params '{"color":"green", "year":2020, "months":[11]}'
