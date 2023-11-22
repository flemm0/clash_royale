from setuptools import find_packages, setup

setup(
    name="clash_royale_etl_project",
    packages=find_packages(exclude=["clash_royale_etl_project_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
