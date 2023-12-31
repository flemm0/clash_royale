from setuptools import find_packages, setup

with open('requirements.txt', 'r') as f:
    required = f.read().splitlines()

setup(
    name="clash_royale_etl_project",
    packages=find_packages(exclude=["clash_royale_etl_project_tests"]),
    install_requires=[
        required,
        "dagster",
        "dagster-cloud",
        "kaggle"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
