from setuptools import find_packages, setup

setup(
    name="publisher_dag",
    packages=find_packages(exclude=["publisher_dag_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "RUST",
        "cargo",
        "duckdb",
        "dagster_duckdb",
        "dagster_duckdb_pandas",
        "pandas",
        "polars",
        "web3",
        "requests",
        "python-dotenv",
        "sqlescapy"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
