from setuptools import find_packages, setup

setup(
    name="jaffle",
    packages=find_packages(exclude=["jaffle_tests"]),
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
