# Origintrail Publisher Data Extractor
This project is designed to efficiently extract publishing addresses and their associated transactions from the Origintrail Parachain via the Subscan API. The extracted transactions are specifically those that pertain to the publishing of Knowledge Assets on the network. Once extracted, the data is swiftly channeled through Pgbouncer to ensure robust connection pooling before it's committed to a TimescaleDB instance hosted by Timescale, a cloud provider that specializes in PostgreSQL with TimescaleDB extensions. Visualization of this data is achieved seamlessly through Grafana dashboards which are updated every 10 seconds, providing real-time insights into the state and activity of the network.

## Key Components:
1. Data Extraction: Implemented in Python, this component constantly fetches data from the Subscan API every 45 seconds.
2. Connection Pooling: Pgbouncer assists in ensuring that the data extraction remains efficient and doesn't overwhelm the database with direct connections.
3. Data Storage: Timescale, a specialized cloud provider for timescaledb on PostgreSQL, is used for robust and scalable data storage.
4. Orchestration: Prefect, a modern data orchestration tool, ensures that the data extraction process runs smoothly and can handle potential failures gracefully. The Prefect agent is persistently hosted on a virtual machine to guarantee its continuous operation.
5. Visualization: Grafana provides an interactive dashboard, giving a clear visual representation of the data, updated every 10 seconds.

## Setup & Configuration:

1. Python Environment: Set up your Python environment with required libraries.

```bash
pip install -r requirements.txt
```
2. Database Configuration: Configure Pgbouncer with appropriate settings to connect to your Timescale instance.

3. (Optional): If you wish to initialize your database with past transactions, run the following command. This will fetch all transactions from the Subscan API and commit them to the database. This process may take a while depending on the number of transactions.
```bash
python initialize_database.py
```
4. Prefect Configuration: Ensure that the Prefect agent is correctly set up on the VM with necessary environment variables.

    1. Create a yaml file for the Prefect deployment configuration.
    ```bash
    prefect deployment build ./origintrail-pipeline-prefect.py:main_flow -n "OriginTrail ETL"
    ```
   2. Create Prefect deployment
    ```bash
    prefect deployment apply main_flow-deployment.yaml
    ```
   3. Start Prefect agent
   ```bash
   prefect agent start
   ```
5. Grafana Dashboard: Import the pre-configured dashboard or set up a new one to connect to Timescale and display the required metrics.
    1. SQL query to show individual publisher's publishes per user's time bucket:
    ```sql
    SELECT
    time_bucket('$bucket_interval', create_at) as time,
    pubber,
    count(*) as amount_published
    FROM publishes 
    WHERE pubber in ($Publisher_Address)
    AND create_at >= $__timeFrom()::timestamptz AND create_at < $__timeTo()::timestamptz
    GROUP BY time_bucket('$bucket_interval',create_at), pubber
    ORDER BY time_bucket('$bucket_interval',create_at), pubber;
    ```
   2. SQL query to show average Trac publish value per user's time bucket.
   ```sql
    SELECT
    time_bucket('$bucket_interval', create_at) as time,
    pubber,
    avg(value) as average_trac_price
    FROM publishes 
    WHERE pubber in ($Publisher_Address)
    AND create_at >= $__timeFrom()::timestamptz AND create_at < $__timeTo()::timestamptz
    GROUP BY time_bucket('$bucket_interval',create_at), pubber
    ORDER BY time_bucket('$bucket_interval',create_at), pubber;
    ```
   