# Origintrail Data Extractor
This project is designed to efficiently extract publishing addresses and their associated transactions from the Origintrail Parachain via the Subscan API. The extracted transactions are specifically those that pertain to the publishing of Knowledge Assets on the network. Once extracted, the data is swiftly channeled through Pgbouncer to ensure robust connection pooling before it's committed to a TimescaleDB instance hosted by Timescale, a cloud provider that specializes in PostgreSQL with TimescaleDB extensions. Visualization of this data is achieved seamlessly through Grafana dashboards which are updated every 10 seconds, providing real-time insights into the state and activity of the network.

## Key Components:
1. Data Extraction: Implemented in Python, this component constantly fetches data from the Subscan API every 45 seconds.
2. Connection Pooling: Pgbouncer assists in ensuring that the data extraction remains efficient and doesn't overwhelm the database with direct connections.
3. Data Storage: Timescale, a specialized cloud provider for timescaledb on PostgreSQL, is used for robust and scalable data storage.
4. Orchestration: Prefect, a modern data orchestration tool, ensures that the data extraction process runs smoothly and can handle potential failures gracefully. The Prefect agent is persistently hosted on a virtual machine to guarantee its continuous operation.
5. Visualization: Grafana provides an interactive dashboard, giving a clear visual representation of the data, updated every 10 seconds.


```bash
prefect deployment build ./origintrail-pipeline-prefect.py:main_flow -n "OriginTrail ETL"
```

```bash
prefect deployment apply main_flow-deployment.yaml
```