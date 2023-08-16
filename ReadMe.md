```bash
prefect deployment build ./origintrail-pipeline-prefect.py:main_flow -n "OriginTrail ETL"
```

```bash
prefect deployment apply main_flow-deployment.yaml
```