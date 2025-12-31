#!/bin/bash

python3 -m venv .venv

source .venv/bin/activate 

pip install -r requirements.txt  

export AIRFLOW_HOME=$(pwd)/airflow
AIRFLOW_VERSION=3.1.3
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

cd dbt
dbt run
cd ..

airflow db migrate

cd metabase
docker build -t metabase-duckdb:latest .
docker run -d -p 3000:3000 -v $(pwd):/metabase-data  -v $(pwd)/../data:/data -e MB_PLUGINS_DIR=/plugins -e "MB_DB_FILE=/metabase-data/metabase.db" --name openaq-metabase metabase-duckdb:latest

cd ..

airflow dag-processor & echo $! > airflow_pids.txt
airflow scheduler & echo $! >> airflow_pids.txt
airflow triggerer & echo $! >> airflow_pids.txt
airflow api-server --port 8090 & echo $! >> airflow_pids.txt
