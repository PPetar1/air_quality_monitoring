🌍 
# Air Quality Data Pipeline

A hands-on data engineering project that demonstrates modern ETL/ELT pipelines using [openaq](https://openaq.org/). Built to showcase data engineering skills with a production-ready stack using entirely free and open-source tools.


# What This Project Is

This is a learning portfolio project that implements a complete data pipeline from extraction to visualization, simulating real-world data engineering scenarios. It focuses on:

*Real data:* Live air quality metrics from the OpenAQ API

*Modern stack:* **Python, Airflow, dbt, Metabase**    

*Production patterns:* Incremental loads, data quality checks, asynchronous extraction    

*Free tools:* Everything runs locally using open source tools  


# Installation 

Requires python >3.13 to be installed on your machine https://www.geeksforgeeks.org/python/download-and-install-python-3-latest-version/  

### Linux/macOS
```
    git clone https://github.com/PPetar1/air_quality_monitoring
    cd air_quality_monitoring
    python3 -m venv .venv
    source .venv/bin/activate 
    pip install -r requirements.txt  
    export AIRFLOW_HOME=$(pwd)/airflow
    AIRFLOW_VERSION=3.1.3
    PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
    pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
    airflow db migrate
    cd metabase
    docker build -t metabase-duckdb:latest .
    docker run -d -p 3000:3000 -v $(pwd):/metabase-data  -v $(pwd)/../data:/data -e MB_PLUGINS_DIR=/plugins -e "MB_DB_FILE=/metabase-data/metabase.db" --name metabase metabase-duckdb:latest
    airflow dag-processor & echo $! > airflow_pids.txt
    airflow scheduler & echo $! >> airflow_pids.txt
    airflow triggerer & echo $! >> airflow_pids.txt
    airflow api-server --port 8090 & echo $! >> airflow_pids.txt
```

To stop the processes after use  

```
    docker stop metabase
    while read pid; do [[ $(ps -p $pid -o comm= 2>/dev/null) == "airflow" ]] && kill $pid; done < airflow_pids.txt
```


### Windows
```
    TODO  
```

After this you can access metabase by visiting http://localhost:3000 and airflow by visiting http://localhost:8090 in your web browser  


# Project Status

Current Phase: Documentation & Testing   
