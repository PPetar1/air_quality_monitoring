🌍 **Air Quality Data Pipeline**

A hands-on data engineering project that demonstrates modern ETL/ELT pipelines using [openaq](https://openaq.org/). Built to showcase data engineering skills with a production-ready stack using entirely free and open-source tools.


**What This Project Is**

This is a learning portfolio project that implements a complete data pipeline from extraction to visualization, simulating real-world data engineering scenarios. It focuses on:

*Real data:* Live air quality metrics from the OpenAQ API

*Modern stack:* Python, Airflow, dbt, Grafana?  

*Production patterns:* Incremental loads, data quality checks  

*Free tools:* Everything runs locally or on free cloud tiers  


**Installation**  
Requires python >3.13 to be installed on your machine https://www.geeksforgeeks.org/python/download-and-install-python-3-latest-version/  

Clone the project with
``` 
   git clone https://github.com/PPetar1/air_quality_monitoring
```

Linux/macOS
```
    python3 -m venv .venv
    source .venv/bin/activate 
    pip install -r requirements.txt  
    export AIRFLOW_HOME=$(pwd)/airflow
    AIRFLOW_VERSION=3.1.3
    PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
    pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
    airflow db migrate
    airflow api-server --port 8080
```

Windows  
```
    TODO  
```

After this you can access airflow by visiting http://localhost:8080/

You can run individual scripts using 
```
    python3 scripts/script_name.py  
```

**Project Status**

Current Phase: Setup & Initial Extraction  
Next Phase: Transformations with dbt


Note: This project is actively being developed as a learning exercise. The codebase will evolve as features are implemented.

