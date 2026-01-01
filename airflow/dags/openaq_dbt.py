"""
Utility dag that runs only dbt related tasks

Consists of following tasks:   
1. dbt_run_bronze - executes dbt bronze models, moving the data from parquet files 
    to duckDB database located at data/warehouse.db
2. dbt_run_silver - executes dbt silver models on duckDB database
3. dbt_run_gold - executes dbt gold models on duckDB database
4. dbt_test - runs all the data quality tests    

Intended to be run manually
"""

from airflow.sdk import dag, task

@dag(
    dag_id="openaq_dbt",
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    description="Utility dag to run dbt only.",
    tags=["openaq", "dbt", "dbt test"],
)
def openaq_dbt():
    @task.bash()
    def dbt_run_bronze():
        return "cd $AIRFLOW_HOME/../dbt && dbt run --select bronze"
    
    @task.bash()
    def dbt_run_silver():
        return "cd $AIRFLOW_HOME/../dbt && dbt run --select silver"

    @task.bash()
    def dbt_run_gold():
        return "cd $AIRFLOW_HOME/../dbt && dbt run --select gold"

    @task.bash()
    def dbt_test():
        return "cd $AIRFLOW_HOME/../dbt && dbt test"

    dbt_run_bronze = dbt_run_bronze()
    dbt_run_silver = dbt_run_silver()
    dbt_run_gold = dbt_run_gold()

    dbt_test = dbt_test()
    (
        dbt_run_bronze
        >> dbt_run_silver
        >> dbt_run_gold
        >> dbt_test
    )

openaq_dbt()
