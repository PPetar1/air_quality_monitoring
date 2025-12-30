import pendulum

from airflow.sdk import dag, task

@dag(
    dag_id="openaq_dbt",
    schedule=None,
    catchup=False,
    description="Utility dag to run dbt only.",
    tags=["openaq", "dbt"],
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
