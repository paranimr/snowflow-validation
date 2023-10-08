from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from snowflake.snowpark import Session
from libs.submit import submit_snowpark_stanalone_from_libs
import snowflake.snowpark.functions as f
from datetime import datetime
import sys  

def submit_snowpark_standalone():
        hook = SnowflakeHook("snowpark-conn")
        conn_params = hook._get_conn_params()
        session = Session.builder.configs(conn_params).create()
        session.query_tag = "dag.sql"
        # Using SQL
        query = """
            select avg(reps_upper), avg(reps_lower) 
            from sample;
            """
        df = session.sql(query)
        print(df)
        print(df.collect())

        # Using DataFrame
        session.query_tag = "dag.df"
        df = session.table("sample")
        print(df.agg(f.avg("reps_upper").alias("avg_upper"), f.avg("reps_lower").alias("avg_lower")).collect())
        session.close()

def submit_snowpark_application(application,py_files,task_id):
    def execute():
        hook = SnowflakeHook('snowpark-conn')
        conn = hook.get_conn()
        from snowflake.snowpark import Session
        import sys
        sys.path.append(py_files)
        session = Session.builder.config("connection", conn).create() 
        exec(open(application).read())
    return PythonOperator(task_id=task_id, python_callable=execute)

with DAG("dag_ingest", start_date=datetime(2023,10,5), schedule_interval='@daily', catchup=False) as dag:
    submit_snowpark_standalone = PythonOperator(python_callable=submit_snowpark_standalone, task_id = "submit_snowpark_standalone" )
    submit_snowpark_stanalone_from_libs = PythonOperator(python_callable=submit_snowpark_stanalone_from_libs, task_id = "submit_snowpark_stanalone_from_libs")
    submit_snowpark_application = submit_snowpark_application(application ='/opt/airflow/dags/park_driver.py',py_files='/opt/airflow/dags/dist/MLFLib-1.0-py3.10.egg', task_id='submit_snowpark_application')
    submit_snowpark_standalone  >> submit_snowpark_stanalone_from_libs >> submit_snowpark_application
