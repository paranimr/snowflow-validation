from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.snowpark import Session
from libs.pkg_submit import submit_snowpark_package_from_libs
import snowflake.snowpark.functions as f

def submit_snowpark_package_from_whl():
        hook = SnowflakeHook("snowpark-conn")
        conn_params = hook._get_conn_params()
        session = Session.builder.configs(conn_params).create()
        session.query_tag = "driver.sql"
        query = """
            select avg(reps_upper), max(reps_lower) 
            from sample;
            """
        df = session.sql(query)
        print(df)
        print(df.collect())

        # Using DataFrame
        session.query_tag = "driver.df"
        df = session.table("sample")
        print(df.agg(f.avg("reps_upper").alias("avg_upper"), f.min("reps_lower").alias("min_lower")).collect())
        session.close()

submit_snowpark_package_from_whl()
submit_snowpark_package_from_libs()