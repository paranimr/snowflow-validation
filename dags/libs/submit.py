from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.snowpark import Session
import snowflake.snowpark.functions as f

def submit_snowpark_stanalone_from_libs():
        hook = SnowflakeHook("snowpark-conn")
        conn_params = hook._get_conn_params()
        session = Session.builder.configs(conn_params).create()
        session.query_tag = "libs.sql"
        query = """
            select max(reps_upper), min(reps_lower) 
            from sample;
            """
        df = session.sql(query)
        print(df)
        print(df.collect())

        # Using DataFrame
        session.query_tag = "libs.df"
        df = session.table("sample")
        print(df.agg(f.max("reps_upper").alias("max_upper"), f.min("reps_lower").alias("min_lower")).collect())
        session.close()
