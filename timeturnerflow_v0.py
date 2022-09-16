from metaflow import FlowSpec, step, IncludeFile

import argparse
import datetime
import logging
import pathlib
import pandas as pd
import time

from airflow.models import Variable
from common import snowflake_utils
from typing import Dict, List

# Set env variables
SNOWFLAKE_ACCOUNT_NAME: str = Variable.get('SNOWFLAKE_ACCOUNT_NAME')  # 'hbomax.us-east-1'
CURRENT_PATH: str = pathlib.Path(__file__).parent.absolute()
TARGET_DATE: str = (datetime.datetime.today()).strftime('%Y-%m-%d')

    
def script_path(filename):
    """
    A convenience function to get the absolute path to a file in this
    tutorial's directory. This allows the tutorial to be launched from any
    directory.

    """
    import os

    filepath = os.path.join(os.path.dirname(__file__))
    return os.path.join(filepath, filename)

logger = logging.getLogger()

def to_csv_s3(bucket, key, df_content):
    client = boto3.client('s3')
    csv_buffer = StringIO()
    df_content.to_csv(csv_buffer)
    client.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
    logger.info(f'Saved to {key}')

def load_query(filename: str, **kwargs) -> str:
    """
    Load a query from disk and fill templates parameters

    :param filename: name of the file that contains the query
    """
    with open(filename, 'r') as f:
        query = f.read()
    query = query.format(**kwargs)
    return query

def connect_to_snowflake(snowflake_account_name, database, schema, warehouse, role, snowflake_env):
    """
    Connect to snowflake

    :param database: name of the database
    :param schema: name of the schema
    :param warehouse: name of the warehouse
    :param role: name of the role
    :param snowflake_env: environment used in Snowflake
    """
    logger.info('Environment defined:')
    logger.info(f'database: {database}')
    logger.info(f'schema: {schema}')
    logger.info(f'warehouse: {warehouse}')
    logger.info(f'role: {role}')
    logger.info(f'snowflake env: {snowflake_env}')

    connection = snowflake_utils.connect(
        snowflake_account_name,
        database,
        schema,
        warehouse,
        role,
        snowflake_env,
        None
    )
    cursor = connection.cursor()
    
    return cursor

def execute_query(query_name, database, schema, cursor):
    """
    Execute a query on snowflake

    :param query_name: name of the query sql file 
    :param cursor: snowflake cursor established by connect_to_snowflake
    """
    start_time = time.time()
    path = CURRENT_PATH
    logger.info(f'Running query {path}/{query_name}, ')
    
    query = load_query(
        f'{path}/{query_name}',
        database=database,
        schema=schema
    )
    
    cursor.execute(query)
    df = pd.DataFrame(cursor.fetchall(), columns = [desc[0] for desc in cursor.description])
    df.columns= df.columns.str.lower()
    
    end_time = time.time()
    logger.info(f'Query run time: {end_time - start_time} seconds')
    
    return df


class TimeTurnerFlow(FlowSpec):
    """
    A flow to generate some statistics about the movie genres.

    The flow performs the following steps:
    1) Ingests a CSV into a Pandas Dataframe.
    2) Fan-out over genre using Metaflow foreach.
    3) Compute quartiles for each genre.
    4) Save a dictionary of genre specific statistics.

    """
    
#     parser = argparse.ArgumentParser()
#     parser.add_argument('--SNOWFLAKE_ENV', required=True)
#     parser.add_argument('--WAREHOUSE', required=True)
#     parser.add_argument('--ROLE', required=True)
#     parser.add_argument('--DATABASE', required=True)
#     parser.add_argument('--SCHEMA', required=True)
#     parser.add_argument('--OUTPUT_BUCKET', required=True)
#     args = parser.parse_args()
    
    cursor = connect_to_snowflake(
                snowflake_account_name=SNOWFLAKE_ACCOUNT_NAME
                database="MAX_DEV",
                schema="CONTENT_DATASCIENCE",
                warehouse="MAX_DATASCIENCE_DEV",
                role="MAX_ETL_DEV",
                snowflake_env="sf_max_dev")
    database =  "MAX_DEV"
    schema = "CONTENT_DATASCIENCE",
    
    @step
    def start(self):
        """
        The start step:
        1) Loads the movie metadata into pandas dataframe.
        2) Finds all the unique genres.
        3) Launches parallel statistics computation for each genre.

        """
        query_name = 'timeturner_endpoint_metric.sql'
        _ = execute_query(query_name, database, schema, cursor)
        self.next(self.end)

    @step
    def end(self):
        """
        End the flow.

        """
        pass


if __name__ == "__main__":
    TimeTurnerFlow()
