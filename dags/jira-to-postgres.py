import datetime

from airflow.sdk import DAG, Variable
# Postgres Operator was replaced by SQLExecuteQueryOperator
# see git blame on this file
# https://github.com/apache/airflow/blob/providers-postgres/6.2.0/providers/postgres/tests/system/postgres/example_postgres.py
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator

import pandas as pd
from jira import JIRA

# Login to JIRA
jira_scrape_api_token = Variable.get("jira-scrape-api-token")
jira_options = {'server': 'https://cpcs.atlassian.net'}
jira = JIRA(options=jira_options, basic_auth=("serhii.ostrikov@cpcs.ws", jira_scrape_api_token))

def jira_scrape_users_api(ds, **kwargs):
    jira.issues
    print(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'

with DAG(
    dag_id="jira_users_to_postgres",
    start_date=datetime.datetime(2021, 1, 1),
    schedule="@daily",
):
    jira_scrape_users_api= PythonOperator(
        task_id="jira_scrape_users_api",
        python_callable=jira_scrape_users_api,
    )

    upsert_users_into_postgres = SQLExecuteQueryOperator(
        task_id='noop',
        conn_id='test-postgres-instance',
        sql="""
        create table if not exists test(id integer);
        """
    )

    jira_scrape_users_api >> upsert_users_into_postgres

if __name__ == "__main__":
    # Tests
    print("Test")
