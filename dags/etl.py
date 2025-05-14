from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
#from airflow.utils.dates import days_ago   # removed from airflow
from datetime import datetime, timedelta
from airflow.operators.python import get_current_context

import json


## DEfine the DAG

with DAG(
    dag_id='nasa_apod_postgres_dag',
    start_date= datetime.now() - timedelta(days=1),
    schedule='@daily',
    catchup=False,

) as dag:
    
    ## Step 1 : create the table if it does not exist

    @task
    def create_table():
        pg_hook = PostgresHook(postgres_conn_id='my_postgres_connection') ## Remember to set up the connection in Airflow UI

        # SQL query to create the table if it does not exist (field names and types should match the API response)
        create_table_query = """
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );

        """
        ## Execute the query
        pg_hook.run(create_table_query)
        print("Table created successfully or already exists.")


    ## Step 2 : get the data from the NASA API (APOD) Data  - Astronomy Picture of the Day
    ##https://api.nasa.gov/planetary/apod?api_key=1rYODuKbLcVbAwcc1QbTr1rF4WDk1EQ5e2RsgZgw   link to the API documentation
    extract_apod_data = HttpOperator(
        task_id='extract_apod_data',
        http_conn_id='nasa_api',                      ## Remember connection ID to set up the connection in Airflow UI
        endpoint='planetary/apod',                    ## NASA API endpoint for Astronomy Picture of the Day
        method='GET',
        data = {"api_key": "{{conn.nasa_api.extra_dejson.api_key}}"},  ## Replace with your API key got in gmail _wil be setup in Airflow UI
        response_filter=lambda response: response.json(),                        ## Filter the response to get the JSON data
        log_response=True,
        do_xcom_push=True   # Ensure the response is pushed to XCom for downstream tasks
    )  


    ## Step 3 : Transform the data (Pick the information you need to store in the database)
    @task
    def transform_apod_data(response):
        context = get_current_context()
        ti = context['ti']
        response = ti.xcom_pull(task_ids='extract_apod_data')

        if not isinstance(response, dict):
            raise ValueError("Expected JSON dict from extract_apod_data, got: {}".format(type(response)))
        
       
        # Extract relevant fields from the response
        apod_data = {
            'title': response.get('title', ''),
            'explanation': response.get('explanation', ''),
            'url': response.get('url', ''),
            'date': response.get('date', ''),
            'media_type': response.get('media_type', '')
        }
        return apod_data


    ## Step 4 : Load the data into the Postgres database (insert into the table)
    @task
    def load_data_to_postgres(apod_data):
        #initialize the Postgres hook
        pg_hook = PostgresHook(postgres_conn_id='my_postgres_connection') ## Remember to set up the connection in Airflow UI

        # SQL query to insert data into the table
        insert_query = """
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s)
        """
        # Execute the SQL insert query with the data
        pg_hook.run(insert_query, parameters=(apod_data['title'], 
                                              apod_data['explanation'],
                                              apod_data['url'],
                                              apod_data['date'],
                                              apod_data['media_type'])
                                              )

        print("Data loaded successfully into the database.")

    ## Step 5 : Verify the data DBviewer to check the data in the table




    ## Step 6 : Define the task dependencies
    # Ensure the table is created before extracting data  and before that two connections need to be  created in Airflow UI
   # Extract
    create_table_task = create_table()
    extract_apod_data_task = extract_apod_data  # reference for task
    # Transform
    transform_apod_data_task = transform_apod_data(extract_apod_data_task)
    # Load    
    load_data_to_postgres_task = load_data_to_postgres(transform_apod_data_task)
    ## Set the task dependencies
    create_table_task >> extract_apod_data_task >> transform_apod_data_task >> load_data_to_postgres_task
