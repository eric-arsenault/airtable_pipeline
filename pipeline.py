from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sqlalchemy
from sqlalchemy import create_engine
import pandas as pd 
import requests
import boto3
import json
import ast

def stage():
    offset = ''
    append_string = ''

    while True:
        headers = {'Authorization': 'Bearer YOUR_KEY',}
        params = (('offset', offset),)
        my_request = requests.get('https://api.airtable.com/YOUR/LINK', headers=headers, params=params)
        response = my_request.json()
        records = response['records']
        append_string = append_string + str(records)[1:-1]

        try:
            offset = str(response['offset'])
            append_string = append_string + ", "
        except:
            break 

    session = boto3.Session(
        aws_access_key_id='YOUR_KEY',
        aws_secret_access_key='YOUR_SECRET_KEY'
    )

    s3 = session.resource('s3')

    s3.Bucket('airtablebucket').put_object(Key='my_file.json', Body=append_string)
    
    
def load():
    client = boto3.client('s3',
                        aws_access_key_id='YOUR_KEY',
                        aws_secret_access_key='YOUR_SECRET_KEY'
                        )

    result = client.get_object(Bucket='airtablebucket', Key='my_file.json') 
    text = result["Body"].read().decode()

    df = pd.DataFrame.from_dict(ast.literal_eval(text))
    df = df['fields'].apply(pd.Series)
    
    df['num'] = df['Name'].str.replace("User ","").astype(int)
    df = df.sort_values(by='num')
    df = df.drop(columns='num')
    
    engine = create_engine(
        'snowflake://USER:PASSWORD@ACCOUNT/DB/SCHEMA'.format(
            user='USER',
            password='Password',
            account='ACCOUNT',
            schema='SCHEMA',
            database='DB'
        )
    )

    connection = engine.connect()

    results = df.to_sql(name='airtable_data_2', con=connection, if_exists='replace', index=False)
        
    connection.close()
    engine.dispose()
    
    
default_args = {
    "owner": "airflow-dev",
    "start_date": datetime.today() - timedelta(days=1)
              }
with DAG(
    "airtable_s3_snowflake_DAG",
    default_args=default_args,
    schedule_interval = "0 1 * * *",
    ) as dag:
    stage = PythonOperator(
        task_id="stage",
        python_callable=stage
    ),
    load = PythonOperator(
        task_id="load",
        python_callable=load
    )

stage >> load     
    
    
