from airflow import DAG
from datetime import datetime
import pandas as pd
from airflow.sdk import task
from clickhouse_driver import Client
from prefect import task, flow

@task
def read_csv():
    return pd.read_csv('/home/artyom/PycharmProjects/asobd/test2.csv', header=None)

@task
def transform_data(df):
    df['transformed'] = df[df.columns[0]] + '-transformed'
    df = df.rename(columns={0: "event_type", 1: "session_id"})
    return df


@task
def load_to_clickhouse(df):
    client = Client(host='localhost', port=9000, user='myuser', password='mypassword')
    client.execute('INSERT INTO mart VALUES', df.to_dict('records'))

@flow
def pipeline():
    df = read_csv()
    transformed = transform_data(df)
    print(df.head())
    load_to_clickhouse(transformed)

if __name__ == "__main__":
    pipeline()