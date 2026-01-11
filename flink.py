import time
import uuid

import pandas as pd
from clickhouse_driver import Client
from prefect import task, flow
import asyncio

@task
def read_csv():
    return pd.read_csv('/home/artyom/PycharmProjects/asobd/test.csv')

@task
def transform_data(df):
    df['received_at'] = time.time()
    df['event_id'] = df.apply(lambda x: str(uuid.uuid4()), axis=1)
    return df

def insert_events(client, df):
    client.execute('INSERT INTO events (event_id, event_type, created_at, received_at, session_id, url, referrer) VALUES',
                   df[['event_id', 'type', 'created_at', 'received_at', 'session_id', 'url', 'referrer']].to_dict('records'))
    client.execute('INSERT INTO clicks (event_id, event_title, element_id, x, y) VALUES',
                   df[df['type' == 'click']][['event_id', 'event_title', 'element_id', 'x', 'y']].to_dict('records'))

#TODO: insert sessions

@task
def load_to_clickhouse(df):
    client = Client(host='localhost', port=9000, user='myuser', password='mypassword')
    insert_events(client, df)

@flow
def pipeline():
    df = read_csv()
    transformed = transform_data(df)
    print(df.head())
    load_to_clickhouse(transformed)
    
if __name__ == "__main__":
    pipeline.serve()