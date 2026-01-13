import time
import uuid

import pandas as pd
from clickhouse_driver import Client
from prefect import task, flow
from prefect.cache_policies import NO_CACHE


@task
def read_csv():
    return pd.read_csv('/home/artyom/PycharmProjects/asobd/test.csv')

@task
def transform_data(df):
    df['received_at'] = int(time.time())
    df['event_id'] = df.apply(lambda x: str(uuid.uuid4()), axis=1)
    return df

@task(cache_policy=NO_CACHE)
def insert_events(client, df):
    client.execute('INSERT INTO events (event_id, event_type, created_at, received_at, session_id, url, referrer, event_title, element_id, x, y) VALUES',
                   df[['event_id', 'event_type', 'created_at', 'received_at', 'session_id', 'url', 'referrer', 'event_title', 'element_id', 'x', 'y']].to_dict('records'))

@task(cache_policy=NO_CACHE)
def insert_sessions(client, df):
    client.execute('INSERT INTO sessions (session_id, device_type, user_agent, ip, user_id) VALUES',
                   df[['session_id', 'device_type', 'user_agent', 'ip', 'user_id']].drop_duplicates(subset=['session_id'], keep='first').to_dict('records'))

@task
def load_to_clickhouse(df):
    client = Client(host='localhost', port=9000, user='myuser', password='mypassword')
    insert_events(client, df)
    insert_sessions(client, df)

@flow
def pipeline():
    df = read_csv()
    transformed = transform_data(df)
    load_to_clickhouse(transformed)
    
if __name__ == "__main__":
#    pipeline.serve()
    df = read_csv()
    transformed = transform_data(df)
    load_to_clickhouse(transformed)
