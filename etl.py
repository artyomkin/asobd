import time
import uuid
from asyncio import wait

import pandas
import pandas as pd
from clickhouse_driver import Client
from prefect import task, flow, Flow
from prefect.cache_policies import NO_CACHE


@task
def read_csv():
    return pd.read_csv('/home/artyom/PycharmProjects/asobd/test.csv')

@task
def receive_data(df):
    df['received_at'] = int(time.time())
    df['event_id'] = df.apply(lambda x: str(uuid.uuid4()), axis=1)
    return df

@task(cache_policy=NO_CACHE)
def insert_events(df):
    client = Client(host='localhost', port=9000, user='myuser', password='mypassword')
    client.execute('INSERT INTO events (event_id, event_type, created_at, received_at, session_id, url, referrer, event_title, element_id, x, y) VALUES',
                   df[['event_id', 'event_type', 'created_at', 'received_at', 'session_id', 'url', 'referrer', 'event_title', 'element_id', 'x', 'y']].to_dict('records'))

@task(cache_policy=NO_CACHE)
def insert_sessions(df):
    client = Client(host='localhost', port=9000, user='myuser', password='mypassword')
    client.execute('INSERT INTO sessions (session_id, device_type, user_agent, ip, user_id) VALUES',
                   df[['session_id', 'device_type', 'user_agent', 'ip', 'user_id']].drop_duplicates(subset=['session_id'], keep='first').to_dict('records'))

# insert into user_activity_mart select created_at, user_id from events e join sessions s on e.session_id = s.session_id order by created_at;
@task(cache_policy=NO_CACHE)
def insert_user_activity_mart(df):
    df_sorted = df[['created_at', 'user_id']].sort_values('created_at')
    df_sorted = df_sorted.rename(columns={'created_at': 'time'})
    client = Client(host='localhost', port=9000, user='myuser', password='mypassword')
    client.execute('INSERT INTO user_activity_mart VALUES',df_sorted.to_dict('records'))

@task
def cnt_events(df, event_type):
    df_cnt = df[['created_at', 'element_id']][df['event_type'] == event_type]
    df_cnt['created_at'] //= 3600 * 6
    df_cnt = df_cnt.groupby(by=['created_at', 'element_id']).size().reset_index()
    df_cnt = df_cnt.rename(columns={0: 'cnt'})
    df_cnt['created_at'] *= 3600 * 6
    return df_cnt

@task(cache_policy=NO_CACHE)
def insert_ctr_marts(df):
    cnt_views = cnt_events(df, 'view')
    cnt_clicks = cnt_events(df, 'click')
    insert_ctr_mart(cnt_views, cnt_clicks)
    insert_ctr_element_mart(cnt_views, cnt_clicks)

# insert into ctr_element_mart select vcnt.d as date, vcnt.element_id as element_id, avg(ccnt.click_cnt / vcnt.view_cnt) as ctr from (select created_at div 3600 div 6 * 3600 * 6 as d, element_id, count(*) as view_cnt from events where event_type = 'view' group by created_at div 3600 div 6, element_id) as vcnt join (select created_at div 3600 div 6 * 3600 * 6 as d, element_id, count(*) as click_cnt from events where event_type = 'click' group by created_at div 3600 div 6, element_id) as ccnt on vcnt.d = ccnt.d group by date, element_id order by date, element_id;
@task(cache_policy=NO_CACHE)
def insert_ctr_element_mart(cnt_views, cnt_clicks):
    df_ctr = cnt_views[['created_at', 'element_id']]
    df_ctr['clicks'] = cnt_clicks['cnt'].astype('int')
    df_ctr['views'] = cnt_views['cnt'].astype('int')
    df_ctr = df_ctr.fillna(0)
    df_ctr['clicks'] = df_ctr['clicks'].astype('int')
    df_ctr['views'] = df_ctr['views'].astype('int')
    df_ctr = df_ctr.groupby(by=['created_at', 'element_id'])[['clicks', 'views']].last().reset_index().sort_values(by=['created_at', 'element_id'])
    df_ctr = df_ctr.rename(columns={'created_at': 'time'})
    client = Client(host='localhost', port=9000, user='myuser', password='mypassword')
    client.execute('INSERT INTO ctr_element_mart VALUES', df_ctr.to_dict('records'))

# insert into ctr_mart select vcnt.d as date, ccnt.click_cnt / vcnt.view_cnt as ctr from (select from_unixtime(created_at div 60 div 5 * 60 * 5) as d, count(*) as view_cnt from events where event_type = 'view' group by created_at div 60 div 5) as vcnt join (select from_unixtime(created_at div 60 div 5 * 60 * 5) as d, count(*) as click_cnt from events where event_type = 'click' group by created_at div 60 div 5) as ccnt on vcnt.d = ccnt.d order by date;
@task(cache_policy=NO_CACHE)
def insert_ctr_mart(cnt_views, cnt_clicks):
    df_ctr = cnt_views[['created_at']]
    df_ctr['clicks'] = cnt_clicks['cnt'].astype('int')
    df_ctr['views'] = cnt_views['cnt'].astype('int')
    df_ctr = df_ctr.fillna(0)
    df_ctr['clicks'] = df_ctr['clicks'].astype('int')
    df_ctr['views'] = df_ctr['views'].astype('int')
    df_ctr = df_ctr.groupby(by=['created_at'])[['clicks', 'views']].last().reset_index().sort_values(by=['created_at'])
    df_ctr = df_ctr.rename(columns={'created_at': 'time'})
    client = Client(host='localhost', port=9000, user='myuser', password='mypassword')
    client.execute('INSERT INTO ctr_mart VALUES', df_ctr.to_dict('records'))

# insert into session_duration_mart select time, quantile(0.95)(dur) as duration from (select argMin(created_at, session_id) as time, session_id, (max(created_at) - min(created_at)) / 60 as dur from events e join sessions s on e.session_id = s.session_id group by session_id) group by time;
@task(cache_policy=NO_CACHE)
def insert_session_duration(df):
    df_session_dur_start = df[['created_at', 'session_id']].groupby(by=['session_id']).min('created_at').reset_index()
    df_session_dur_end = df[['created_at', 'session_id']].groupby(by=['session_id']).max('created_at').reset_index()
    df_session_dur_start = df_session_dur_start.rename(columns={'created_at': 'session_start'})
    df_session_dur_end = df_session_dur_end.rename(columns={'created_at': 'session_end'})
    df_session_dur = df_session_dur_start.merge(
        df_session_dur_end,
        on='session_id'
    )
    df_session_dur['duration'] = df_session_dur['session_end'] - df_session_dur['session_start']
    df_session_dur = df_session_dur.drop(columns=['session_end'])
    df_session_dur = df_session_dur.rename(columns={'session_start': 'time'})
    client = Client(host='localhost', port=9000, user='myuser', password='mypassword')
    client.execute('INSERT INTO session_duration_mart VALUES', df_session_dur.to_dict('records'))

@task(cache_policy=NO_CACHE)
def insert_storage(client, df):
    insert_events(client, df)
    insert_sessions(client, df)

@task
def process_duplicates(df):
    return df.drop_duplicates()

@task
def process_required_fields(df):
    df = df.dropna()
    df['created_at'] = df['created_at'].astype('int')
    return df

@task
def parse_events(events: list):
    df = pandas.DataFrame()
    for event in events:
        row_event = {
            "event_type": event["event_type"],
            "session_id": event["session_id"],
            "created_at": event["created_at"],
            "ip": event["ip"],
            "user_id": event["user"]["id"],
            "fio": event["user"]["fio"],
            "country": event["user"]["country"],
            "url": event["url"],
            "referrer": event["referrer"],
            "device_type": event["device_type"],
            "user_agent": event["user_agent"],
            "event_title": event["payload"]["event_title"],
            "element_id": event["payload"]["element_id"],
            "x": event["payload"]["x"],
            "y": event["payload"]["y"]
        }
        df = pd.concat([df, pd.DataFrame([row_event])], ignore_index=True)
        return df
@flow
def pipeline(events=None):
    #df = parse_events(events)
    df = read_csv()
    df = receive_data(df)

    df = process_duplicates(df)
    df = process_required_fields(df)

    insertion_tasks = []
    insertion_tasks.append(insert_events.submit(df))
    insertion_tasks.append(insert_sessions.submit(df))
    insertion_tasks.append(insert_ctr_marts.submit(df))
    insertion_tasks.append(insert_user_activity_mart.submit(df))
    insertion_tasks.append(insert_session_duration.submit(df))

    wait(insertion_tasks)

if __name__ == "__main__":
    pipeline.serve()

