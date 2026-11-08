import json
from datetime import datetime, timedelta

from cassandra.query import ConsistencyLevel

BASE_EVENT_TIME = datetime(2026, 1, 1)

EVENT_TYPES = ['login', 'purchase', 'view']


def create_tables(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS user_actions (
            user_id     int,
            event_time  timestamp,
            event_type  text,
            payload     text,
            PRIMARY KEY ((user_id), event_time)
        ) WITH CLUSTERING ORDER BY (event_time DESC) AND compression = {'class': 'LZ4Compressor'}
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS user_actions_none (
            user_id     int,
            event_time  timestamp,
            event_type  text,
            payload     text,
            PRIMARY KEY ((user_id), event_time)
        ) WITH CLUSTERING ORDER BY (event_time DESC) AND compression = {'enabled': 'false'}
    """)
    return ['user_actions', 'user_actions_none']



def insert_events(session, table, user_id, n, consistency):
    query = f'INSERT INTO {table} (user_id, event_time, event_type, payload) VALUES (?, ?, ?, ?)'
    if not query:
        raise NotImplementedError('TODO: insert_events')

    prepared = session.prepare(query)

    for i in range(n):
        event_time = BASE_EVENT_TIME + timedelta(minutes=i)
        event_type = EVENT_TYPES[i % 3]
        payload = json.dumps({'seq': i})
        bound = prepared.bind((user_id, event_time, event_type, payload))
        bound.consistency_level = consistency
        session.execute(bound)

    return n


def read_last(session, table, user_id, limit, consistency):
    query = f'SELECT event_time, event_type, payload FROM {table} WHERE user_id = ? LIMIT ?'
    if not query:
        raise NotImplementedError('TODO: read_last')

    prepared = session.prepare(query)
    bound = prepared.bind((user_id, limit))
    bound.consistency_level = consistency

    return list(session.execute(bound))


def configure_twcs_ttl(session, table):
    session.execute(f"""
        ALTER TABLE
            {table}
        WITH compaction = {{
            'class': 'TimeWindowCompactionStrategy',
            'compaction_window_unit': 'DAYS',
            'compaction_window_size': '1'
        }}
        AND default_time_to_live = 2592000
    """)
