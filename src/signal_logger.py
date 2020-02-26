import sqlite3
from argparse import ArgumentParser
from unms_api_client import UnmsApi
from bisect import bisect_right

def parse_args():
    ap = ArgumentParser()
    ap.add_argument('-k', '--key', dest='api_key', required=True)
    ap.add_argument('-e', '--endpoint', dest='endpoint', required=True)
    ap.add_argument('-d', '--devices', dest='devices', nargs='+')
    ap.add_argument('-s', '--stats', dest='stat_names', nargs='+', default=['signal'])
    ap.add_argument('-db', '--database', dest='db', default='data.db')
    return vars(ap.parse_args())

def create_or_update_schema(cursor, stats):
    cursor.execute('''CREATE TABLE IF NOT EXISTS devices
        (device_id text primary key)''')

    for s in stats:
        cursor.execute(f'''CREATE TABLE IF NOT EXISTS {s}
            (device_mapped_id INTEGER, x integer, y integer,
             FOREIGN KEY(device_mapped_id) REFERENCES devices(rowid))''')
        cursor.execute(f'''CREATE INDEX IF NOT EXISTS {s}_DeviceIndex
            ON {s} (device_mapped_id)''')
    return

def get_device_mappings(cursor, devices):
    for d in devices:
        cursor.execute('SELECT rowid FROM devices WHERE device_id=?', (d,))
        rowid = cursor.fetchone()
        if rowid is None:
            cursor.execute('INSERT INTO devices VALUES (?)', (d,))
    
    cursor.execute('SELECT device_id, rowid FROM devices')
    rowids = cursor.fetchall()
    dev_map = {k:v for (k,v) in rowids if k in devices}
    return dev_map

def record_new_stats(cursor, did, stat, stat_name):
    c = cursor
    stat = [(did, s['x'], s['y']) for s in stat]
    stat.sort(key=lambda x:x[1])
    x = [t[1] for t in stat]
    c.execute(f'''SELECT x FROM {stat_name} WHERE device_mapped_id=?
        ORDER BY rowid DESC LIMIT 1''', (did,))
    last_recorded = c.fetchone()
    last_recorded = last_recorded[0] if last_recorded is not None else -1
    first_unrecorded = bisect_right(x, last_recorded)
    c.executemany(f'''INSERT INTO {stat_name} VALUES (?,?,?)''', stat[first_unrecorded:])
    return len(stat)-first_unrecorded

def main(endpoint, api_key, devices, stat_names, db):
    api = UnmsApi(endpoint, api_key)
    conn = sqlite3.connect(db)
    c = conn.cursor()

    create_or_update_schema(c, stat_names)
    dev_map = get_device_mappings(c, devices)

    for did in devices:
        try:
            stats = api.device_stats(did, interval='hour')
            for stat_name in stat_names:
                try:
                    new_entries = record_new_stats(c, dev_map[did], stats[stat_name], stat_name)
                    conn.commit()
                    print(f'Logged {new_entries} new "{stat_name}" entries for {did}')
                except Exception as e:
                    print(f'Failed to record "{stat_name}" for {did}')
                    print(f'Exception thrown: {e}')
        except Exception as e:
            print(f'Failed to fetch stats for {did}')
            print(f'Exception thrown: {e}')
    return

if __name__ == '__main__':
    args = parse_args()
    main(**args)