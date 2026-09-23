#!/usr/bin/env python3
from __future__ import annotations
import argparse, importlib.util, itertools, os, re, sys, time, tomllib
from pathlib import Path
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

HERE=Path(__file__).resolve().parent
TIME_COLS={'eventtime':'seconds','clienteventtime':'seconds','localeventtime':'seconds','eventdate':'days'}

def batches(files):
    for name in files:
        for batch in pq.ParquetFile(name).iter_batches(batch_size=200_000): yield honest(batch)

def honest(batch):
    arrays=[]; fields=[]
    for col,field in zip(batch.columns,batch.schema):
        kind=TIME_COLS.get(field.name.lower())
        if kind=='seconds' and pa.types.is_integer(field.type):
            col=pc.multiply(col.cast(pa.int64()),pa.scalar(1_000_000,pa.int64())).cast(pa.timestamp('us')); field=field.with_type(pa.timestamp('us'))
        elif kind=='days' and pa.types.is_integer(field.type): col=col.cast(pa.int32()).cast(pa.date32()); field=field.with_type(pa.date32())
        arrays.append(col); fields.append(field)
    return pa.RecordBatch.from_arrays(arrays,schema=pa.schema(fields))

def ddl_columns(path):
    out={}
    for line in path.read_text().splitlines():
        m=re.match(r'^\s{4}([A-Za-z][A-Za-z0-9_]*)\s+(.*?),?\s*$',line)
        if m: out[m.group(1)]=m.group(2).rstrip(',')
    return out

def pg(cfg,table,files,force):
    import adbc_driver_postgresql.dbapi as dbapi
    import psycopg
    c=cfg['pg']; pw=os.environ[c['password_env']]
    uri=f"postgresql://{c['user']}:{pw}@{c['host']}:{c['port']}/{c['database']}?sslmode={c.get('sslmode','disable')}"
    iterator=iter(batches(files)); first=next(iterator); columns=[f.name for f in first.schema]; types=ddl_columns(HERE/'pg_create.sql')
    body=',\n'.join(f'    {n} {types[n]}' for n in columns); ddl=f'CREATE TABLE {table} (\n{body}\n)'
    con=dbapi.connect(uri); cur=con.cursor(); cur.execute(f"SELECT count(*) FROM pg_tables WHERE tablename='{table}'")
    exists=cur.fetchone()[0]
    if exists and force: cur.execute(f'DROP TABLE {table}'); con.commit(); exists=0
    if not exists: cur.execute(ddl); con.commit()
    total=0
    for batch in itertools.chain([first],iterator):
        schema=pa.schema([f.with_name(f.name.lower()) for f in batch.schema]); batch=pa.RecordBatch.from_arrays(batch.columns,schema=schema)
        total+=cur.adbc_ingest(table,batch,mode='append'); con.commit()
    cur.close(); con.close()
    with psycopg.connect(uri.replace('?sslmode=disable',''),autocommit=True) as con2: con2.execute(f'VACUUM (FREEZE, ANALYZE) {table}')
    print(f'pg {table}: {total} rows')

def ch(cfg,table,files,force):
    import clickhouse_connect
    c=cfg['ch']; client=clickhouse_connect.get_client(host=c['host'],port=c['http_port'],username=c['user'],password=os.environ[c['password_env']],database=c['database'],secure=c.get('secure',False))
    if force: client.command(f'DROP TABLE IF EXISTS {table}')
    iterator=iter(batches(files)); first=next(iterator); types=ddl_columns(HERE/'ch_create.sql'); body=',\n'.join(f'    {n} {types[n]}' for n in first.schema.names)
    client.command(f'CREATE TABLE IF NOT EXISTS {table} (\n{body}\n) ENGINE=MergeTree ORDER BY tuple()')
    total=0
    for batch in itertools.chain([first],iterator): client.insert_arrow(table,pa.Table.from_batches([batch])); total+=batch.num_rows
    client.command(f'OPTIMIZE TABLE {table} FINAL'); print(f'ch {table}: {total} rows')

def main():
    p=argparse.ArgumentParser(); p.add_argument('--config',required=True); p.add_argument('--engine',required=True,choices=['pg','ch']); p.add_argument('--table',required=True); p.add_argument('--force',action='store_true'); p.add_argument('files',nargs='+'); a=p.parse_args()
    with open(a.config,'rb') as f: cfg=tomllib.load(f)
    (pg if a.engine=='pg' else ch)(cfg,a.table,[Path(x) for x in a.files],a.force)
if __name__=='__main__': main()
