from __future__ import annotations
import csv, hashlib, json, os, random, shlex, shutil, subprocess, sys, time
from pathlib import Path
from .config import connection_env

ROOT=Path(__file__).resolve().parents[1]
SCENARIO=ROOT/'benchmark'/'scenarios'/'09_gen_paths.py'

def read_json(name): return json.loads((ROOT/'experiment'/name).read_text(encoding='utf-8'))
def parse_env(text):
    out={}
    for token in shlex.split(text or ''):
        key,sep,value=token.partition('=')
        if sep: out[key]=value
    return out

def command(cell, no_cgroup=False):
    cmd=[sys.executable,str(SCENARIO),'--path',cell['legacy_cli_path']]
    limit=cell.get('memory_limit')
    if limit and not no_cgroup and sys.platform.startswith('linux'):
        if not shutil.which('systemd-run'):
            raise RuntimeError('systemd-run is required for the declared memory limit; use --no-cgroup only for smoke tests')
        cmd=['systemd-run','--user','--scope','--quiet','-p',f'MemoryMax={limit}',
             '-p',f"MemorySwapMax={cell.get('swap_max',0)}",*cmd]
    return cmd

def append_journal(path, record):
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('a',encoding='utf-8') as f: f.write(json.dumps(record,ensure_ascii=False)+'\n')

def run(cfg, engine, output, *, rounds=(0,1,2,3,4,5), max_rows=0, paths=None,
        seed=20260908, no_cgroup=False, fail_fast=False, dry_run=False):
    output=Path(output); output.mkdir(parents=True,exist_ok=True)
    raw=output/'raw.csv'; journal=output/'journal.jsonl'
    cells=[c for c in read_json('cells.json') if c['engine']==engine]
    if max_rows: cells=[c for c in cells if int(c['rows_expected'])<=max_rows]
    if paths: cells=[c for c in cells if c['path_id'] in paths]
    by_id={c['cell_id']:c for c in cells}
    blocks=[]
    for b in read_json('blocks.json'):
        ids=[x for x in b['cell_ids'] if x in by_id]
        if b['lane']==engine and ids: blocks.append((b['block_id'],[by_id[x] for x in ids]))
    base=os.environ.copy(); base.update(connection_env(cfg,engine)); base['RESULTS_FILE']=str(raw)
    failures=0
    for block_id, members in blocks:
        attempt=f"{block_id}-{int(time.time())}"
        append_journal(journal,{'event':'block_start','block_id':block_id,'attempt_id':attempt,'ts':time.time()})
        for round_no in rounds:
            order=list(members); random.Random(f'{seed}:{block_id}:{round_no}').shuffle(order)
            for cell in order:
                env=base.copy(); env.update(parse_env(cell.get('legacy_environment','')))
                env.update({'MEASURE_LABEL':cell['cell_id'],'BENCH_SCENARIO_ID':'09_gen_paths',
                            'BENCH_ROUND':str(round_no),'BENCH_BLOCK_ATTEMPT':attempt,
                            'BENCH_LANE':engine,'BENCH_CANVAS':'narrow','BENCH_TABLE':cell['table'],
                            'BENCH_ROWS':str(cell['rows_expected']),'BENCH_ENTRY_ID':engine,
                            'BENCH_MEM_LIMIT':str(cell.get('memory_limit','')),
                            'BENCH_SWAP_MAX':str(cell.get('swap_max',''))})
                cmd=command(cell,no_cgroup=no_cgroup)
                record={'event':'cell','cell_id':cell['cell_id'],'block_id':block_id,
                        'attempt_id':attempt,'round':round_no,'command':cmd,'ts':time.time()}
                if dry_run:
                    record['status']='dry_run'; append_journal(journal,record); continue
                proc=subprocess.run(cmd,cwd=ROOT/'benchmark'/'scenarios',env=env,text=True,
                                    stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,
                                    timeout=int(cell.get('timeout_seconds') or 3600))
                record.update({'status':'ok' if proc.returncode==0 else 'error','returncode':proc.returncode,
                               'elapsed_s':time.time()-record['ts'],'stderr_tail':'\n'.join(proc.stderr.splitlines()[-8:])})
                append_journal(journal,record)
                if proc.returncode:
                    failures+=1
                    if fail_fast: return 1
        append_journal(journal,{'event':'block_end','block_id':block_id,'attempt_id':attempt,'ts':time.time()})
    return 1 if failures else 0
