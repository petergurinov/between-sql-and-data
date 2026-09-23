from __future__ import annotations
import argparse, json, sys
from pathlib import Path
from . import __version__
from .config import load
from .runner import read_json, run
from .summarize import summarize

def main(argv=None):
    p=argparse.ArgumentParser(prog='smartdata-benchmark'); p.add_argument('--version',action='version',version=__version__)
    sub=p.add_subparsers(dest='command',required=True)
    q=sub.add_parser('validate-config'); q.add_argument('--config',required=True)
    q=sub.add_parser('list-cells'); q.add_argument('--engine',choices=['pg','ch']); q.add_argument('--max-rows',type=int,default=0)
    q=sub.add_parser('run'); q.add_argument('--config',required=True); q.add_argument('--engine',required=True,choices=['pg','ch']); q.add_argument('--output',required=True)
    q.add_argument('--max-rows',type=int,default=0); q.add_argument('--path',action='append',dest='paths'); q.add_argument('--rounds',default='0,1,2,3,4,5')
    q.add_argument('--seed',type=int,default=20260908); q.add_argument('--no-cgroup',action='store_true'); q.add_argument('--fail-fast',action='store_true'); q.add_argument('--dry-run',action='store_true')
    q=sub.add_parser('summarize'); q.add_argument('--raw',required=True); q.add_argument('--out',required=True)
    a=p.parse_args(argv)
    if a.command=='validate-config': load(a.config); print('configuration is valid'); return 0
    if a.command=='list-cells':
        cells=read_json('cells.json')
        if a.engine: cells=[x for x in cells if x['engine']==a.engine]
        if a.max_rows: cells=[x for x in cells if int(x['rows_expected'])<=a.max_rows]
        for x in cells: print('\t'.join(map(str,[x['cell_id'],x['engine'],x['path_id'],x['table'],x['rows_expected']])))
        return 0
    if a.command=='run':
        rounds=tuple(int(x) for x in a.rounds.split(',') if x.strip())
        return run(load(a.config, require_secrets=not a.dry_run, engines=(a.engine,)),a.engine,a.output,rounds=rounds,max_rows=a.max_rows,paths=a.paths,seed=a.seed,no_cgroup=a.no_cgroup,fail_fast=a.fail_fast,dry_run=a.dry_run)
    summarize(a.raw,a.out); return 0
if __name__=='__main__': raise SystemExit(main())
