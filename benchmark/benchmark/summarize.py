from __future__ import annotations
import csv, statistics
from collections import defaultdict
from pathlib import Path

def number(value):
    try: return float(value)
    except (TypeError,ValueError): return None

def summarize(raw, out):
    with Path(raw).open(encoding='utf-8',newline='') as f: rows=list(csv.DictReader(f))
    groups=defaultdict(list)
    for r in rows:
        try: round_no=int(r.get('round') or -1)
        except ValueError: continue
        if r.get('status')=='ok' and round_no in (1,2,3,4,5): groups[r['path']].append(r)
    fields=['cell_id','engine','rounds','wall_median_s','wall_min_s','wall_max_s','wall_spread_ratio',
            'peak_rss_median_mib','cpu_total_median_s','interface_rx_median_mib','rows_median','digest_count']
    with Path(out).open('w',encoding='utf-8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields); w.writeheader()
        for cell,items in sorted(groups.items()):
            vals=lambda key:[v for r in items if (v:=number(r.get(key))) is not None]
            wall=vals('wall_s'); peak=vals('peak_rss_mb'); user=vals('cpu_user_s'); system=vals('cpu_sys_s')
            rx=vals('bytes_rx_mb'); count=vals('rows')
            cpu=[a+b for a,b in zip(user,system)] if len(user)==len(system) else []
            w.writerow({'cell_id':cell,'engine':items[0].get('lane',''),'rounds':len(items),
                'wall_median_s':statistics.median(wall) if wall else '', 'wall_min_s':min(wall) if wall else '',
                'wall_max_s':max(wall) if wall else '', 'wall_spread_ratio':max(wall)/min(wall) if wall and min(wall)>0 else '',
                'peak_rss_median_mib':statistics.median(peak) if peak else '',
                'cpu_total_median_s':statistics.median(cpu) if cpu else '',
                'interface_rx_median_mib':statistics.median(rx) if rx else '',
                'rows_median':statistics.median(count) if count else '',
                'digest_count':len({r.get('digest') for r in items if r.get('digest')})})
