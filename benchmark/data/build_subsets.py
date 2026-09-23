#!/usr/bin/env python3
"""Эталонные подмножества ClickBench для G3 (решение Петра 2026-09-12).

Одни и те же байты грузятся во все четыре базы, поэтому подмножества
собираются ОДИН раз из публичной раздачи и кладутся в Object Storage:

  строки: 10M = десять файлов раздачи hits_0..9 в их порядке (десять частей
          cb_<w>_10m_part<i>.parquet), 1M = первый файл целиком, 100k / 10k / 1 -
          первые строки первого файла (порядок строк parquet детерминирован);
  ширины: w10 = та же десятка, что у hits_w10 серий F/G/G2 (bench/29-narrow-
          tables.sh COLS10: WatchID, JavaEnable, Title, EventTime, CounterID,
          ClientIP, RegionID, UserID, URL, Referer - только bigint, smallint,
          int, text, timestamp; эти типы держат все протоколы G3, а цифры
          сопоставимы с прежними сериями), w50 = первые 50 колонок DDL,
          w105 = все 105.

Типы в файлах остаются такими, как в раздаче (EventTime - unix-секунды int64,
EventDate - дни uint16): загрузчики приводят их к честным типам сами (PG -
honest_times в 10-load-hits-pg.py, CH - типы DDL при INSERT SELECT).

Запуск (локально, кеш раздачи - каталог с hits_0..9.parquet):
    python3 bench/29-g3-build-subsets.py --cache ~/g3-data-cache --out ~/g3-data-out
Манифест с SHA256 и происхождением - bench/data/g3-canonical-manifest.json
(пересобирается при каждом запуске; отсутствующие файлы кеша пропускаются
с пометкой, готовые объекты не переписываются, если sha256 совпал).
"""
import argparse
import hashlib
import json
import os
import re
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

WIDTHS = {"w10": 10, "w50": 50, "w105": 105}
# w10 - не первые десять колонок DDL, а десятка hits_w10 прежних серий
COLS10 = ["WatchID", "JavaEnable", "Title", "EventTime", "CounterID", "ClientIP",
          "RegionID", "UserID", "URL", "Referer"]
ROWS = [(1, "1"), (10_000, "10k"), (100_000, "100k"), (1_000_000, "1m")]
SRC_TPL = "https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_{i}.parquet"


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def ddl_columns(ddl_path: Path) -> list:
    """Имена колонок из ch_create.sql в порядке объявления."""
    cols = []
    for line in ddl_path.read_text().splitlines():
        # список колонок кончается на PRIMARY KEY / закрывающей скобке; строки
        # SETTINGS ниже похожи на колонки по форме и в список идти не должны
        if re.match(r"^\s*(PRIMARY|\)|ENGINE|SETTINGS)", line):
            break
        m = re.match(r"^\s{4}([A-Za-z][A-Za-z0-9_]*)\s+\S", line)
        if m:
            cols.append(m.group(1))
    return cols


def write_subset(table: pa.Table, cols: list, n: int, out: Path) -> dict:
    sub = table.select(cols).slice(0, n)
    if sub.num_rows != n:
        raise SystemExit(f"{out.name}: ожидалось {n} строк, в источнике {sub.num_rows}")
    tmp = out.with_suffix(".parquet.tmp")
    pq.write_table(sub, tmp, compression="zstd", row_group_size=200_000)
    os.replace(tmp, out)
    md = pq.read_metadata(out)
    if md.num_rows != n:
        raise SystemExit(f"{out.name}: записано {md.num_rows} строк вместо {n}")
    return {"name": out.name, "rows": n, "columns": len(cols), "bytes": out.stat().st_size,
            "sha256": sha256(out)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache", required=True, help="каталог с hits_0..9.parquet")
    ap.add_argument("--out", required=True, help="куда писать подмножества")
    ap.add_argument("--manifest", default=str(Path(__file__).parent / "data" / "g3-canonical-manifest.json"))
    ap.add_argument("--widths", default="w10,w50,w105")
    args = ap.parse_args()
    cache, out = Path(args.cache).expanduser(), Path(args.out).expanduser()
    out.mkdir(parents=True, exist_ok=True)
    manifest_path = Path(args.manifest)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    old = {}
    if manifest_path.exists():
        old = {o["name"]: o for o in json.loads(manifest_path.read_text()).get("objects", [])}

    ddl_cols = ddl_columns(Path(__file__).parent / "ch_create.sql")
    if len(ddl_cols) != 105:
        raise SystemExit(f"ch_create.sql: разобрано {len(ddl_cols)} колонок, ожидалось 105")
    widths = {w: (COLS10 if w == "w10" else ddl_cols[:WIDTHS[w]]) for w in args.widths.split(",")}
    for w, cols in widths.items():
        missing = [c for c in cols if c not in ddl_cols]
        if missing:
            raise SystemExit(f"{w}: колонок нет в DDL: {missing}")

    objects, sources, skipped = [], [], []
    t0 = time.time()
    for i in range(10):
        src = cache / f"hits_{i}.parquet"
        if not src.exists() or src.with_suffix(".parquet.part").exists():
            skipped.append(src.name)
            continue
        table = pq.read_table(src)
        sch_names = [f.name for f in table.schema]
        if sch_names != ddl_cols:
            raise SystemExit(f"{src.name}: порядок колонок parquet не совпадает с DDL")
        if table.num_rows != 1_000_000:
            raise SystemExit(f"{src.name}: {table.num_rows} строк, ожидался ровно 1M")
        src_sha = sha256(src)
        sources.append({"file": src.name, "url": SRC_TPL.format(i=i), "bytes": src.stat().st_size,
                        "sha256": src_sha, "rows": table.num_rows})
        for w, cols in widths.items():
            # десять частей 10M: часть i = проекция файла i
            objects.append({**write_subset(table, cols, 1_000_000, out / f"cb_{w}_10m_part{i}.parquet"),
                            "table": f"cb_{w}_10m", "part": i, "source": src.name, "width": w})
            if i == 0:
                for n, suffix in ROWS:
                    objects.append({**write_subset(table, cols, n, out / f"cb_{w}_{suffix}.parquet"),
                                    "table": f"cb_{w}_{suffix}", "part": None, "source": src.name, "width": w})
        print(f"{src.name}: готово, {time.time() - t0:.0f} c", flush=True)
        del table

    manifest = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "tool": {"pyarrow": pa.__version__, "python": sys.version.split()[0], "compression": "zstd"},
        "rule": ("10M = hits_0..9 раздачи в их порядке (части part0..9); 1M = hits_0 целиком; "
                 "100k/10k/1 = первые строки hits_0; w10 = десятка hits_w10 серий F/G/G2, "
                 "w50 = первые 50 колонок DDL ClickBench, w105 = все 105"),
        "widths": {w: cols for w, cols in widths.items()},
        "sources": sources,
        "objects": sorted(objects, key=lambda o: o["name"]),
        "skipped_sources": skipped,
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=1) + "\n")
    total = sum(o["bytes"] for o in objects)
    print(f"объектов {len(objects)}, {total / 2**20:.0f} МиБ; пропущено источников: {skipped or 'нет'}; "
          f"манифест {manifest_path}")


if __name__ == "__main__":
    main()
