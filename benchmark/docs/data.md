# Подготовка данных

Для эксперимента используется публичный датасет ClickBench. Первые десять файлов `hits_0.parquet` … `hits_9.parquet` содержат по одному миллиону строк. Из них строятся таблицы со следующими размерами:

- число строк: 1, 10 000, 100 000, 1 000 000 и 10 000 000;
- число столбцов: `w10`, `w50` и `w105`;
- `w10`: WatchID, JavaEnable, Title, EventTime, CounterID, ClientIP, RegionID, UserID, URL, Referer;
- `w50`: первые 50 столбцов канонического DDL;
- `w105`: все 105 столбцов.

`data/build_subsets.py` строит Parquet-файлы и записывает их SHA-256. `data/source-manifest.json` фиксирует URL, порядок частей, размеры и хеши исходных файлов. `data/load.py` загружает одни и те же Parquet-файлы в `pg` и `ch`. После загрузки PostgreSQL выполняет `VACUUM (FREEZE, ANALYZE)`, а ClickHouse — `OPTIMIZE TABLE ... FINAL`.

```bash
python data/build_subsets.py --cache data/cache --out data/generated
python data/load.py --config config/benchmark.toml --engine pg --table cb_w10_1m data/generated/cb_w10_1m.parquet
python data/load.py --config config/benchmark.toml --engine ch --table cb_w10_1m data/generated/cb_w10_1m.parquet
```

Перед измерением сверьте число строк, порядок столбцов, типы и диапазон дат. Затем сравните контрольную сумму результата (`digest`) на обеих СУБД.
