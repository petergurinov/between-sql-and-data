# Запросы и способы чтения

Исполняемый SQL для каждой комбинации способа чтения и таблицы находится в `experiment/queries.csv`; рядом записан SHA-256 текста запроса. Базовая форма — `SELECT * FROM <table>`. Конкретная реализация может обернуть запрос в команду протокола: PostgreSQL COPY binary/text/csv или ClickHouse `FORMAT Native`, ArrowStream, RowBinary, CSV, JSON и Parquet.

SQL строится в `benchmark/scenarios/bench_sql.py`. Реализации способов чтения находятся в `paths_pg.py`, `paths_ch.py` и `paths_analyst.py`. `09_gen_paths.py` выводит список способов и фактический SQL:

```bash
python benchmark/scenarios/09_gen_paths.py --list-paths
python benchmark/scenarios/09_gen_paths.py --print-sql --path pg_psycopg
python benchmark/scenarios/09_gen_paths.py --print-sql --path ch_http
```

В опубликованный набор входят psycopg, PostgreSQL COPY, PostgreSQL ADBC, ConnectorX, ClickHouse Native TCP, HTTP-форматы, ADBC по HTTP, эмуляция протокола и чтение файла через DuckDB. Код также поддерживает Flight SQL, но измерения Flight SQL не входят в основной набор из 912 вариантов.
