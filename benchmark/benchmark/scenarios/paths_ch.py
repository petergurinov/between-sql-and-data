"""Пути ClickHouse: родной Native TCP, HTTP с осью FORMAT, обе эмуляции
(pg-wire :9005, mysql-wire :9004) и Flight SQL :9090.

Путь ch_pg_emu переиспользует psycopg-код из paths_pg (однонаправленный
импорт): клиент тот же до строки, меняется только то, кто на том конце.
"""

import base64
import csv
import io
import json
import os
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq

try:
    # опции драйвера Flight: тот же импорт, что у paths_pg. Пакета может не
    # быть на машине без клеток Flight - тогда путь ch_flightsql и так не
    # снимается, а импорт не должен ронять весь модуль путей ClickHouse
    from adbc_driver_flightsql import DatabaseOptions as FlightOptions
except Exception:                                              # noqa: BLE001
    FlightOptions = None

try:  # pymysql нужен одному пути; его отсутствие не должно валить --list-paths
    import pymysql
except ImportError:
    pymysql = None

try:  # mysqlclient (import MySQLdb) - второй клиент того же пути, тоже мягко
    import MySQLdb
    import MySQLdb.cursors
except ImportError:
    MySQLdb = None

try:  # Flight SQL - опциональный пакет
    import adbc_driver_flightsql.dbapi as flightsql
except ImportError:
    flightsql = None

try:  # ADBC ClickHouse (protocol=http): общий manager для драйверов
    from adbc_driver_manager import dbapi as adbc_manager
except ImportError:
    adbc_manager = None

from bench_axes import (ARROW_FMTS, BATCH, BATCH_SET, CH, CH_FMTS,
                        CH_NO_PARSER, CODEC, CODEC_LEVEL, EXECS, FMT, MODE,
                        QUERY_FORM, RESTORE_TYPES, TARGET, VARIANT)
from bench_sql import (SQL_CH, SQL_CH_EMU, _expected_rows, _flight_sql,
                       point_keys_sql, point_sql)
from bench_runtime import (_arrow_from_ndjson, _declared_names,  # noqa: I001
                           _df_from_csv, _df_from_ndjson, _finish_polars,
                           _polars_from_columns, _polars_from_csv,
                           _polars_from_ndjson,
                           HTTP_BLOCK_BYTES, _arrow_result, _arrow_to_tuples, _raw_result, _finish_df,
                           _byte_blocks, _ch_connect_http, _ch_connect_native,
                           _ch_raw_query, _ch_raw_stream, _ch_settings, _close,
                           _dfgate, _drain, _execs, _extras, _maybe_retained,
                           _pg_emu_connect, _polars_read_database,
                           _rows_result, _srvcost, _wire_codec, cell_query_id,
                           ch_qid_settings, register_post_check, shuffle_keys)
# маркер служебных запросов клетки (F15): выборка ключей и чтение
# system.settings идут по тому же соединению, что и зачетный запрос
from _srvcontrol import setup_sql
from paths_pg import _pg_consume
# счетчик интерфейса берем у обвязки - той же функцией, что пишет колонку
# bytes_rx: вторая реализация счета байтов разъехалась бы с корпусом
from _measure import _net_counters


# =========================================================================
# Нативное сжатие: кодек ответа выбирает СЕРВЕР
# =========================================================================
# В нативном протоколе ClickHouse compression= у clickhouse-driver означает
# только "сжимать разрешено": МЕТОД, которым сервер жмет ответ клиенту,
# задается серверной настройкой network_compression_method (дефолт LZ4), а
# клиент разбирает кадр по байту метода в его заголовке - то есть съедает
# любой кодек молча. Поэтому прогон 2026-09-02 снял клетку
# a6-tcp-zstd-drain-g10-1000k на lz4: байты совпали с lz4-клеткой до сотых
# процента, и ни один ассерт этого не увидел.
# Лечение из двух половин, как у транспортного сжатия HTTP: метод уходит в
# настройки ЗАПРОСА явно и тут же перечитывается С СЕРВЕРА (факт вместо
# намерения драйвера).
_CH_NET_METHOD = {"lz4": "LZ4", "zstd": "ZSTD"}

# Уровень сетевого zstd прибивается так же явно, как уровень кодека на HTTP
# (http_zlib_compression_level): без него цифру "сжатие экономит столько-то"
# задает дефолт сборки сервера. Единица - обычный zstd, не режим высокого
# сжатия; у сетевого LZ4 ручки уровня нет вовсе (он всегда обычный LZ4, а не
# LZ4HC), поэтому пара кодеков остается сравнимой.
_CH_NET_ZSTD_LEVEL = CODEC_LEVEL  # тот же уровень, что у HTTP-половины оси:
# иначе TCP-zstd и HTTP-zstd в корпусе стоят рядом как сравнимые, а на деле
# различаются уровнем, и разница читается как свойство протокола

# Настройки, отсутствие которых на сервере - остановка клетки, а не пометка:
# без метода кодек снова выбрал бы сервер.
_CH_NET_STRICT = ("network_compression_method",)

# Порог контрольной клетки: относительная разница байтов lz4 и zstd. Ожидаемая
# разница на профиле g10 - десятки процентов, порог берем заведомо ниже, он
# ловит совпадение "до сотых процента", а не разницу кодеков.
_CH_NET_FACT_MIN_DIFF = 0.05


def _ch_block_frame() -> str:
    """Колонка frame: кадр чтения клетки у путей ClickHouse.

    Кадрирует поток серверный блок (max_block_size) - та же ручка, что
    itersize у psycopg и fetchSize у pgJDBC. Пусто, когда ось размера порции
    не задана: тогда кадр определяет дефолт сборки сервера, и число в колонке
    обещало бы корпусу точку оси, которой в клетке нет.
    """
    return f"max_block_size={BATCH}" if BATCH_SET else ""


def _close_native(client) -> None:
    """Закрыть нативного клиента: у clickhouse-driver метод зовется disconnect."""
    for name in ("disconnect", "close"):
        closer = getattr(client, name, None)
        if closer is None:
            continue
        try:
            closer()
        except Exception:  # соединение могло умереть само - это не сбой пробы
            pass
        break


def _ch_native_codec_settings(codec: str = None) -> dict:
    """Настройки ЗАПРОСА, задающие кодек ответа нативного протокола.

    Пустой словарь у клеток без транспортного сжатия: none-пара и клетки
    моста обязаны уйти на сервер ровно теми же настройками, что в прошлых
    сериях, - иначе побайтовая сверка моста перестала бы сходиться.
    """
    codec = _wire_codec() if codec is None else codec
    if not codec:
        return {}
    method = _CH_NET_METHOD.get(codec)
    if method is None:
        raise SystemExit(
            f"BENCH_CODEC={codec!r} на нативном пути: кодек ответа по TCP "
            f"задается методами {tuple(_CH_NET_METHOD)}")
    settings = {"network_compression_method": method}
    if method == "ZSTD":
        settings["network_zstd_compression_level"] = _CH_NET_ZSTD_LEVEL
    return settings


def _ch_native_assert_codec(client, settings: dict) -> None:
    """Перечитать настройки кодека С СЕРВЕРА и сверить с тем, что просили.

    clickhouse-driver шлет настройки неважными (settings_is_important=False):
    незнакомое имя сервер молча игнорирует, а ответ едет на его дефолте - ровно
    та тишина, из-за которой клетка zstd оказалась клеткой lz4. Настройки
    запроса видны в system.settings ИЗ САМОГО запроса, поэтому проба идет с тем
    же словарем, с которым потом поедут данные.

    Цена пробы - один короткий обмен, и только у клеток со сжатием: клетки
    none и мост на сервер за подтверждением не ходят вовсе. Начиная со среза v1.5
    проба идет ПОСЛЕ замера и на отдельном соединении (F3): внутри окна
    коннекта она добавляла бы сжатым клеткам лишний обмен, и connect_s
    перестал бы сравниваться между точками оси сжатия одного пути (в серии F
    none давал 0.000, а lz4 и zstd - 0.009-0.010, и это была цена пробы).
    """
    if not settings:
        return
    names = ", ".join(f"'{name}'" for name in settings)
    rows = client.execute(
        setup_sql("SELECT name, value FROM system.settings "
                  f"WHERE name IN ({names})"),
        settings=settings)
    got = {str(name): str(value) for name, value in rows}
    for name, want in settings.items():
        value = got.get(name)
        if value is None:
            if name in _CH_NET_STRICT:
                raise SystemExit(
                    f"сервер не знает настройки {name} - кодек ответа выбирал "
                    "бы он сам, клетка нативного сжатия недействительна")
            print(f"# ch_native: сервер не знает настройки {name} - остается "
                  "дефолт сборки", file=sys.stderr)
            continue
        if value.upper() != str(want).upper():
            raise SystemExit(
                f"сервер не принял {name}={want}: system.settings отдает "
                f"{value!r} - ответ поехал бы чужим кодеком под нашей меткой")
    print("# ch_native: кодек ответа подтвержден сервером - "
          + ", ".join(f"{k}={v}" for k, v in sorted(settings.items())),
          file=sys.stderr)


def _ch_native_codec_fact():
    """Контрольная клетка: байты lz4 и zstd обязаны разъехаться.

    Прием тот же, что у гейта кодеков сводки (сжатая клетка легче своей
    none-пары), только пара здесь lz4 против zstd и снимается она внутри
    одного процесса - до ночи, а не после нее. Байты берутся тем же счетчиком
    интерфейса, что пишет колонку bytes_rx: рассказу драйвера верить нельзя,
    именно он и был зеленым на недействительной клетке.

    Соединение одно на обе половины: метод задается настройкой ЗАПРОСА, и
    смена метода между запросами - это ровно то, что мы проверяем.
    """
    if MODE != "drain":
        raise SystemExit(
            f"BENCH_MODE={MODE} на контрольной клетке кодеков ch_native: "
            "факт снимается по байтам доставки - клетка только drain")
    if not _wire_codec():
        raise SystemExit(
            "BENCH_CODEC=none на контрольной клетке кодеков ch_native: "
            "без сжатия сравнивать нечего")
    if _net_counters() is None:
        raise SystemExit(
            "счетчиков интерфейса нет (см. BENCH_RX_IFACE) - факт по байтам "
            "снять нечем, контрольная клетка недействительна")

    client = _ch_connect_native()
    seen = {}
    rec = {}
    for codec in ("lz4", "zstd"):
        settings = _ch_native_codec_settings(codec)
        _ch_native_assert_codec(client, settings)
        rx0 = _net_counters()["rx_bytes"]
        t0 = time.perf_counter()
        rec = _drain(client.execute_iter(SQL_CH, settings=settings), t0,
                     rows_of=lambda row: 1, klass="rows", path="ch_native")
        seen[codec] = (_net_counters()["rx_bytes"] - rx0) / 2**20

    note = ";".join(f"{c}={v:.2f}MB" for c, v in seen.items())
    lo, hi = min(seen.values()), max(seen.values())
    if hi <= 0 or (hi - lo) / hi < _CH_NET_FACT_MIN_DIFF:
        raise SystemExit(
            f"байты нативного сжатия совпали ({note}) - кодек ответа выбрал "
            "сервер, а не настройка запроса: ось нативного сжатия "
            "недействительна")
    print(f"# ch_native codecfact: {note}", file=sys.stderr)
    return (rec["rows"], rec.get("ttfb_s"), None,
            {"client_dtype": "codecfact:" + note})


def path_ch_native():
    """Родной бинарный протокол по TCP - точка отсчета для эмуляций.
    FORMAT здесь не выбирается: провод всегда Native."""
    if MODE == "srvcost":
        return _srvcost("ch")
    if os.environ.get("BENCH_CH_TCP_CODEC_FACT") == "1":
        return _ch_native_codec_fact()
    if FMT not in ("", "Native"):
        raise SystemExit(
            f"BENCH_FMT={FMT!r}: у родного TCP-протокола формат один - Native")
    # кодек ответа задает сервер - см. блок выше; на клетках без сжатия
    # словарь пуст и запрос уходит ровно тем же, что в прошлых сериях
    codec_settings = _ch_native_codec_settings()

    def connect():
        # F3: проба кодека тут больше НЕ зовется. С честным рукопожатием
        # (force_connect в _ch_connect_native) она добавляла бы клеткам со
        # сжатием лишний обмен внутрь connect_s, и точки оси сжатия
        # перестали бы быть сравнимыми между собой (в серии F none давал
        # 0.000, а lz4/zstd - 0.009-0.010, и это была цена пробы, а не
        # кодека). Проба идет ПОСЛЕ замера, отдельным соединением.
        if TARGET == "np" and MODE == "materialize":
            # numpy-результат clickhouse-driver включается в КОНСТРУКТОРЕ
            # клиента (см. _ch_connect_native): settings запроса его не дают
            return _ch_connect_native(use_numpy=True)
        return _ch_connect_native()

    def consume(client, t0):
        if MODE == "drain":
            # execute_iter - настоящий поток: блоки разбираются по мере
            # прихода и тут же отпускаются
            return _drain(client.execute_iter(SQL_CH,
                                              settings=codec_settings or None,
                                              query_id=cell_query_id()),
                          t0, rows_of=lambda row: 1, klass="rows",
                          path="ch_native",
                          # C1: единица нативного потока - СТРОКА, поэтому
                          # усыпление медленного клиента идет раз в блок
                          # (BENCH_BATCH - та же ручка, что max_block_size),
                          # иначе задержка умножалась бы на миллион
                          delay_every=max(BATCH, 1))
        if TARGET in ("columnar", "np") and MODE == "materialize":
            # тот же провод до байта, другая целевая структура - ось
            # целевой структуры
            extra = dict(codec_settings)
            if TARGET == "np":
                # с use_numpy в конструкторе (connect выше) ключ в settings
                # запроса избыточен, но безвреден: намерение клетки остается
                # рядом с запросом, а чтение колонок блока смотрит именно сюда
                extra["use_numpy"] = True
            cols = client.execute(SQL_CH, columnar=True,
                                  settings=extra or None,
                                  query_id=cell_query_id())
            rows = len(cols[0]) if cols else 0
            if TARGET == "np":
                # метка np обязана соответствовать структуре: кортежи под
                # меткой numpy (регресс серии G) хуже отсутствующей клетки.
                # Колонки типов без numpy-поддержки clickhouse-driver отдает
                # кортежем - это законно и видно в client_dtype
                if cols and not any(hasattr(c, "dtype") for c in cols):
                    raise SystemExit(
                        "BENCH_TARGET_STRUCT=np у ch_native: clickhouse-driver "
                        "вернул кортежи, а не numpy - use_numpy должен стоять "
                        "в конструкторе клиента (см. _ch_connect_native)")
                # у np в client_dtype идет и dtype массива: "ndarray;ndarray"
                # не отличило бы честные типы от object-массивов
                dtypes = ";".join(
                    f"{type(c).__name__}[{c.dtype}]" if hasattr(c, "dtype")
                    else type(c).__name__ for c in cols)
            else:
                dtypes = ";".join(type(c).__name__ for c in cols)
            return _maybe_retained(
                {"rows": rows, "client_dtype": dtypes},
                cols, "np" if TARGET == "np" else "columnar")
        if TARGET == "df" and MODE == "materialize":
            t1 = time.perf_counter()
            df = client.query_dataframe(SQL_CH,
                                        settings=codec_settings or None,
                                        query_id=cell_query_id())
            return _finish_df(df, t1, path="ch_native_driver")
        if TARGET == "polars" and MODE == "materialize":
            # D52: polars из колоночной выдачи драйвера. Arrow у Native TCP
            # нет, значения уже объекты питона - класс 1, как у кортежей
            cols, types = client.execute(SQL_CH, columnar=True,
                                         with_column_types=True,
                                         settings=codec_settings or None,
                                         query_id=cell_query_id())
            t1 = time.perf_counter()
            frame = _polars_from_columns(cols, [c[0] for c in types])
            return _finish_polars(frame, t1, path="ch_native_driver")
        if MODE == "dfgate":
            t1 = time.perf_counter()
            df = client.query_dataframe(SQL_CH,
                                        settings=codec_settings or None,
                                        query_id=cell_query_id())
            return _dfgate(df, time.perf_counter() - t1)
        rows, cols = client.execute(SQL_CH, with_column_types=True,
                                    settings=codec_settings or None,
                                    query_id=cell_query_id())
        return _rows_result(rows, [c[0] for c in cols], path="ch_native")

    def consume_framed(client, t0):
        # кадр чтения клетки в колонку frame: у нативного провода он один -
        # max_block_size, и только когда ось размера порции задана явно
        return _extras(consume(client, t0), frame=_ch_block_frame())

    # F3: факт кодека перечитывается с сервера ПОСЛЕ замера, отдельным
    # соединением - вне окна коннекта и вне окна исполнения. Проверка от
    # переноса не слабеет: она спрашивает сервер, принимает ли он метод,
    # которым ехал ответ, а не состояние конкретного сокета. Клетка с чужим
    # кодеком падает так же громко, только замер уже не искажен.
    #
    # F01: до среза stand-g-v1.0 проба стояла ЗДЕСЬ, между _execs и возвратом
    # пути, и при BENCH_EXECS=1 это все еще было ВНУТРИ окна: часы одиночного
    # контракта останавливает обвязка, когда путь вернул результат. Проба со
    # своим соединением стоила сжатым клеткам 0.005-0.0095 с, то есть цена
    # проверки попадала в "цену сжатия". Теперь путь пробу РЕГИСТРИРУЕТ, а
    # зовет ее обвязка после остановки часов; длительность уезжает отдельной
    # колонкой post_check_s. Тело пробы не меняется.
    if codec_settings:
        def _post_check_codec():
            probe = _ch_connect_native()
            try:
                _ch_native_assert_codec(probe, codec_settings)
            finally:
                _close_native(probe)

        register_post_check(_post_check_codec)
    return _execs(connect, consume_framed)


from _rowbinary import parse_rowbinary_with_names_and_types  # noqa: E402


def _ch_http_rows(client):
    """Разбор HTTP-ответа в list[tuple] для форматов, у которых парсер есть.
    Возвращает (rows, colnames|None). Используется и целевой структурой
    tuples, и режимами digest / dfgate."""
    # C2: зачетный запрос клетки едет с явным query_id - серверная секция
    # суммирует записи query_log по нему, а не берет самый тяжелый запрос окна
    settings = ch_qid_settings(_ch_settings())
    if FMT in CH_NO_PARSER:
        raise SystemExit(
            f"формат {FMT} питоновского парсера не имеет - работает только "
            "drain. Это честная пустая клетка, она проговаривается на слайде")
    if FMT == "Native":
        result = client.query(SQL_CH, settings=settings)
        return result.result_rows, result.column_names
    if FMT in ARROW_FMTS:
        table = _ch_http_arrow(client)
        return _arrow_to_tuples(table), table.column_names

    raw = _ch_raw_query(client, SQL_CH, fmt=FMT, settings=settings)
    if FMT in ("JSONEachRow", "JSONCompactEachRow"):
        # json.loads принимает bytes - целиком в str блоб не разворачиваем,
        # иначе в пике лежали бы две копии ответа и peak RSS был бы про нас,
        # а не про формат. Типы у JSON-форматов CH деградируют по дефолтам,
        # это видно в client_dtype - ради этого колонка и заведена.
        rows = [json.loads(line) for line in raw.splitlines() if line]
        if FMT == "JSONEachRow":
            names = list(rows[0].keys()) if rows else None
            return [tuple(d.values()) for d in rows], names
        return [tuple(r) for r in rows], None
    # TextIOWrapper декодирует потоком, по той же причине
    text = io.TextIOWrapper(io.BytesIO(raw), encoding="utf-8", newline="")
    # D52: у форматов без заголовка имена колонок берутся из объявленной
    # схемы клетки - иначе датафрейм для сверки конечного фрейма получал бы
    # номера вместо имен и не приводился бы к схеме
    names = _declared_names()
    if FMT == "TabSeparated":
        rows = [tuple(line.rstrip("\n").split("\t"))
                for line in text if line.strip()]
        return rows, names
    if FMT == "CSV":
        return [tuple(row) for row in csv.reader(text)], names
    raise SystemExit(f"BENCH_FMT={FMT!r}, ожидается одно из {CH_FMTS}")


def _ch_http_arrow(client):
    """Полная Arrow-таблица тем форматом, который просили: у ArrowStream
    кадрирование другое, поэтому собираем ее из батчей, а не через
    query_arrow (тот всегда ходит форматом Arrow)."""
    settings = ch_qid_settings(_ch_settings())
    if FMT == "Arrow":
        return client.query_arrow(SQL_CH, settings=settings)
    if FMT == "ArrowStream":
        with client.query_arrow_stream(SQL_CH, settings=settings) as stream:
            chunks = list(stream)
        if not chunks:
            raise SystemExit("ArrowStream не отдал ни одного батча")
        if isinstance(chunks[0], pa.Table):  # версия драйвера решает, что отдать
            return pa.concat_tables(chunks)
        return pa.Table.from_batches(chunks)
    if FMT == "Parquet":
        raw = _ch_raw_query(client, SQL_CH, fmt="Parquet", settings=settings)
        return pq.read_table(io.BytesIO(raw))
    raise SystemExit(f"BENCH_FMT={FMT!r}: Arrow-таблицу дают Arrow, "
                     "ArrowStream и Parquet")


def _ch_point_consume():
    """Серия точечных запросов П4 по HTTP: BENCH_EXECS исполнений в одном
    соединении, каждый - Arrow-ответ на один ключ. Ключи снимаются в
    connect-фазе (вне окон исполнений) и обстреливаются в воспроизводимо
    случайном порядке. Ключ вшивается в текст ЛИТЕРАЛОМ - у
    clickhouse-connect свой синтаксис параметров, а server-side prepared
    на HTTP нет в любом случае (пометка в CONTRACT).
    Таблицы narrow у ClickHouse созданы с ORDER BY tuple(): точечный запрос
    здесь честно платит полный скан - это находка клетки, а не ошибка."""
    if MODE != "materialize":
        raise SystemExit(
            f"BENCH_MODE={MODE} на точечной клетке П4 пути ch_http: "
            "точечная форма снимается только в materialize - клетка с "
            "чужим режимом уехала бы в CSV под чужой меткой mat_level")
    if FMT not in ("", "Arrow"):
        raise SystemExit(
            f"BENCH_FMT={FMT!r} на точечной клетке ch_http: П4 по HTTP идет "
            "Arrow-ответом (query_arrow) - другой формат уехал бы в CSV "
            "под чужой меткой")
    if _wire_codec():
        # query_arrow внутри драйвера ходит тем же raw_query, но объект ответа
        # наружу не отдает: снять Content-Encoding и распаковать поток тут
        # негде. В сетке такой клетки нет - лучше отказ, чем тихий разбор
        # сжатого блоба как Arrow
        raise SystemExit(
            f"BENCH_CODEC={CODEC} на точечной клетке ch_http: транспортное "
            "сжатие на этой форме не снимается, в сетке такой клетки нет")
    state = {}

    def connect():
        client = _ch_connect_http()
        rows = client.query(setup_sql(point_keys_sql("ch", max(EXECS, 1))),
                            settings=_ch_settings()).result_rows
        state["keys"] = shuffle_keys(r[0] for r in rows)
        state["i"] = 0
        return client

    def consume(client, t0):
        i = state["i"]
        state["i"] += 1
        key = state["keys"][i % len(state["keys"])]
        table = client.query_arrow(point_sql("ch", key),
                                   settings=ch_qid_settings(_ch_settings()))
        return {"rows": table.num_rows, "ttfb_s": None,
                "client_dtype": ";".join(str(f.type) for f in table.schema)}

    return _execs(connect, consume)


def path_ch_http():
    """Один клиент, один HTTP, один сервер - меняется одно слово FORMAT.
    Самая чистая ось серии: провод другой, все остальное то же."""
    if MODE == "srvcost":
        return _srvcost("ch")
    if QUERY_FORM == "point":
        return _ch_point_consume()
    if QUERY_FORM == "keyset":
        raise SystemExit(
            "BENCH_QUERY_FORM=keyset у пути ch_http: keyset-выгрузка - "
            "клетка psycopg (П6), у HTTP-клиента этой формы в сетке нет")
    if FMT not in CH_FMTS:
        raise SystemExit(f"BENCH_FMT={FMT!r}, ожидается одно из {CH_FMTS}")

    def consume(client, t0):
        settings = ch_qid_settings(_ch_settings())
        if MODE == "drain":
            # уровень доставки: сырые байты, никакого разбора. Транспортный
            # кодек снимает _byte_blocks - распаковка входит в окно замера,
            # это и есть цена сжатия на стороне получателя
            reader = _ch_raw_stream(client, SQL_CH, fmt=FMT, settings=settings)
            try:
                # C1: усыпление идет на КАЖДЫЙ кусок ответа - для HTTP это
                # и есть естественная единица потока (HTTP_BLOCK_BYTES)
                return _drain(_byte_blocks(reader), t0, bytes_of=len,
                              klass="raw", path="ch_http")
            finally:
                _close(reader)
        if MODE == "dfgate":
            if FMT not in ("Native",) + tuple(ARROW_FMTS) and RESTORE_TYPES:
                # сверка Д2: фрейм из строковых кортежей текстового формата,
                # восстановление и гейт - общим разбором _rows_result
                rows, cols = _ch_http_rows(client)
                return _rows_result(rows, cols, path="ch_http")
            t1 = time.perf_counter()
            if FMT == "Native":
                df = client.query_df(SQL_CH, settings=settings)
            else:
                df = _ch_http_arrow(client).to_pandas()
            return _dfgate(df, time.perf_counter() - t1)
        if MODE == "digest" and FMT == "RowBinaryWithNamesAndTypes":
            # D52, ревью п.3: сверяется ФАКТИЧЕСКИЙ поток RowBinary - разбор
            # по типам его собственного заголовка, вне окна замера. Имена
            # колонок заголовка обязаны совпасть с объявленной схемой
            raw = _ch_raw_query(client, SQL_CH, fmt=FMT, settings=settings)
            names, types, rows = parse_rowbinary_with_names_and_types(raw)
            declared = _declared_names()
            if declared and [n.lower() for n in names] != [n.lower() for n in declared]:
                raise SystemExit(f"RowBinary: имена колонок заголовка {names[:3]}... "
                                 f"не совпали с объявленной схемой {declared[:3]}...")
            rec = _rows_result(rows, names, path="ch_http")
            rec["proto_mode"] = "digest_parsed=RowBinaryWithNamesAndTypes"
            return rec
        if MODE == "digest" and FMT in CH_NO_PARSER:
            # RowBinary без имен и Values: заголовка с типами нет, разбор
            # не объявлен - сверка форматом Native того же пути, и это
            # ОГРАНИЧЕННАЯ сверка (данные запроса, не кадрирование байт);
            # в строке сверки это названо явно
            print(f"# digest: формат {FMT} без парсера - ОГРАНИЧЕННАЯ сверка "
                  "форматом Native того же пути", file=sys.stderr)
            result = client.query(SQL_CH, settings=settings)
            rec = _rows_result(result.result_rows, result.column_names,
                               path="ch_http")
            rec["proto_mode"] = "digest_via=Native:limited"
            return rec
        if MODE == "digest":
            # Срез C1. Сверка идет ПО ТОМУ ЖЕ пути и ТЕМ ЖЕ форматом, что и
            # замер: кодек codec/v1 канонизирует по объявленной логической
            # схеме клетки, поэтому Arrow-объект и кортежи дают ОДИН digest, и
            # подменять формат ради сверки больше не нужно.
            #
            # Прежний код всегда строил кортежи ("хеш по канонической выдаче,
            # а не по тому, во что клиент ее сложил"). Под старым текстовым
            # порядкозависимым хешем это было единственным способом сравнить
            # пути, но у него две беды: проверялся не тот объект, который
            # клетка меряет, и цена. На 10M x 50 сверка Arrow-клетки через
            # кортежи стоила 486 с и 35 ГБ пика против 5.8 с самой выборки
            # (подготовительный прогон 2026-09-15, ch).
            if FMT in ARROW_FMTS:
                return _arrow_result(_ch_http_arrow(client), path="ch_http")
            rows, cols = _ch_http_rows(client)
            return _rows_result(rows, cols, path="ch_http")
        if TARGET == "raw":
            t1 = time.perf_counter()
            raw = _ch_raw_query(client, SQL_CH, fmt=FMT, settings=settings)
            # F07/F44: сырой ответ живет до stop через общую воронку raw
            return _raw_result(raw, t1)
        if MODE == "materialize" and FMT == "Native" and TARGET == "polars":
            # D52: polars из колоночной выдачи clickhouse-connect - Arrow у
            # Native по HTTP нет, зеркало клетки ch_native до polars
            result = client.query(SQL_CH, settings=settings)
            t1 = time.perf_counter()
            frame = _polars_from_columns(result.result_columns,
                                         result.column_names)
            return _finish_polars(frame, t1, path="ch_http")
        if MODE == "materialize" and FMT == "CSV" and TARGET in ("df", "polars"):
            # D52: CSV до датафрейма ПАРСЕРОМ БИБЛИОТЕКИ (read_csv), а не
            # модулем csv через кортежи. Имена колонок - из объявленной
            # схемы клетки: у FORMAT CSV заголовка нет
            raw = _ch_raw_query(client, SQL_CH, fmt=FMT, settings=settings)
            names = _declared_names()
            if TARGET == "polars":
                return _polars_from_csv(raw, names=names, path="ch_http")
            return _df_from_csv(raw, names=names, path="ch_http")
        if MODE == "materialize" and FMT == "JSONEachRow" \
                and TARGET in ("df", "polars", "arrow"):
            # D52: JSONEachRow парсерами библиотек: pandas.read_json,
            # polars.read_ndjson, pyarrow.json. 64-битные целые ClickHouse
            # по умолчанию пишет в JSON строками - приведение к схеме
            # (extra2_s) их возвращает в числа, и это часть цены формата
            raw = _ch_raw_query(client, SQL_CH, fmt=FMT, settings=settings)
            if TARGET == "polars":
                return _polars_from_ndjson(raw, path="ch_http")
            if TARGET == "arrow":
                return _arrow_from_ndjson(raw, path="ch_http")
            return _df_from_ndjson(raw, path="ch_http")
        if TARGET in ("arrow", "df_arrowdtype", "polars") or \
                (TARGET == "df" and FMT in ARROW_FMTS):
            return _arrow_result(_ch_http_arrow(client), path="ch_http")
        if TARGET == "np":
            if FMT != "Native":
                raise SystemExit("query_np ходит форматом Native - "
                                 f"с BENCH_FMT={FMT} это была бы подмена провода")
            arr = client.query_np(SQL_CH, settings=settings)
            return _maybe_retained(
                {"rows": len(arr),
                 "client_dtype": str(getattr(arr, "dtype", ""))}, arr, "np")
        if TARGET == "df":
            if FMT != "Native":
                # Ось Д2: датафрейм ИЗ ТЕКСТОВОГО провода законен только на
                # клетке восстановления типов - фрейм собирается из строковых
                # кортежей общим разбором (_rows_result), и там же считается
                # extra2_s доводки до типов. Без флага - прежний запрет:
                # query_df ходит Native, и подменять провод молча нельзя.
                if RESTORE_TYPES:
                    rows, cols = _ch_http_rows(client)
                    return _rows_result(rows, cols, path="ch_http")
                raise SystemExit("query_df ходит форматом Native - "
                                 f"с BENCH_FMT={FMT} это была бы подмена провода")
            t1 = time.perf_counter()
            df = client.query_df(SQL_CH, settings=settings)
            return _finish_df(df, t1, path="ch_native")
        if TARGET == "columnar":
            if FMT != "Native":
                raise SystemExit("колоночную выдачу clickhouse-connect строит "
                                 f"из Native, а BENCH_FMT={FMT}")
            result = client.query(SQL_CH, settings=settings)
            cols = result.result_columns
            return _maybe_retained(
                {"rows": len(cols[0]) if cols else 0,
                 "client_dtype": ";".join(type(c).__name__ for c in cols)},
                cols, "columnar")
        rows, cols = _ch_http_rows(client)
        return _rows_result(rows, cols, path="ch_http")

    def consume_framed(client, t0):
        # кадр чтения клетки в колонку frame: у слива это кусок HTTP-ответа,
        # у остальных режимов - серверный блок (когда ось задана явно)
        frame = f"http={HTTP_BLOCK_BYTES}" if MODE == "drain" \
            else _ch_block_frame()
        return _extras(consume(client, t0), frame=frame)

    return _execs(_ch_connect_http, consume_framed)


def path_ch_pg_emu():
    """Тот же psycopg-код, что и в pg_psycopg, но провод ведет в ClickHouse,
    притворяющийся постгресом (:9005). Главная пара серии: клиент не
    изменился ни на строку.

    streaming=False: pg-эмуляция ClickHouse держит только простой протокол,
    курсора с DECLARE / FETCH у нее нет - клетка drain честно помечается
    drain:buffered, а не притворяется потоком."""
    # у эмуляции кодировка НЕ ось: она держит только simple text, и пустой
    # BENCH_FMT честнее свести к байзлайну, чем потерять все клетки пути.
    # Эффективное значение передается аргументом, глобал не трогается:
    # мутация глобала из этого модуля молча разъезжалась бы с читателями
    # в paths_pg
    fmt = FMT or "simple_text"
    if MODE == "srvcost":
        # на том конце ClickHouse - и серверную цену надо спрашивать у него,
        # EXPLAIN ANALYZE SERIALIZE эмуляция не понимает
        return _srvcost("ch")

    def consume(conn, t0):
        rec = _pg_consume(conn, t0, SQL_CH_EMU, streaming=False, fmt=fmt,
                          path="ch_pg_emu")
        # эмуляция держит только простой протокол
        rec.setdefault("proto_mode", "simple")
        return rec

    return _execs(_pg_emu_connect, consume)


def path_ch_mysql_emu():
    """Третье лицо того же движка: mysql-эмуляция на :9004.

    Пустая клетка по построению: бинарного результата тут нет ни при каких
    настройках - эмуляция не поддерживает prepared statements, а в mysql-wire
    формат пристегнут к способу исполнения. Порт требует пользователя с
    double_sha1_password (CH_MYSQL_USER / CH_MYSQL_PASSWORD).

    Клиент выбирается осью BENCH_VARIANT: пусто - pymysql (чистый Python),
    mysqlclient - MySQLdb (C-разбор). Провод и семантика drain/mat одни и те
    же (SSCursor-аналог у MySQLdb - MySQLdb.cursors.SSCursor) - пара A11
    развязывает цену протокола от цены клиента.
    """
    if VARIANT not in ("", "mysqlclient"):
        raise SystemExit(
            f"BENCH_VARIANT={VARIANT!r} у пути ch_mysql_emu: ожидается пусто "
            "(pymysql) или mysqlclient")
    if VARIANT == "mysqlclient":
        if MySQLdb is None:
            raise SystemExit(
                "mysqlclient (MySQLdb) не установлен - клетка mysqlclient "
                "пути ch_mysql_emu пропущена")
        driver, ss_cursor = MySQLdb, MySQLdb.cursors.SSCursor
    else:
        if pymysql is None:
            raise SystemExit(
                "pymysql не установлен - путь ch_mysql_emu пропущен")
        driver, ss_cursor = pymysql, pymysql.cursors.SSCursor
    if MODE == "srvcost":
        return _srvcost("ch")
    if CODEC != "none":
        raise SystemExit("сжатия у mysql-эмуляции ClickHouse нет")
    if FMT not in ("", "text", "mysql_text"):
        raise SystemExit(
            f"BENCH_FMT={FMT!r}: на mysql-эмуляции формат один - текстовый "
            "построчный. Бинарного результата там нет по построению")

    def connect():
        return driver.connect(
            host=os.environ.get("CH_MYSQL_HOST", CH["host"]),
            port=int(os.environ.get("CH_MYSQL_PORT", "9004")),
            user=os.environ.get("CH_MYSQL_USER", CH["user"]),
            password=os.environ.get("CH_MYSQL_PASSWORD", CH["password"]),
            database=CH["db"],
        )

    def consume(conn, t0):
        if MODE == "materialize" and TARGET == "polars":
            # то же соединение, но вычитывает курсор polars, а не мы. Сверять
            # BENCH_FMT тут нечего - формат на этом проводе один и он уже
            # проверен выше
            return _polars_read_database(conn, SQL_CH_EMU, path="ch_mysql_emu")
        if MODE == "drain":
            # SSCursor вместо обычного: дефолтный курсор обоих клиентов
            # вычитывает ВЕСЬ ответ в список кортежей еще внутри execute -
            # время до первой строки было бы почти равно полному, а в пике
            # памяти лежал бы весь ответ. Это и есть та подмена, из-за которой
            # drain мерил бы не доставку (см. CONTRACT)
            cur = conn.cursor(ss_cursor)
            try:
                cur.execute(SQL_CH_EMU)

                def batches():
                    while True:
                        chunk = cur.fetchmany(BATCH)
                        if not chunk:
                            return
                        yield chunk
                # T11: кадр чтения у mysql-эмуляции - тот же серверный
                # блок ClickHouse, что у ch_native и ch_pg_emu, поэтому и
                # токен колонки frame один на все три пути (CONTRACT 7.1);
                # порция SSCursor берется из той же оси BENCH_BATCH. Пусто,
                # когда ось не задана: кадр решает дефолт сборки сервера
                return _extras(
                    _drain(batches(), t0, rows_of=len, klass="rows",
                           path="ch_mysql_emu"),
                    frame=_ch_block_frame())
            finally:
                _close(cur)

        cur = conn.cursor()
        cur.execute(SQL_CH_EMU)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description] if cur.description else None
        return _rows_result(rows, cols, path="ch_mysql_emu")

    return _execs(connect, consume)


def path_ch_flightsql():
    """Arrow Flight SQL (direct CH, :9090): сквозной Arrow - формат на
    проводе совпадает с форматом в памяти клиента."""
    if MODE == "srvcost":
        return _srvcost("ch")
    if flightsql is None:
        raise SystemExit("adbc-driver-flightsql не установлен - путь пропущен")
    if CODEC != "none":
        raise SystemExit(
            f"BENCH_CODEC={CODEC}: серверные ручки формата во Flight-фронтенд "
            "через ADBC не прокидываются - гипотеза сжатия Flight снимается "
            "отдельно")

    def connect():
        # basic-хендшейк Flight у текущего ClickHouse падает на стороне
        # сервера - работает только авторизация заголовком
        cred = f"{CH['user']}:{CH['password']}"
        token = base64.b64encode(cred.encode()).decode()
        kwargs = {"adbc.flight.sql.authorization_header": "Basic " + token}
        # Потолок принимаемого сообщения gRPC. У клиента он по умолчанию
        # 16 МиБ, а ClickHouse отдает батч ЦЕЛИКОМ: на cb_w10_1m это 22.99 МБ,
        # и проба падает "received message larger than max (22990993 vs
        # 16777216)". Путь pg_flightsql эту опцию ставит с самого начала
        # (_flight_db_kwargs), а здесь ее не было - то есть падал КЛИЕНТ, а не
        # реализация Flight на сервере, и без правки это ушло бы в отчет как
        # отказ ClickHouse. Молчаливый пропуск опции запрещен планом сессии:
        # если ее нет в пакете, говорим вслух.
        if FlightOptions is not None and hasattr(FlightOptions,
                                                 "WITH_MAX_MSG_SIZE"):
            kwargs[FlightOptions.WITH_MAX_MSG_SIZE.value] = str(2 ** 31 - 1)
        else:
            print("# ch_flightsql: у пакета adbc-driver-flightsql нет опции "
                  "with_max_msg_size - выдача больше 16 МиБ не приедет, и это "
                  "ограничение КЛИЕНТА, а не сервера", file=sys.stderr)
        return flightsql.connect(
            os.environ.get("CH_FLIGHT_URI", f"grpc://{CH['host']}:9090"),
            db_kwargs=kwargs,
        )

    if QUERY_FORM == "point":
        # П4 по Flight: ключ вшивается литералом (параметры у flight-курсора
        # передаются несопоставимо с остальными клиентами), выборка ключей -
        # в connect-фазе, вне окон исполнений
        if MODE != "materialize":
            raise SystemExit(
                f"BENCH_MODE={MODE} на точечной клетке П4 пути ch_flightsql: "
                "точечная форма снимается только в materialize - клетка с "
                "чужим режимом уехала бы в CSV под чужой меткой mat_level")
        state = {}

        def connect_point():
            conn = connect()
            with conn.cursor() as cur:
                cur.execute(
                    _flight_sql(setup_sql(point_keys_sql("ch", max(EXECS, 1)))))
                keys = [r[0] for r in cur.fetchall()]
            state["keys"] = shuffle_keys(keys)
            state["i"] = 0
            return conn

        def consume_point(conn, t0):
            i = state["i"]
            state["i"] += 1
            key = state["keys"][i % len(state["keys"])]
            with conn.cursor() as cur:
                cur.execute(_flight_sql(point_sql("ch", key)))
                return _arrow_result(cur.fetch_arrow_table(),
                                     path="ch_flightsql")

        return _execs(connect_point, consume_point)

    # текст с хвостом SETTINGS (как у эмуляций): у Flight-сессии нет настроек
    # сессии, и без хвоста единственный CH-путь ехал бы на серверном дефолте
    # потоков рядом с прибитым max_threads=1 у остальных (порядок строк
    # numbers() при >1 потоке плавает - digest между раундами расходился бы)
    query = _flight_sql(SQL_CH_EMU)

    def consume(conn, t0):
        with conn.cursor() as cur:
            cur.execute(query)
            if MODE == "drain":
                reader = cur.fetch_record_batch()
                return _drain(reader, t0, rows_of=lambda b: b.num_rows,
                              klass="arrow", path="ch_flightsql")
            return _arrow_result(cur.fetch_arrow_table(), path="ch_flightsql")

    return _execs(connect, consume)


def _ch_adbc_driver() -> str:
    """Путь к native-драйверу ClickHouse для ADBC driver manager.

    dbc ставит драйвер в ~/.config/adbc/drivers/clickhouse_<os>_<arch>_v<ver>/
    и пишет манифест clickhouse.toml, но adbc-driver-manager 1.4.0 (лок серии)
    манифесты по имени не читает: driver="clickhouse" уходит в dlopen
    libclickhouse.so и падает (G3 v14, 2026-09-12). Порядок: явный
    CH_ADBC_DRIVER, иначе .so из раскладки dbc (самая свежая версия), иначе
    имя - для менеджеров, которые манифесты уже понимают."""
    explicit = os.environ.get("CH_ADBC_DRIVER")
    if explicit:
        return explicit
    import glob
    found = sorted(glob.glob(os.path.expanduser(
        "~/.config/adbc/drivers/clickhouse_*/libadbc_driver_clickhouse.so")))
    return found[-1] if found else "clickhouse"


def path_ch_adbc_http():
    """ADBC ClickHouse поверх HTTP: тот же запросный текст через driver='clickhouse'
    и transport=protocol=http, с теми же контрактными ограничениями, что и
    прочие Arrow-пути CH."""
    if MODE == "srvcost":
        return _srvcost("ch")
    if adbc_manager is None:
        raise SystemExit("adbc-driver-manager не установлен - путь пропущен")
    if CODEC != "none":
        raise SystemExit(
            f"BENCH_CODEC={CODEC}: ADBC over HTTP идет как отдельный клиент "
            "без отдельной оси сжатия в раннере")

    uri = os.environ.get(
        "CH_ADBC_URI",
        f"clickhouse://{CH['host']}:{CH['http_port']}?protocol=http")
    if "protocol=http" not in uri:
        uri = f"{uri}{'&' if '?' in uri else '?'}protocol=http"

    def connect():
        return adbc_manager.connect(
            driver=_ch_adbc_driver(),
            # опции "database" у native-драйвера 0.1.0 нет (INVALID_ARGUMENT:
            # unknown database option); база и не нужна - текст запроса CH
            # всегда квалифицирован именем базы (bench_sql: FROM bench.<таблица>)
            db_kwargs={
                "uri": uri,
                "username": os.environ.get("CH_ADBC_USER", CH["user"]),
                "password": os.environ.get("CH_ADBC_PASSWORD", CH["password"]),
            },
            autocommit=True,
        )

    if QUERY_FORM == "point":
        if MODE != "materialize":
            raise SystemExit(
                f"BENCH_MODE={MODE} на точечной клетке П4 пути ch_adbc_http: "
                "точечная форма снята только в materialize")
        state = {}

        def connect_point():
            conn = connect()
            with conn.cursor() as cur:
                cur.execute(
                    _flight_sql(
                        setup_sql(point_keys_sql("ch", max(EXECS, 1)))))
                state["keys"] = shuffle_keys(r[0] for r in cur.fetchall())
            state["i"] = 0
            return conn

        def consume_point(conn, t0):
            i = state["i"]
            state["i"] += 1
            key = state["keys"][i % len(state["keys"])]
            with conn.cursor() as cur:
                cur.execute(_flight_sql(point_sql("ch", key)))
                return _arrow_result(cur.fetch_arrow_table(),
                                     path="ch_adbc_http")

        return _execs(connect_point, consume_point)

    query = _flight_sql(SQL_CH_EMU)

    def consume(conn, t0):
        with conn.cursor() as cur:
            cur.execute(query)
            if MODE == "drain":
                reader = cur.fetch_record_batch()
                return _drain(reader, t0, rows_of=lambda b: b.num_rows,
                              klass="arrow", path="ch_adbc_http")
            return _arrow_result(cur.fetch_arrow_table(), path="ch_adbc_http")

    return _execs(connect, consume)


PATHS_CH = {
    "ch_native": path_ch_native,
    "ch_http": path_ch_http,
    "ch_pg_emu": path_ch_pg_emu,
    "ch_mysql_emu": path_ch_mysql_emu,
    "ch_flightsql": path_ch_flightsql,
    "ch_adbc_http": path_ch_adbc_http,
}
