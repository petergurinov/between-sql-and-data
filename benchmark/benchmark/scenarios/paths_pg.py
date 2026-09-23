"""Пути PostgreSQL: psycopg (курсор клиента, COPY, серверный курсор, ось
самоускорения), ADBC и connectorx.

Общий код psycopg (_pg_consume и хелперы) живет здесь и переиспользуется
путем ch_pg_emu из paths_ch: в этом и смысл пары - клиент не меняется вообще,
меняется только то, кто на том конце провода.
"""

import itertools
import os
import sys
import time

# Пакет ADBC: соединение открывает общая фабрика _session, но импорт остается
# здесь - отсутствие драйвера обязано быть видно при импорте модуля путей,
# а не в середине замера (--list-paths печатает набор до сетки)
import adbc_driver_postgresql.dbapi as pgdbapi   # noqa: F401

# psycopg нужен не только замерным путям на нем: у pg_flightsql им снимается
# схема запроса ДО замера (см. _flight_assert_types) - неподдержанный тип
# роняет Flight-порт целиком, и узнать про него надо по дешевому проводу
import psycopg

try:  # connectorx нужен ровно одному пути - его отсутствие не должно ломать
    import connectorx as cx  # импорт всего модуля и остальные пути
except ImportError:
    cx = None

try:  # Flight SQL - опциональный пакет (тот же, что у пути ch_flightsql)
    import adbc_driver_flightsql.dbapi as flightsql
    from adbc_driver_flightsql import DatabaseOptions as FlightOptions
except ImportError:
    flightsql = None
    FlightOptions = None

from bench_axes import (ADBC_COPY, ADBC_NUMERIC_AS, BATCH, BATCH_SET, CANVAS,
                        CX_PROTOCOL, EXECS, FMT, ITERSIZE, KEYSET_CHUNK, MODE,
                        PG, PG_COPY_FMTS, PG_FMTS, PREPARE_THRESHOLD,
                        QUERY_FORM, TARGET)
from bench_sql import (KEYSET_POS, SQL_PG, _expected_rows, keyset_sql,
                       point_keys_sql, point_sql)
from bench_runtime import (_df_from_csv, _polars_from_csv,  # noqa: I001
                           DRAIN_FORM, _arrow_result, _assert_pg_codec,
                           _assert_polars_wire, _dfgate, _drain, _execs,
                           _extras, _maybe_retained, _pg_connect,
                           _polars_read_database, _raw_result, _rows_result, _single,
                           _srvcost, drain_class_of, shuffle_keys)
# F15: маркер служебных запросов клетки
from _srvcontrol import setup_sql
# контракт сессии PostgreSQL берется по имени модуля, а не from-импортом:
# тесты и гейт подменяют кортеж PG_SESSION_SETTINGS целиком, и снимок на
# импорте сделал бы подмену невидимой (та же причина, что в _session)
import _srvcontrol
# класс остановки клетки с объявленным именем причины (колонка error_class)
from _measure import PathExit
# F02: единая фабрика соединений и обратное чтение контракта сессии - тот же
# модуль зовет гейт сессии раннера
import _session

# Один счетчик имен серверных курсоров на весь процесс: его делят
# _pg_drain_stream и path_pg_server_cursor, и два независимых счетчика
# рано или поздно дали бы совпадающие имена курсоров в одном соединении
_CURSOR_SEQ = itertools.count(1)

# Фактический протокольный режим драйвера (колонка proto_mode): скрытая
# переменная разговорчивости провода и ttfb. Значения объявленные, не
# снифферские: simple - простой протокол; prepared - extended с prepare=True;
# extended - extended без подготовки (binary=True); copy - кадрирование COPY
_PG_PROTO = {"simple_text": "simple", "ext_text": "prepared",
             "ext_binary": "extended",
             "copy_text": "copy", "copy_binary": "copy", "copy_csv": "copy"}


def _pg_proto_mode(fmt: str) -> str:
    return _PG_PROTO.get(fmt, "")


def _pg_fmt(path: str, fmt: str = None) -> str:
    """BENCH_FMT для путей, у которых кодировка значений НЕ является осью.

    У pg_psycopg формат - это и есть ось кодировки, там пустое значение
    обязано падать. У pg_server_cursor и pg_execs ось другая (размер порции и
    самоускорение), и пустой BENCH_FMT честнее свести к байзлайну корпуса -
    simple_text, - чем потерять всю ось из-за незаданной чужой переменной.
    """
    effective = FMT if fmt is None else fmt
    if not effective:
        return "simple_text"
    if effective not in PG_FMTS:
        raise SystemExit(
            f"BENCH_FMT={effective!r} у пути {path} не из набора pg-wire: "
            f"{PG_FMTS}")
    return effective


def _pg_execute_kwargs(fmt: str = None) -> dict:
    """Ручки psycopg, соответствующие BENCH_FMT.

    Простой протокол psycopg берет только когда параметров нет И формат
    текстовый, поэтому binary=True меняет сразу две вещи - ячейка ext_text
    обязательна, иначе кодировка и протокол перепутаны в одном замере.
    """
    effective = FMT if fmt is None else fmt
    if effective == "ext_text":
        return {"prepare": True}
    if effective == "ext_binary":
        return {"binary": True}
    return {}


def _pg_drain_stream(conn, t0, query, itersize: int, fmt: str = None,
                     path: str = "pg_psycopg"):
    """Честный drain для psycopg: серверный курсор с itersize.

    Так требует контракт, и вот почему. На обычном курсоре psycopg уже к
    моменту возврата execute забрал ВЕСЬ ответ в буфер libpq: время до первой
    строки почти равно полному, а в пике памяти лежит весь ответ. Такой
    замер называется drain, а меряет материализацию - и он давал бы вывод
    "arrow-стрим втрое дешевле по памяти", сравнивая разное.

    Плата за честность проговаривается вслух: на серверном курсоре кадрирование
    всегда DECLARE / FETCH, поэтому в drain различие simple против extended
    исчезает - остается только кодировка значений (текст или двоичная). Ось
    кодировки-кадрирования целиком живет в materialize.
    """
    effective = FMT if fmt is None else fmt
    name = f"bench_{os.getpid()}_{next(_CURSOR_SEQ)}"
    if effective == "simple_text":
        print("# drain: simple-протокол недостижим через серверный курсор - "
              "кадрирование FETCH, кодировка текстовая", file=sys.stderr)
    with conn.cursor(name=name) as cur:
        cur.itersize = itersize
        cur.execute(query, binary=(effective == "ext_binary"))
        return _extras(
            _drain(cur, t0, rows_of=lambda row: 1, klass="rows", path=path),
            frame=f"itersize={itersize}")


def _pg_drain_buffered(cur, t0, path: str = "pg_psycopg"):
    """drain там, где стримить нельзя ПО СУЩЕСТВУ пути или где так просит ось.

    Три случая: pg-эмуляция ClickHouse (курсоров с DECLARE / FETCH у нее
    нет), ось самоускорения (серверный курсор исполняет DECLARE - другой текст
    запроса, автоподготовка psycopg на нем не срабатывает никогда, и ось мерила
    бы пустоту) и ось формы слива BENCH_DRAIN_FORM=buffered (HC7): обычный
    курсор клиента с fetchmany и выбросом против серверного курсора - две
    разные физики под одним именем drain, и разницу между ними надо мерить, а
    не подразумевать. Клетка помечается drain:buffered и на разборе не
    сравнивается ни по памяти, ни по времени до первой строки.
    """
    def batches():
        while True:
            chunk = cur.fetchmany(BATCH)
            if not chunk:
                return
            yield chunk
    # T11: словарь токенов колонки frame один на весь корпус (CONTRACT 7.1).
    # У pg-эмуляции провод чужой, но поток кадрирует серверный блок
    # ClickHouse, и зовется он так же, как у ch_native и ch_mysql_emu -
    # max_block_size; пусто, когда ось порции не задана явно: тогда кадр
    # решает дефолт сборки сервера, и число обещало бы точку оси, которой в
    # клетке нет. У psycopg и у оси самоускорения кадр свой - fetchmany
    # клиентского курсора, и называть его max_block_size было бы враньем.
    frame = (f"max_block_size={BATCH}" if BATCH_SET else "") \
        if path == "ch_pg_emu" else f"fetchmany={BATCH}"
    return _extras(
        _drain(batches(), t0, rows_of=len, klass="buffered", path=path),
        frame=frame)


def _pg_consume(conn, t0, query, streaming=True, fmt: str = None,
                path: str = "pg_psycopg"):
    """Один код на настоящий PostgreSQL и на pg-эмуляцию ClickHouse: в этом
    и смысл пары - клиент не меняется вообще, меняется только то, кто на том
    конце провода.

    Текст запроса приходит аргументом: эмуляция говорит по pg-wire, но
    разбирает SQL ClickHouse, и на холсте генерации это разные запросы
    (generate_series против numbers) - при том же самом клиенте.

    fmt приходит аргументом по той же причине: у эмуляции кодировка не ось,
    и ее путь передает сюда СВОЕ эффективное значение, не трогая глобал -
    мутация глобала из чужого модуля молча разъезжается с читателями.

    path нужен колонке drain_class (F4): физика слива у настоящего
    PostgreSQL и у эмуляции разная, и класс объявляется по пути, а не по
    ветке кода.
    """
    effective = FMT if fmt is None else fmt
    if effective not in PG_FMTS:
        if effective in PG_COPY_FMTS:
            raise SystemExit(
                f"BENCH_FMT={effective!r} - это путь pg_copy: у COPY другое "
                "кадрирование, и по контракту это отдельный путь, а не ветка")
        raise SystemExit(
            f"BENCH_FMT={effective!r} не из набора pg-wire: {PG_FMTS}")

    if MODE == "materialize" and TARGET == "polars":
        # ось целевой структуры: соединение отдается polars, и курсор он
        # вычитывает сам. Кортежи по дороге все равно возникают - в этом и
        # суть пары с pl.from_arrow на Arrow-путях
        if effective in ("", "simple_text"):
            _assert_polars_wire("psycopg", effective)
            return _polars_read_database(conn, query, path="psycopg")
        # D52: на extended text / binary курсор открываем сами - polars
        # собирает фрейм из готовых кортежей. Иначе клетка polars на
        # двоичной кодировке была бы невозможна вовсе: pl.read_database
        # ни prepare=True, ни binary=True не передашь
        cur = conn.cursor()
        cur.execute(query, **_pg_execute_kwargs(effective))
        rows = cur.fetchall()
        cols = [d.name for d in cur.description] if cur.description else None
        return _rows_result(rows, cols, path="psycopg")

    if MODE == "materialize" and TARGET == "raw":
        return _pg_raw_consume(conn, query, effective, path)

    if MODE == "drain":
        # HC7: BENCH_DRAIN_FORM=buffered снимает серверный курсор и сливает
        # обычным курсором клиента (fetchmany и выброс). Формула Д10 на
        # курсорном сливе оказалась недействительной именно потому, что
        # серверный курсор меряет не выдачу, а кадрирование FETCH: libpq
        # отдает сообщение на строку, и цикл крутится по числу строк
        if streaming and DRAIN_FORM != "buffered":
            return _pg_drain_stream(conn, t0, query, BATCH, fmt=effective,
                                    path=path)
        cur = conn.cursor()
        cur.execute(query, **_pg_execute_kwargs(effective))
        return _pg_drain_buffered(cur, t0, path=path)

    cur = conn.cursor()
    cur.execute(query, **_pg_execute_kwargs(effective))
    rows = cur.fetchall()
    cols = [d.name for d in cur.description] if cur.description else None
    return _rows_result(rows, cols, path="psycopg")


def _pg_raw_consume(conn, query, fmt: str, path: str = "pg_psycopg"):
    """Поток без разбора у pg-wire (D52): execute без fetch.

    libpq разобрал кадры DataRow в PGresult, объектов Python еще нет,
    результат удерживается до остановки часов (holder), как кортежи у
    соседей. Это пол пути psycopg: разница с клеткой до кортежей - цена
    конвертации значений в объекты Python, разница с полом провода - работа
    сервера по сборке ответа плюс кадрирование. Байт "как есть" у pg-wire
    через libpq не бывает: интерфейса к сырому потоку у него нет, и это
    честная граница протокола, а не пропуск.

    Число строк - PQntuples через rowcount, сам буфер не разбирается.
    Вес структуры (retained_mb) у PGresult не считается: бухгалтерии размера
    у libpq нет, в client_dtype об этом сказано явно.
    """
    cur = conn.cursor()
    t1 = time.perf_counter()
    cur.execute(query, **_pg_execute_kwargs(fmt))
    rows = cur.rowcount
    if rows is None or rows < 0:
        rows = _expected_rows()
    rec = {"rows": rows, "extra_s": time.perf_counter() - t1,
           "client_dtype": "raw:libpq:rows_counted"}
    # курсор держит PGresult; освобождение уходит в cleanup_s через holder
    return _maybe_retained(rec, cur, "pgresult")


def _pg_keyset_consume(conn, t0, fmt: str):
    """П6: keyset-выгрузка чанками против сплошного SELECT (пара - обычная
    клетка той же таблицы). Один цикл в одном соединении: чанк LIMIT N,
    ключ последней строки - в WHERE следующего. Ключ составной (см.
    bench_sql.KEYSET_COLS): по неуникальному одиночному ключу строгое '>'
    молча теряло бы дубли. Требует у PostgreSQL составного индекса по этим
    колонкам - создается в блоке 0 подготовки данных.
    """
    rows_acc = [] if MODE == "materialize" else None
    if MODE not in ("drain", "materialize") or \
            (MODE == "materialize" and TARGET != "tuples"):
        raise SystemExit(
            f"BENCH_QUERY_FORM=keyset в режиме {MODE}/{TARGET}: форма П6 "
            "меряет выгрузку - законны drain и materialize/tuples")
    total = 0
    chunks = 0
    ttfb = None
    last = None
    cols = None
    binary = (fmt == "ext_binary")
    while True:
        with conn.cursor() as cur:
            if last is None:
                cur.execute(keyset_sql(True), binary=binary)
            else:
                cur.execute(keyset_sql(False), last, binary=binary)
            chunk = cur.fetchall()
            if cols is None and cur.description:
                cols = [d.name for d in cur.description]
        if ttfb is None and chunk:
            ttfb = time.perf_counter() - t0
        if not chunk:
            break
        total += len(chunk)
        chunks += 1
        last = tuple(chunk[-1][p] for p in KEYSET_POS)
        if rows_acc is not None:
            rows_acc.extend(chunk)
        if len(chunk) < KEYSET_CHUNK:
            break
    print(f"# keyset: {chunks} чанков по {KEYSET_CHUNK}", file=sys.stderr)
    if MODE == "drain":
        rec = _extras({"rows": total, "ttfb_s": ttfb,
                       "client_dtype": "drain:rows"},
                      drain_class=drain_class_of("pg_psycopg"),
                      # T11: токен кадра по имени формы, а не по имени
                      # ключевого слова SQL - limit= в корпусе читался бы как
                      # ось BENCH_ROWS
                      frame=f"keyset={KEYSET_CHUNK}")
    else:
        rec = _rows_result(rows_acc, cols, path="pg_psycopg")
        rec["ttfb_s"] = ttfb
    # параметры уводят psycopg в extended независимо от BENCH_FMT
    rec["proto_mode"] = "extended+keyset"
    return rec


def path_pg_psycopg():
    if MODE == "srvcost":
        return _srvcost("pg")
    if QUERY_FORM == "point":
        raise SystemExit(
            "BENCH_QUERY_FORM=point у пути pg_psycopg: серия точечных "
            "запросов - это механика BENCH_EXECS, клетка П4 живет на пути "
            "pg_execs")

    def consume(conn, t0):
        if QUERY_FORM == "keyset":
            return _pg_keyset_consume(conn, t0, _pg_fmt("pg_psycopg"))
        rec = _pg_consume(conn, t0, SQL_PG)
        rec.setdefault("proto_mode", _pg_proto_mode(FMT))
        return rec

    return _execs(_pg_connect, consume)


def path_pg_copy():
    """COPY TO STDOUT: тот же сервер, тот же сокет, но кадрирование другое -
    CopyData вместо DataRow. Кортежей не строим (ячейки COPY идут отдельной
    под-таблицей "кортежей не строим"), поэтому из всего materialize здесь
    законна только целевая структура raw - сырой поток, целиком удержанный в
    памяти клиента."""
    if MODE == "srvcost":
        return _srvcost("pg")
    if FMT not in PG_COPY_FMTS:
        raise SystemExit(
            f"BENCH_FMT={FMT!r}: путь pg_copy понимает {PG_COPY_FMTS}")
    fmt = {"copy_text": "text", "copy_binary": "binary",
           "copy_csv": "csv"}[FMT]

    if MODE in ("digest", "dfgate"):
        # D52: сверка значений ТЕМ ЖЕ путем. Поток COPY разбирает сам psycopg
        # (copy.rows() с объявленными типами колонок - загрузчики на C, те же,
        # что у DataRow), в окно замера это не входит. До этого среза путь
        # отвергал режим, и блок с клеткой COPY не стартовал бы вовсе - как
        # файловый маршрут в C6 (F105)
        return _execs(_pg_connect,
                      lambda conn, t0: _pg_copy_typed_rows(conn, fmt))

    if MODE == "materialize" and TARGET in ("df", "polars"):
        if FMT != "copy_csv":
            raise SystemExit(
                f"COPY {fmt} до {TARGET}: табличные парсеры не понимают "
                "экранирование COPY text, а двоичный формат в Python не "
                "разбирается - эта дорога в пакете есть как ADBC и "
                "connectorx (D52). До датафрейма законен только copy_csv")
        return _execs(_pg_connect,
                      lambda conn, t0: _pg_copy_csv_frame(conn))

    if MODE == "materialize" and TARGET != "raw":
        raise SystemExit(
            f"COPY в режиме {MODE}/{TARGET}: кортежей из сырого потока мы не "
            "строим - ячейки COPY живут на уровне доставки (drain), в "
            "целевой структуре raw и, у csv, в датафрейме парсером библиотеки")

    def consume(conn, t0):
        cur = conn.cursor()
        with cur.copy(f"COPY ({SQL_PG}) TO STDOUT (FORMAT {fmt})") as cp:
            if MODE == "drain":
                rec = _drain(cp, t0, bytes_of=len, klass="raw", path="pg_copy")
            else:
                t1 = time.perf_counter()
                blob = bytearray()
                for chunk in cp:
                    blob += chunk
                # F07/F44: буфер - удерживаемая структура, уходит в holder
                # через общую воронку; число строк - по потоку, где это
                # возможно без разбора (text), иначе из холста с пометкой
                rows, rows_src = _copy_rows(blob, fmt)
                rec = _raw_result(blob, t1, rows=rows, rows_src=rows_src)
        rec.setdefault("proto_mode", "copy")
        return rec

    return _execs(_pg_connect, consume)


def _pg_copy_column_types(conn):
    """OID типов и имена колонок выдачи: LIMIT 0 тем же текстом запроса.
    Нужны copy.set_types - без них copy.rows() отдает строки как есть."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM ({SQL_PG}) AS q LIMIT 0")
        return ([d.type_code for d in cur.description],
                [d.name for d in cur.description])


def _pg_copy_typed_rows(conn, fmt: str):
    """Разбор потока COPY в кортежи для сверки (режимы digest/dfgate).

    text и binary разбирает сам psycopg (copy.rows() с объявленными типами
    колонок - загрузчики на C). csv psycopg не разбирает вовсе: строки
    режет модуль csv, значения остаются строками, и канонизирует их кодек
    ПО ОБЪЯВЛЕННОЙ СХЕМЕ таблицы (срез D52 научил кодек читать текстовые
    моменты, даты и логические). NULL в csv PostgreSQL пишет пустым полем,
    пустая строка - в кавычках; модуль csv их не различает, поэтому сверка
    csv законна на таблицах без NULL (у cb_* все колонки NOT NULL).
    """
    types, names = _pg_copy_column_types(conn)
    cur = conn.cursor()
    if fmt == "csv":
        import csv as _csv                                     # noqa: PLC0415
        import io as _io                                       # noqa: PLC0415
        blob = bytearray()
        with cur.copy(f"COPY ({SQL_PG}) TO STDOUT (FORMAT csv)") as cp:
            for chunk in cp:
                blob += chunk
        text = _io.TextIOWrapper(_io.BytesIO(bytes(blob)), encoding="utf-8",
                                 newline="")
        rows = [tuple(r) for r in _csv.reader(text)]
    else:
        with cur.copy(f"COPY ({SQL_PG}) TO STDOUT (FORMAT {fmt})") as cp:
            cp.set_types(types)
            rows = list(cp.rows())
    rec = _rows_result(rows, names, path="pg_copy")
    rec.setdefault("proto_mode", "copy")
    return rec


def _pg_copy_csv_frame(conn):
    """COPY csv до датафрейма парсером библиотеки (D52).

    HEADER включен сознательно: так выгружает аналитик, и имена колонок
    едут в самом потоке, а не берутся из объявления рядом. Цена строки
    заголовка - десятки байт на весь ответ.
    """
    cur = conn.cursor()
    blob = bytearray()
    with cur.copy(f"COPY ({SQL_PG}) TO STDOUT (FORMAT csv, HEADER)") as cp:
        for chunk in cp:
            blob += chunk
    if TARGET == "polars":
        rec = _polars_from_csv(blob, header=True, path="pg_copy")
    else:
        rec = _df_from_csv(blob, header=True, path="pg_copy")
    rec.setdefault("proto_mode", "copy")
    return rec


# рамка COPY BINARY (docs: COPY, Binary Format): сигнатура 11 байт, флаги и
# длина расширения заголовка по 4 байта, в конце потока int16 -1
_COPY_BINARY_MAGIC = b"PGCOPY\n\xff\r\n\x00"
_COPY_BINARY_TRAILER = b"\xff\xff"


def _copy_rows(blob, fmt):
    """Число строк сырого потока COPY и источник числа (см. _raw_result).

    text: перевод строки внутри значения экранирован как \\n, счет точный.
    csv: перевод строки внутри кавычек посчитается - оценка сверху (lines).
    binary: строки различимы только полным разбором длин полей (10M строк
    на 105 колонок - сотни миллионов чтений в python, окну не по силам), число
    берется из холста; вместо разбора проверяется рамка потока - сигнатура и
    завершающий маркер, их отсутствие роняет клетку классом copy_framing.
    Проверка O(1) и идет внутри окна сознательно: пост-проба держала бы
    ссылку на буфер и увела бы его освобождение из cleanup_s в post_check_s."""
    if fmt == "text":
        return blob.count(b"\n"), "counted"
    if fmt == "csv":
        return blob.count(b"\n"), "lines"
    if not blob.startswith(_COPY_BINARY_MAGIC) or not blob.endswith(_COPY_BINARY_TRAILER):
        raise PathExit("copy_framing",
                       f"COPY BINARY: поток без сигнатуры или без завершающего "
                       f"маркера ({len(blob)} байт) - ответ неполон")
    return _expected_rows(), "expected"


def path_pg_server_cursor():
    """Ось размера порции у psycopg: та же одна ручка, что defaultRowFetchSize
    у pgJDBC и max_block_size у ClickHouse, только зовется itersize. Смысл оси
    в ttfb - где память, где немота до первой строки, где сладкая точка."""
    if MODE == "srvcost":
        return _srvcost("pg")
    fmt = _pg_fmt("pg_server_cursor")

    def consume(conn, t0):
        name = f"bench_{os.getpid()}_{next(_CURSOR_SEQ)}"
        with conn.cursor(name=name) as cur:
            cur.itersize = ITERSIZE
            cur.execute(SQL_PG, binary=(fmt == "ext_binary"))
            if MODE == "drain":
                rec = _extras(
                    _drain(cur, t0, rows_of=lambda row: 1, klass="rows",
                           path="pg_server_cursor"),
                    frame=f"itersize={ITERSIZE}")
            else:
                # fetchall у серверного курсора psycopg 3 - это один FETCH
                # FORWARD ALL: itersize в нем не участвует, а строка сырья
                # обещала frame=itersize=N (серия G: у a8-*-mat в окне
                # pg_stat_statements лежал ровно один FETCH FORWARD ALL).
                # Читаем порциями по ITERSIZE - тем же шагом и с тем же
                # условием остановки, что __iter__ курсора в drain, - и копим
                # в список: материализация остается, кадр становится тем, что
                # написано в метке. Список и есть живой объект результата
                # (holder в _rows_result), как раньше
                rows = []
                while True:
                    chunk = cur.fetchmany(ITERSIZE)
                    rows.extend(chunk)
                    if len(chunk) < ITERSIZE:
                        break
                cols = [d.name for d in cur.description] \
                    if cur.description else None
                rec = _extras(
                    _rows_result(rows, cols, path="pg_server_cursor"),
                    frame=f"itersize={ITERSIZE}")
        # кадрирование серверного курсора всегда DECLARE / FETCH - в колонку
        # идет кодировка значений, кадрирование зафиксировано в CONTRACT
        rec.setdefault("proto_mode", _pg_proto_mode(fmt))
        return rec

    return _execs(_pg_connect, consume)


def path_pg_execs():
    """Ось самоускорения: одно соединение, BENCH_EXECS исполнений подряд.

    prepare=True здесь НЕ ставится сознательно: ось меряет момент, когда
    драйвер сам решает подготовить запрос, и порог решает BENCH_PREPARE_THRESHOLD
    (none - автоподготовка выключена совсем, 0 - с первого раза). Явный
    prepare=True снял бы ровно тот эффект, ради которого ось заведена: до
    порога psycopg говорит простым протоколом, после - переходит на extended
    с подготовленным запросом, и это видно прямо на проводе.
    """
    if MODE == "srvcost":
        return _srvcost("pg")
    fmt = _pg_fmt("pg_execs")
    if fmt == "ext_text":
        raise SystemExit(
            "BENCH_FMT=ext_text на оси самоускорения означал бы prepare=True "
            "на каждом исполнении - порог тогда не срабатывает никогда. "
            "Берите simple_text или ext_binary")
    threshold = "none" if PREPARE_THRESHOLD is None else str(PREPARE_THRESHOLD)

    if QUERY_FORM == "point":
        # П4: BENCH_EXECS точечных запросов по случайным ключам в одном
        # соединении. Текст один (плейсхолдер %s) - автоподготовка psycopg
        # работает как в жизни: "simple" клетка = threshold none
        # (неподготовленный extended: с параметрами простой протокол
        # недостижим в принципе), "prepared" = threshold 0. Ключи снимаются
        # в connect-фазе, ВНЕ окон исполнений - connect_s этой клетки
        # включает выборку ключей (пометка в паспорт клетки). Требует
        # индекса PostgreSQL по ключу - создается в блоке 0.
        if MODE != "materialize":
            raise SystemExit(
                f"BENCH_MODE={MODE} на точечной клетке П4 пути pg_execs: "
                "точечная форма снимается только в materialize - клетка с "
                "чужим режимом уехала бы в CSV под чужой меткой mat_level")
        state = {}

        def connect_point():
            conn = _pg_connect()
            conn.prepare_threshold = PREPARE_THRESHOLD
            with conn.cursor() as cur:
                cur.execute(setup_sql(point_keys_sql("pg", max(EXECS, 1))))
                state["keys"] = shuffle_keys(r[0] for r in cur.fetchall())
            state["i"] = 0
            return conn

        def consume_point(conn, t0):
            i = state["i"]
            state["i"] += 1
            key = state["keys"][i % len(state["keys"])]
            with conn.cursor() as cur:
                cur.execute(point_sql("pg"), (key,),
                            binary=(fmt == "ext_binary"))
                rows = cur.fetchall()
                cols = [d.name for d in cur.description] \
                    if cur.description else None
            rec = _rows_result(rows, cols, path="pg_execs")
            rec.setdefault("proto_mode", f"extended+threshold={threshold}")
            return rec

        return _execs(connect_point, consume_point)

    def connect():
        conn = _pg_connect()
        conn.prepare_threshold = PREPARE_THRESHOLD
        return conn

    def consume(conn, t0):
        cur = conn.cursor()
        cur.execute(SQL_PG, binary=(fmt == "ext_binary"))
        if MODE == "drain":
            # серверный курсор тут запрещен: DECLARE - другой текст запроса,
            # автоподготовка на нем не срабатывает, и ось мерила бы пустоту
            rec = _pg_drain_buffered(cur, t0, path="pg_execs")
        else:
            rows = cur.fetchall()
            cols = [d.name for d in cur.description] \
                if cur.description else None
            rec = _rows_result(rows, cols, path="pg_execs")
        rec.setdefault("proto_mode",
                       f"{_pg_proto_mode(fmt)}+threshold={threshold}")
        return rec

    return _execs(connect, consume)


# F24 (PATH-04): adbc-driver-postgresql довозит NUMERIC ТЕКСТОМ внутри Arrow
# (extension arrow.opaque[storage_type=string; type_name=numeric]), и клетка
# d10-g1decimal-adbc под меткой mat-typed называла ценой Decimal цену
# numeric-как-текста. Имя опции драйвера, которая это меняет, на пине 1.4.0
# не проверено, а неизвестный ключ ADBC отдает ошибкой и уронил бы всю полосу.
# Поэтому опция - ЯВНАЯ ручка (BENCH_ADBC_NUMERIC_AS, по умолчанию пусто =
# как в серии F): сначала проба на стенде, потом клетки. Если драйвер ключ не
# принял, клетка живет дальше с честной строкой типов в client_dtype, а
# уровень материализации решает МЕТКА (mat-untyped), а не наша надежда.
#
# R2-05: значение приходит из bench_axes (ADBC_NUMERIC_AS) - там ось заведена
# с проверкой по реестру допустимых значений. Свое чтение окружения здесь
# было вторым дефолтом одной оси: сегодня оба пустые, но с первой же правкой
# одного из них кривое значение не упало бы на импорте, а тихо уехало в замер.


def _adbc_set_options(cur) -> None:
    """Опции драйвера на курсор ADBC (use_copy - ось, numeric_as - проба)."""
    # опция передается СТРОКОЙ: константы StatementOptions.USE_COPY
    # в текущих версиях adbc-driver-postgresql нет
    cur.adbc_statement.set_options(**{"adbc.postgresql.use_copy": ADBC_COPY})
    if not ADBC_NUMERIC_AS:
        return
    try:
        cur.adbc_statement.set_options(
            **{"adbc.postgresql.numeric_as": ADBC_NUMERIC_AS})
    except Exception as exc:  # noqa: BLE001 - имя ключа зависит от версии
        print(f"# pg_adbc: драйвер не принял adbc.postgresql.numeric_as="
              f"{ADBC_NUMERIC_AS!r} ({type(exc).__name__}: {exc}) - клетка "
              "снята БЕЗ опции, numeric приедет строкой; уровень "
              "материализации такой клетки - mat-untyped", file=sys.stderr)


def path_pg_adbc():
    """ADBC поверх того же pg-wire. Ручка BENCH_ADBC_COPY переключает драйвер
    между COPY BINARY и обычным extended-протоколом - это перекрестная проверка
    оси кодировки у другого клиента."""
    if MODE == "srvcost":
        return _srvcost("pg")
    _assert_pg_codec()

    # F8 (DATA-01, DATA-02, PATH-01, PGSESS-01): ADBC открывает соединение
    # сам, мимо _pg_connect, то есть до среза v1.5 шел БЕЗ контракта сессии -
    # 61 клетка снята при jit=on и parallel=2, а на холсте hits с LIMIT без
    # synchronize_seqscans=off читала плавающий миллион строк (17.7% разброса
    # по байтам против 0.3% у psycopg).
    #
    # F02 (срез stand-g-v1.0): одних стартовых ключей libpq мало - через
    # remote-пулер на 6432 они не доезжают, и это видно было только по
    # разбросу байтов. Теперь соединение открывает общая фабрика
    # _session.pg_connect: те же options в URI ПЛЮС SET каждого ключа после
    # connect (ADBC умеет исполнять SQL) ПЛЮС обратное чтение с сервера в той
    # же фазе подключения. Расхождение роняет клетку классом session:<ключ> -
    # молчаливого замера чужой сессии больше нет.
    proto = "copy" if ADBC_COPY == "true" else "extended"

    def connect_adbc():
        # verify=True: контракт читается обратно с сервера в фазе подключения,
        # расхождение роняет клетку классом session:<ключ>
        return _session.pg_connect("adbc", canvas=CANVAS, verify=True)

    if QUERY_FORM == "point":
        # П4 через ADBC: ключ вшивается литералом (параметры ADBC передаются
        # несопоставимо с psycopg, а int-литерал одинаков и безопасен) -
        # текст меняется на каждом исполнении, server-side prepared не
        # возникает. Выборка ключей - в connect-фазе, вне окон исполнений.
        state = {}

        def connect_point():
            conn = connect_adbc()
            with conn.cursor() as cur:
                cur.execute(setup_sql(point_keys_sql("pg", max(EXECS, 1))))
                keys = cur.fetch_arrow_table().column(0).to_pylist()
            state["keys"] = shuffle_keys(keys)
            state["i"] = 0
            return conn

        def consume_point(conn, t0):
            i = state["i"]
            state["i"] += 1
            key = state["keys"][i % len(state["keys"])]
            with conn.cursor() as cur:
                _adbc_set_options(cur)
                cur.execute(point_sql("pg", key))
                rec = _arrow_result(cur.fetch_arrow_table(), path="pg_adbc")
            rec.setdefault("proto_mode", proto)
            return rec

        return _execs(connect_point, consume_point)

    def consume(conn, t0):
        with conn.cursor() as cur:
            _adbc_set_options(cur)
            cur.execute(SQL_PG)
            if MODE == "drain":
                reader = cur.fetch_record_batch()
                rec = _drain(reader, t0, rows_of=lambda b: b.num_rows,
                             klass="arrow", path="pg_adbc")
            else:
                rec = _arrow_result(cur.fetch_arrow_table(), path="pg_adbc")
        rec.setdefault("proto_mode", proto)
        return rec

    return _execs(connect_adbc, consume)


# =========================================================================
# Arrow Flight SQL поверх PostgreSQL (расширение arrow_flight_sql)
# =========================================================================
# Пара к ch_flightsql: клиент ТОТ ЖЕ (adbc_driver_flightsql), меняется только
# то, кто на том конце провода - у ClickHouse Flight встроен в сервер, у
# PostgreSQL это стороннее расширение apache/arrow-flight-sql-postgresql,
# которое поднимает gRPC отдельным фоновым воркером и на каждое соединение
# запускает второй воркер-исполнитель.
#
# Все три ограничения ниже проверены локально 2026-09-08 на PostgreSQL 18.6 +
# arrow_flight_sql 0.2.0-dev (commit 6b9b8a7, сборка - standf/pgflight):
#
#  1. НЕПОДДЕРЖАННЫЙ ТИП РОНЯЕТ ВЕСЬ FLIGHT-ПОРТ. Клиент получает внятное
#     "NotImplemented: Unsupported PostgreSQL type: <oid>", но одновременно
#     падает фоновый воркер "arrow-flight-sql: server", и до перезапуска
#     PostgreSQL Flight не отвечает НИКОМУ (все следующие клетки полосы -
#     "error reading server preface: EOF"). Поэтому схема запроса снимается
#     ДО замера дешевым проводом psycopg, и клетка с чужим типом падает
#     классом types:<колонка>:<тип>, НЕ добравшись до Flight. Это "клетка
#     невозможна", а не крах стенда.
#  2. SET через Flight КРАШИТ ИСПОЛНИТЕЛЯ (FATAL CRASHED!!! с трассой внутри
#     arrow_flight_sql.so) - утилитные операторы адаптер не исполняет. Поэтому
#     контракт сессии (F02) доставляется единственной формой, которая
#     проходит: SELECT set_config(...) - обычный запрос с текстовым
#     результатом. Читается контракт обратно ТЕМ ЖЕ current_setting, что у
#     остальных PG-клиентов (_session), расхождение роняет клетку классом
#     session:<ключ>.
#  3. ОДНО ГИГАНТСКОЕ СООБЩЕНИЕ. Адаптер отдает до
#     arrow_flight_sql.max_n_rows_per_record_batch строк (по умолчанию
#     1048576) ОДНИМ record batch, то есть весь миллион строк g10f (72 МБ)
#     едет одним gRPC-сообщением. Дефолтный потолок сообщения у клиента -
#     16 МиБ, поэтому with_max_msg_size обязателен, иначе клетка падает на
#     ровном месте. Фактический потолок пишется в колонку frame, а форма
#     кадра - в proto_mode (flight_single_batch против flight_batched).
#
# Плюс операционное: каждое Flight-соединение держит воркер-исполнитель до
# arrow_flight_sql.session_timeout (по умолчанию 300 с) ДАЖЕ ПОСЛЕ close, а
# крашнувшийся исполнитель оставляет сегмент в /dev/shm. Значит серверу нужен
# большой max_worker_processes и большой --shm-size (см. bootstrap-standf.sh).

# Порт gRPC адаптера по умолчанию (arrow_flight_sql.uri в контейнере).
_PGFLIGHT_PORT = 15432

# Типы результата, которые адаптер умеет положить в Arrow. Список ПРОВЕРЕН
# замером, а не вычитан из README: каждый тип снят отдельной пробой, каждый
# отказ - отдельным перезапуском сервера (иначе первая же неудача уносит
# порт). Ключ - OID PostgreSQL, значение - имя типа для колонки причины.
PGFLIGHT_OK_TYPES = {
    17: "bytea",
    20: "int8",
    21: "int2",
    23: "int4",
    25: "text",
    700: "float4",
    701: "float8",
    1043: "varchar",
    1114: "timestamp",       # БЕЗ зоны; timestamptz (1184) роняет сервер
}


def _pg_type_name(oid: int) -> str:
    """Имя типа PostgreSQL по OID из статического реестра psycopg.

    Реестр статический - к базе за именем ходить не нужно, а значит функция
    годится и в тесте без базы. Неизвестный OID печатается как oid<N>:
    молчать про тип, из-за которого клетка не состоялась, нельзя.
    """
    try:
        info = psycopg.postgres.types.get(oid)
    except Exception:            # noqa: BLE001 - реестр версионный
        info = None
    return info.name if info is not None else f"oid{oid}"


def pgflight_schema_problems(columns):
    """Колонки, которые Flight-адаптер PostgreSQL отдать не сможет.

    columns - последовательность пар (имя, тип), где тип это OID (int или
    его десятичная строка) либо уже имя типа. Возврат - список пар
    (имя колонки, имя типа) в порядке колонок; пустой список = схема
    совместима.

    Функция ЧИСТАЯ и вынесена наружу сознательно: на ней стоит тест без базы,
    и она же единственный источник правды о наборе типов - список в
    комментарии рядом с путем однажды разошелся бы с проверкой.
    """
    names = set(PGFLIGHT_OK_TYPES.values())
    bad = []
    for name, type_ in columns:
        oid = None
        if isinstance(type_, bool):
            oid = None
        elif isinstance(type_, int):
            oid = type_
        elif isinstance(type_, str) and type_.strip().isdigit():
            oid = int(type_)
        if oid is not None:
            if oid not in PGFLIGHT_OK_TYPES:
                bad.append((str(name), _pg_type_name(oid)))
            continue
        label = str(type_)
        if label not in names:
            bad.append((str(name), label))
    return bad


def _flight_assert_types(query: str) -> None:
    """Гейт типов ДО замера: схему запроса снимает psycopg, не Flight.

    Форма проверки - обертка LIMIT 0 вокруг замерного текста: сервер строит
    план и отдает описание колонок, строк не читает вовсе. Через Flight то же
    самое сделать нельзя по причине 1 в шапке секции - первая же чужая
    колонка унесла бы порт для всех следующих клеток полосы.
    """
    conn = _session.pg_connect("psycopg", canvas=CANVAS, verify=True)
    try:
        with conn.cursor() as cur:
            cur.execute(setup_sql(
                f"SELECT * FROM (\n{query}\n) _pgflight_probe LIMIT 0"))
            columns = [(d.name, d.type_code) for d in (cur.description or ())]
    finally:
        conn.close()
    if not columns:
        raise PathExit(
            "types:none",
            "запрос клетки не отдал ни одной колонки - проверять на "
            "совместимость с Flight-адаптером нечего")
    bad = pgflight_schema_problems(columns)
    if not bad:
        return
    name, type_name = bad[0]
    raise PathExit(
        f"types:{name}:{type_name}",
        "путь pg_flightsql не снимает эту клетку: Flight-адаптер PostgreSQL "
        f"не умеет тип {type_name} колонки {name} "
        f"(всего несовместимых колонок {len(bad)}: "
        + ", ".join(f"{n}:{t}" for n, t in bad) + "). Запускать такой запрос "
        "через Flight НЕЛЬЗЯ: адаптер отвечает NotImplemented и тут же роняет "
        "свой фоновый воркер - порт умирает до перезапуска PostgreSQL. Это "
        "невозможная клетка, а не сбой замера: см. профиль g10f и проекцию "
        "w10")


def _flight_uri() -> str:
    """Адрес gRPC адаптера: явный PG_FLIGHT_URI либо хост PG и порт 15432."""
    uri = os.environ.get("PG_FLIGHT_URI", "").strip()
    return uri or f"grpc://{PG['host']}:{_PGFLIGHT_PORT}"


def _flight_db_kwargs() -> dict:
    """Ключи соединения: логин/пароль те же, что у psycopg, база - заголовком.

    Заголовок x-flight-sql-database и пара username/password вместе законны,
    а вот заголовок Authorization с ними конфликтует - адаптер отвечает
    Unauthenticated. Поэтому здесь именно username/password, в отличие от
    ch_flightsql, где рукопожатие basic не работает и остается заголовок.
    """
    if FlightOptions is None:
        raise SystemExit(
            "adbc-driver-flightsql не установлен - путь pg_flightsql пропущен")
    prefix = FlightOptions.RPC_CALL_HEADER_PREFIX.value
    db = os.environ.get("PG_FLIGHT_DB", "").strip() or PG["db"]
    kwargs = {
        "username": PG["user"],
        "password": _session.pg_password(),
        f"{prefix}x-flight-sql-database": db,
    }
    if hasattr(FlightOptions, "WITH_MAX_MSG_SIZE"):
        # причина 3 в шапке секции: без поднятого потолка миллион строк не
        # приезжает вовсе - клиент отказывает на 16 МиБ
        kwargs[FlightOptions.WITH_MAX_MSG_SIZE.value] = str(2 ** 31 - 1)
    else:
        print("# pg_flightsql: у пакета adbc-driver-flightsql нет опции "
              "with_max_msg_size - результат больше 16 МиБ не приедет",
              file=sys.stderr)
    return kwargs


def _flight_set_config_sql() -> str:
    """Контракт сессии одним запросом: SET адаптер не исполняет (причина 2).

    Имена и значения берутся из ЕДИНСТВЕННОГО источника
    _srvcontrol.PG_SESSION_SETTINGS - второго списка настроек в проекте быть
    не должно (F8). Литерал безопасен: значения осей уже проверены при
    импорте _srvcontrol (jit - on/off, параллелизм - число), но кавычка в
    значении все равно роняет клетку, а не уезжает в текст запроса.
    """
    parts = []
    for name, value in _srvcontrol.PG_SESSION_SETTINGS:
        if "'" in str(name) or "'" in str(value):
            raise SystemExit(
                f"контракт сессии: значение {name}={value!r} с кавычкой - "
                "в текст set_config такое не подставляем")
        parts.append(f"set_config('{name}', '{value}', false) "
                     f"AS s_{str(name).lower()}")
    return setup_sql("SELECT " + ", ".join(parts))


def _flight_batch_cap(conn) -> int:
    """Потолок строк в одном record batch - для колонок frame и proto_mode.

    Читается с сервера, а не берется из умолчания: это ручка адаптера
    (arrow_flight_sql.max_n_rows_per_record_batch), и подпись клетки обязана
    называть ФАКТ. Второй аргумент current_setting - missing_ok: на сервере
    без расширения запрос вернет NULL, а не уронит фазу подключения.
    """
    with conn.cursor() as cur:
        cur.execute(setup_sql(
            "SELECT current_setting("
            "'arrow_flight_sql.max_n_rows_per_record_batch', true) AS cap"))
        # именно fetch_arrow_table, а не fetchone: частично прочитанный поток
        # драйвер закрывает отменой, и адаптер пишет в лог "stream ended with
        # error: context canceled" - шум ровно на служебном запросе
        values = cur.fetch_arrow_table().column(0).to_pylist()
    try:
        return int(values[0])
    except (IndexError, TypeError, ValueError):
        return 0


def _flight_proto_mode(rows, cap: int) -> str:
    """Форма кадра ответа объявленным значением (колонка proto_mode).

    flight_single_batch - весь результат приехал ОДНИМ record batch (так
    выглядят все клетки сетки до миллиона строк включительно);
    flight_batched - строк больше потолка, батчей несколько;
    flight - потолок неизвестен (сервер без расширения), гадать не будем.
    """
    if not cap:
        return "flight"
    if rows is None:
        return "flight"
    return "flight_single_batch" if rows <= cap else "flight_batched"


def path_pg_flightsql():
    """Arrow Flight SQL поверх PostgreSQL: сквозной Arrow там, где родной
    провод его не знает.

    Пара к pg_adbc на том же сервере и к ch_flightsql на том же клиенте: у
    PostgreSQL Arrow появляется только в расширении, и цена этого перехода -
    предмет клетки. Ограничения адаптера (типы, одно сообщение, падение
    воркера на SET и на чужом типе) разобраны в шапке секции.
    """
    if MODE == "srvcost":
        return _srvcost("pg")
    if flightsql is None:
        raise SystemExit(
            "путь pg_flightsql требует пакет adbc-driver-flightsql, а он не "
            "установлен - это пустая клетка окружения, а не сбой замера "
            "(pip install adbc-driver-flightsql)")
    # сжатия здесь нет по той же причине, что у ch_flightsql: серверных ручек
    # формата во Flight-фронтенд через ADBC не прокинуть, а gRPC-компрессию
    # драйвер не открывает. Клетка с чужим кодеком мерила бы не то, что
    # обещает метка
    _assert_pg_codec()

    state = {}

    def connect_flight():
        conn = flightsql.connect(_flight_uri(), db_kwargs=_flight_db_kwargs())
        try:
            # контракт сессии - причина 2 в шапке: SET через Flight роняет
            # исполнителя, остается set_config обычным запросом
            with conn.cursor() as cur:
                try:
                    cur.execute(_flight_set_config_sql())
                    cur.fetch_arrow_table()
                except PathExit:
                    raise
                except Exception as exc:   # noqa: BLE001 - драйверных классов много
                    raise PathExit(
                        "session:set_config",
                        "контракт сессии PostgreSQL не доставлен через "
                        f"Flight ({type(exc).__name__}: {exc}). Молча мерить "
                        "чужую сессию нельзя - клетка снята")
            # обратное чтение с сервера ТЕМ ЖЕ current_setting, что у
            # остальных PG-клиентов; расхождение роняет клетку session:<ключ>
            _session.assert_pg_session("flightsql", conn)
            state["cap"] = _flight_batch_cap(conn)
        except BaseException:
            conn.close()
            raise
        return conn

    def _finish(rec):
        rec.setdefault("proto_mode",
                       _flight_proto_mode(rec.get("rows"), state.get("cap", 0)))
        cap = state.get("cap", 0)
        if cap:
            # T11: токен кадра свой, потому что и кадр свой - не itersize и не
            # max_block_size, а потолок строк в record batch у адаптера
            _extras(rec, frame=f"flight_batch={cap}")
        return rec

    if QUERY_FORM == "point":
        # П4 по Flight: ключ вшивается литералом (параметры у flight-курсора
        # передаются несопоставимо с psycopg), выборка ключей - в connect-фазе,
        # вне окон исполнений. Проверка типов - по тексту точечного запроса:
        # проекция у него та же, что у сплошного, но проверять надо ровно то,
        # что поедет
        if MODE != "materialize":
            raise SystemExit(
                f"BENCH_MODE={MODE} на точечной клетке П4 пути pg_flightsql: "
                "точечная форма снимается только в materialize - клетка с "
                "чужим режимом уехала бы в CSV под чужой меткой mat_level")
        _flight_assert_types(point_sql("pg", 0))

        def connect_point():
            conn = connect_flight()
            with conn.cursor() as cur:
                cur.execute(setup_sql(point_keys_sql("pg", max(EXECS, 1))))
                keys = cur.fetch_arrow_table().column(0).to_pylist()
            state["keys"] = shuffle_keys(keys)
            state["i"] = 0
            return conn

        def consume_point(conn, t0):
            i = state["i"]
            state["i"] += 1
            key = state["keys"][i % len(state["keys"])]
            with conn.cursor() as cur:
                cur.execute(point_sql("pg", key))
                rec = _arrow_result(cur.fetch_arrow_table(),
                                    path="pg_flightsql")
            return _finish(rec)

        return _execs(connect_point, consume_point)

    _flight_assert_types(SQL_PG)

    def consume(conn, t0):
        with conn.cursor() as cur:
            cur.execute(SQL_PG)
            if MODE == "drain":
                reader = cur.fetch_record_batch()
                rec = _drain(reader, t0, rows_of=lambda b: b.num_rows,
                             klass="arrow", path="pg_flightsql")
            else:
                rec = _arrow_result(cur.fetch_arrow_table(),
                                    path="pg_flightsql")
        return _finish(rec)

    return _execs(connect_flight, consume)


def path_cx_pg():
    """connectorx: три провода под одним API (BENCH_CX_PROTOCOL = binary, csv,
    cursor). Соединение прячется внутри библиотеки, поэтому ось самоускорения и
    честный поток тут недоступны по построению: connectorx собирает результат
    целиком и отдает готовую структуру - в drain клетка помечается
    drain:buffered."""
    if cx is None:
        raise SystemExit(
            "путь cx_pg требует пакет connectorx, а он не установлен - "
            "это пустая клетка окружения, а не сбой замера "
            "(pip install connectorx)")
    if MODE == "srvcost":
        return _srvcost("pg")
    _assert_pg_codec()

    # F8: connectorx строит соединение сам (rust-postgres), контракт сессии
    # ему тоже уезжает стартовыми ключами. sslmode=disable добавлен заодно:
    # без него cx единственный из путей ехал на sslmode=prefer и тратил лишний
    # SSLRequest. Набор ключей, который принимает rust-postgres, свой -
    # поэтому клетка a12-cx_pg-binary-arrow снимается смоуком до сетки, а
    # факт применения читается гейтом приемки (SHOW четырех настроек).
    # F02: у connectorx SET нет по построению - остаются options, и это
    # единственный клиент, которому на remote-контуре разрешено отклонение
    # (allowlist гейта сессии). Клетка подписывается session=options, чтобы
    # способ доставки контракта был виден в самой строке сырья, а не выводился
    # из чужого журнала
    uri = _session.pg_connect("cx", canvas=CANVAS)
    proto = f"cx:{CX_PROTOCOL}"
    # гейт датафрейма обязан сравнивать один и тот же класс структуры на всех
    # путях, поэтому в dfgate connectorx всегда просят pandas
    if MODE == "dfgate" or (MODE == "materialize" and TARGET == "df"):
        return_type = "pandas"
    elif MODE == "materialize" and TARGET == "polars":
        return_type = "polars"
    else:
        return_type = "arrow"

    t0 = time.perf_counter()
    data = cx.read_sql(uri, SQL_PG, protocol=CX_PROTOCOL,
                       return_type=return_type)
    build_s = time.perf_counter() - t0

    def _mark_session(rec: dict) -> dict:
        """F02: способ доставки контракта сессии - в самой строке клетки.

        Пометка идет в proto_mode, а НЕ в client_dtype. client_dtype -
        сверяемая колонка: гейт 4 требует, чтобы строка совпала у всех путей
        блока аналитика (lib.sh: distinct_cols по dfgate:cols=N), а таблицы
        читают из нее класс слива (standf_tables: dtype.split(":", 1)[1] у
        drain:<класс>). Хвост в ней ронял бы гейт 4 на клетке cx_pg и уводил
        клетку слива в собственный класс. proto_mode - колонка-аннотация
        того же пути (там уже стоят extended+keyset, copy, cx:binary), ее
        никто не сверяет на равенство.
        """
        note = _session.session_note("cx")
        base = str(rec.get("proto_mode") or "")
        rec["proto_mode"] = f"{base}+{note}" if base else note
        return rec

    if MODE == "drain":
        # честно: библиотека уже все собрала - потока тут нет ни на секунду,
        # первая строка приезжает вместе с последней
        return _single(_extras(_mark_session({"rows": data.num_rows,
                                              "ttfb_s": build_s,
                                              "proto_mode": proto,
                                              "client_dtype": "drain:buffered"}),
                               drain_class=drain_class_of("cx_pg")))
    if return_type == "arrow":
        rec = _arrow_result(data, path="cx_pg")
    elif MODE == "dfgate":
        rec = _dfgate(data, build_s)
    else:
        # пересъемка Д6 и тут: connectorx отдает уже готовый pandas- или
        # polars-фрейм, retained берется по его собственной версии
        rec = _maybe_retained(
            {
                "rows": len(data),
                "extra_s": build_s,
                "client_dtype": ";".join(str(t) for t in data.dtypes),
            },
            data, "polars" if return_type == "polars" else "df")
    rec.setdefault("proto_mode", proto)
    return _single(_mark_session(rec))


PATHS_PG = {
    "pg_psycopg": path_pg_psycopg,
    "pg_copy": path_pg_copy,
    "pg_adbc": path_pg_adbc,
    "pg_flightsql": path_pg_flightsql,
    "pg_server_cursor": path_pg_server_cursor,
    "pg_execs": path_pg_execs,
    "cx_pg": path_cx_pg,
}
