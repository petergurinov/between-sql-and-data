"""Обвязка исполнения путей: drain / execs, целевые структуры и гейты,
подключения и сжатие, режим srvcost, канонический digest.

stdout занят CSV - вся диагностика этого модуля идет в stderr строками
с решеткой.
"""

import json
import os
import random
import re
import resource
import socket
import sys
import time
from decimal import Decimal
from pathlib import Path
import datetime as _dt

import _codec

import clickhouse_connect
import pandas as pd
import psycopg
import pyarrow as pa
from clickhouse_driver import Client as CHClient

# Транспортное сжатие HTTP распаковываем сами (см. _decode_wire): urllib3 про
# lz4 не знает вовсе, а zstd в urllib3 2.x снимает молча - два разных
# поведения на одной оси дали бы клетки, которые меряют разное. Оба пакета -
# обязательные спутники стека CH (lz4 у clickhouse-driver, zstandard у
# clickhouse-connect), отдельной зависимости серии тут не появляется.
import lz4.frame
import zstandard

# Срез финального прогона (D52): polars читает POLARS_MAX_THREADS при
# импорте. Клетка polars идет в ОДИН поток, если оператор не задал иное: из
# Arrow и из кортежей фрейм и так собирается в один поток, а csv и ndjson
# polars читает всеми ядрами - против pandas в один поток это мерило бы
# параллельный парсер, а не формат. Фактическое число потоков уезжает в
# строку сырья (proto_mode, polars_threads=N) - см. _finish_polars.
if (os.environ.get("BENCH_TARGET_STRUCT", "").strip() == "polars"
        and not os.environ.get("POLARS_MAX_THREADS", "").strip()):
    os.environ["POLARS_MAX_THREADS"] = "1"

try:  # polars - одна целевая структура, а не зависимость серии
    import polars as pl
except ImportError:
    pl = None

# Счетчики и часы берем у обвязки, а не пишем свои: у _net_counters уже есть
# выбор интерфейса по BENCH_RX_IFACE, у peak - clear_refs с фолбэком, и две
# разные реализации разъехались бы на первой же серии
from _measure import (EXTRA_COLS, _net_counters, _now_ms, extra_stage_of,
                      keep_alive,
                      peak_rss_mb, register_post_check, release_live,
                      reset_peak_rss, settled_rss_mb, steal_ticks)
import _measure
# F02: единая фабрика соединений PostgreSQL и обратное чтение контракта
# сессии - тот же модуль зовет гейт сессии раннера
import _session
from _srvcontrol import (_canon, _canon_decimal, ch_codec_plan,
                         ch_collect_log, ch_cost_from_log, ch_marker,
                         ch_query_settings, ch_server_cost, new_query_id,
                         pg_check_generator_plan, pg_prepare_session,
                         pg_server_cost, sha256_digest)

from bench_axes import (BATCH_SET, BATCH, CANVAS, CH, CH_FMTS, CODEC,
                        CODEC_LEVEL, CODEC_LEVEL_SET, DF_SCHEMA, DIGEST_SAMPLE,
                        EXECS, FMT, MAX_THREADS, MODE, PARALLEL_FMT, PG,
                        PG_COPY_FMTS, PG_FMTS, RESTORE_TYPES, RETAINED,
                        TARGET)
from _dfschema import DfSchemaError, normalize_df, normalize_polars
import bench_axes
# класс ошибки клетки: не привести датафрейм к объявленной схеме (E1)
from _measure import PathExit
from bench_sql import SQL_CH, SQL_PG, _expected_rows


# =========================================================================
# СЕКЦИЯ 0. Новые оси среза v1.5 и колонки паспорта клетки
# =========================================================================
# Оси среза v1.5 читаются через getattr: разбор и валидация значений живут в
# bench_axes (единый словарь имен), но пути обязаны работать и на дереве, где
# ось еще не зарегистрирована - тогда берется дефолт КОНТРАКТА, тот же самый.
# Дубля правды здесь нет: значение по-прежнему одно, просто у него есть
# запасной вход.
def _axis(name: str, default):
    return getattr(bench_axes, name, default)


# F11 (PATH-02): маршрут Arrow -> питоновские кортежи. pylist - как в серии F
# (словарь на строку), colwise - поколоночный zip без словарного слоя.
# Дефолт pylist держит мост: четыре клетки a1-fmt-*-mat корпуса F обязаны
# сходиться байт в байт.
ARROW_TO_ROWS = _axis("ARROW_TO_ROWS", "pylist")
# C1: усыпление клиента на каждый кусок слива, микросекунды. Ноль - как было.
CLIENT_DELAY_US = int(_axis("CLIENT_DELAY_US", 0))
# HC3: слой сжатия ClickHouse (transport | format), см. ch_codec_plan.
CH_CODEC_LAYER = _axis("CH_CODEC_LAYER", "transport")
# HC7: форма слива psycopg (cursor - серверный курсор, buffered - обычный
# курсор клиента с fetchmany и выбросом).
DRAIN_FORM = _axis("DRAIN_FORM", "cursor")

# Размер куска чтения HTTP-ответа: он же кадр слива ch_http, поэтому живет
# константой, а не литералом в сигнатуре - его значение уходит в колонку frame.
# Имя и значение общие с bench_axes.HTTP_BLOCK_BYTES (там оно печатается в
# колонку frame): берем оттуда, чтобы кусок чтения и его паспорт не разошлись.
HTTP_BLOCK_BYTES = _axis("HTTP_BLOCK_BYTES", 1 << 20)

# C7 (RS-02): приемный буфер сокета клиента, байты; 0 - как решит ядро.
# Ось объявлена контрактом, и пара k7-pgsock-default против k7-pgsock-1m
# отличается РОВНО ею. Пока значение никуда не доезжало, обе половины пары
# снимали одно и то же, то есть выдавали ложное "размер буфера ни при чем" по
# тому самому механизму, который стоит под слайдом про мелкие чтения pg-wire.
# Поэтому здесь не только применение (apply_sock_rcvbuf), но и учет ФАКТА
# применения: путь, который ось поставить не умеет, обязан упасть вслух -
# пустая клетка честнее клетки-обманки.
SOCK_RCVBUF = int(_axis("SOCK_RCVBUF", 0))
# фактические значения (getsockopt) по всем соединениям процесса: пусто
# означает "ось не применял никто"
_SOCK_RCVBUF_SET = []

_EXTRA_WARNED = set()


def _conn_fileno(conn):
    """Дескриптор сокета соединения psycopg.

    Берем два входа - fileno() самого соединения и pgconn.socket, - чтобы не
    зависеть от версии драйвера: у psycopg 3 есть оба, и оба указывают на
    один и тот же сокет libpq.
    """
    getter = getattr(conn, "fileno", None)
    if callable(getter):
        try:
            fd = getter()
        except Exception:   # соединение уже закрыто или это не сокет
            fd = None
        if isinstance(fd, int) and fd >= 0:
            return fd
    fd = getattr(getattr(conn, "pgconn", None), "socket", None)
    if isinstance(fd, int) and fd >= 0:
        return fd
    return None


def apply_sock_rcvbuf(conn, path: str = "") -> int:
    """Поставить SO_RCVBUF на сокет соединения и прочитать факт обратно (C7).

    Обратное чтение обязательно: ядро режет запрос потолком
    net.core.rmem_max, а на Linux хранит УДВОЕННОЕ значение (вторая половина
    уходит на служебные структуры сокета). Единственное число, о котором
    клетка вправе говорить, - то, что вернул getsockopt; без него метка
    k7-pgsock-1m обещала бы мегабайт, а мерился бы дефолт ядра.

    Оговорка, которую надо держать в голове при чтении цифр: буфер ставится
    ПОСЛЕ рукопожатия - своего сокета до соединения libpq не отдает. Масштаб
    окна TCP к этому моменту уже согласован по tcp_rmem, а ось меняет размер
    буфера и выключает автоподстройку. Ровно это контракт и обещает - "до
    первого чтения", а не "до соединения": данные ответа к моменту вызова
    еще не запрашивались.

    Вторая оговорка - на сколько буфер вообще способен укрупнить чтение:
    сколько байт заберет один read(), решает не только ядро, но и приемный
    буфер самой libpq. Поэтому клетка отвечает на практический вопрос
    "поможет ли ручка сокета", а не на теоретический "какой кадр возможен".
    """
    if SOCK_RCVBUF <= 0:
        return 0
    fd = _conn_fileno(conn)
    if fd is None:
        raise SystemExit(
            f"BENCH_SOCK_RCVBUF={SOCK_RCVBUF} у пути {path or '?'}: у "
            "соединения нет сокета - ось ставить некуда, а клетка "
            "отличалась бы от парной только меткой")
    # socket-объект закрывает свой дескриптор, а закрывать сокет живого
    # соединения нельзя: работаем на копии дескриптора, копию и закрываем
    dup = os.dup(fd)
    try:
        sock = socket.socket(fileno=dup)
    except OSError as exc:
        os.close(dup)
        raise SystemExit(
            f"BENCH_SOCK_RCVBUF={SOCK_RCVBUF} у пути {path or '?'}: "
            f"дескриптор {fd} не сокет ({exc})")
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, SOCK_RCVBUF)
        actual = sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
    except OSError as exc:
        raise SystemExit(
            f"BENCH_SOCK_RCVBUF={SOCK_RCVBUF} у пути {path or '?'}: "
            f"setsockopt(SO_RCVBUF) не прошел ({exc})")
    finally:
        sock.close()
    print(f"# sock_rcvbuf ({path or 'pg'}): запрошено {SOCK_RCVBUF} Б, "
          f"getsockopt вернул {actual} Б (на Linux это удвоенное значение - "
          "половина уходит на служебные структуры)", file=sys.stderr)
    # На Linux без потолка ядро отдает ровно 2x; меньше - значит запрос
    # срезан net.core.rmem_max, и об этом клетка обязана сказать в лог, иначе
    # метка соврет о том, что мерялось
    ceiling = 2 * SOCK_RCVBUF if sys.platform.startswith("linux") \
        else SOCK_RCVBUF
    if actual < ceiling:
        print(f"# sock_rcvbuf: ядро выдало МЕНЬШЕ запрошенного (потолок "
              f"net.core.rmem_max): мерилось {actual} Б, метка обещает "
              f"{SOCK_RCVBUF} Б", file=sys.stderr)
    _SOCK_RCVBUF_SET.append(actual)
    return actual


def _assert_sock_rcvbuf_applied(path: str = "") -> None:
    """Ось задана, а поставить ее было некому - падаем (RS-02).

    Пара k7-pgsock-* различается ровно этой осью. Путь, который ее не умеет
    (ADBC, connectorx, HTTP-клиент ClickHouse, JDBC), обязан сказать об этом
    вслух: иначе в корпусе останется пара строк с разными метками и
    одинаковыми числами, из которой прочитают ложное "буфер ни при чем".
    """
    if SOCK_RCVBUF > 0 and not _SOCK_RCVBUF_SET:
        raise SystemExit(
            f"BENCH_SOCK_RCVBUF={SOCK_RCVBUF} у пути "
            f"{path or os.environ.get('BENCH_PATH_KIND', '?')}: ось умеют "
            "только клиенты на psycopg (pg_psycopg, pg_copy, "
            "pg_server_cursor, pg_execs, pg_pandas, ch_pg_emu) - здесь буфер "
            "остался бы дефолтным, а метка клетки обещала бы другой размер")


def _extras(rec: dict, **values) -> dict:
    """Дописать в запись колонки паспорта клетки, терпимо к старой схеме.

    Колонки среза v1.5 (codec_level, drain_class, frame) добавляются в конец
    схемы _measure. Пока схема не обновлена, значение отбрасывается с одной
    строкой в stderr, а не роняет клетку: путь и обвязка выкатываются
    разными правками одного среза.
    """
    for key, value in values.items():
        if value is None or value == "":
            continue
        if key in EXTRA_COLS:
            rec.setdefault(key, value)
        elif key not in _EXTRA_WARNED:
            _EXTRA_WARNED.add(key)
            print(f"# extras: колонки {key} нет в схеме _measure - значение "
                  f"{value!r} в CSV не поедет (старый срез обвязки)",
                  file=sys.stderr)
    return rec


# F4 (GRID-03, PATH-12, PATH-07): mat_level=drain склеивал пять разных
# физик, и клетки разных классов сравнивались между собой как однородные.
# Класс объявляется ФУНКЦИЕЙ от пути и режима - ровно та же таблица живет в
# runner/lib.sh (--list-cases печатает класс и проверяет однородность блока),
# девятого поля в спецификацию клетки не добавляется.
_DRAIN_CLASS_BY_PATH = {
    "ch_http": "raw",        # сырые байты ответа, разбора нет
    "pg_copy": "raw",        # CopyData, тоже байты
    "file_export": "raw",    # чтение файла блоками
    "pg_adbc": "arrow",      # батчи Arrow
    "ch_flightsql": "arrow",
    "ch_adbc_http": "arrow",
    "pg_flightsql": "arrow",
    "duckdb_file": "arrow",
    "ch_native": "rows",     # драйвер строит строки, мы их сразу отпускаем
    "pg_server_cursor": "rows",
    "ch_pg_emu": "rows",     # эмуляции: пара к pg_psycopg по построению
    "ch_mysql_emu": "rows",
    "jdbc_pg": "rows",
    "jdbc_ch": "rows",
    "pg_execs": "buffered",  # клиент физически не стримит
    "cx_pg": "buffered",
    "pg_pandas": "buffered",
}


def drain_class_of(path: str, mode: str = None, drain_form: str = None) -> str:
    """Класс слива клетки: raw | rows | arrow | buffered | srvcost | пусто.

    Пусто у клеток вне слива (materialize, digest, dfgate) - там уровень
    материализации и так назван меткой.

    Основная таблица живет в bench_axes (единый словарь имен, его же двойник
    в lib.sh для --list-cases); здесь только вызов и запасной вариант на
    дерево, где функции еще нет. Своей третьей копии правды заводить нельзя -
    именно на разъехавшихся копиях класс слива и терялся.
    """
    fn = getattr(bench_axes, "drain_class", None)
    if fn is not None:
        return fn(path, mode, drain_form)
    mode = MODE if mode is None else mode
    drain_form = DRAIN_FORM if drain_form is None else drain_form
    if mode == "srvcost":
        return "srvcost"
    if mode != "drain":
        return ""
    if path == "pg_psycopg":
        # HC7: серверный курсор против обычного курсора клиента - это две
        # разные физики под одним именем drain, поэтому класс задает ось
        return "buffered" if drain_form == "buffered" else "rows"
    return _DRAIN_CLASS_BY_PATH.get(path, "rows")


def _cell_codec_level():
    """Значение колонки codec_level: точка оси уровня или пусто.

    Уровень пишется только там, где он ФИЗИЧЕСКИ доезжает до сервера - на
    транспортном слое (http_zlib_compression_level у HTTP,
    network_zstd_compression_level у нативного zstd). У форматного слоя и у
    none-клеток ручки уровня нет вовсе, и число в колонке обещало бы корпусу
    точку оси, которой в клетке нет. Значение - из bench_axes
    (CODEC_LEVEL_COL), тем же правилом.
    """
    value = getattr(bench_axes, "CODEC_LEVEL_COL", None)
    if value is not None:
        return value or None
    if CODEC == "none" or CODEC_PLAN["layer"] != "transport":
        return None
    return CODEC_LEVEL


# =========================================================================
# СЕКЦИЯ 1. Обвязка исполнения: _drain / _execs / _single / _byte_blocks
# =========================================================================

# Идентификатор контракта измерения (срез C1, шаг 2). Контракт отвечает на
# один вопрос: КАКИЕ интервалы входят в окно клетки. Их два, и они не
# смешиваются в одной таблице без подписи:
#
#   timer/c1-exec  окно = одно исполнение в уже открытом соединении.
#       Установка соединения - отдельная колонка connect_s, закрытие вне
#       окна, освобождение объекта результата - отдельная колонка cleanup_s,
#       служебные пробы пути - отдельная колонка post_check_s. Так идут все
#       зачетные клетки C1: и при одном исполнении на соединение, и при
#       нескольких - одним и тем же кодом (_measured_exec);
#   timer/c1-call  окно = весь вызов пути, вместе с установкой и закрытием.
#       Так идут пути, у которых соединения в нашем распоряжении нет:
#       connectorx, выгрузка в файл, чтение файла (_single).
#
# Версия в имени обязательна: корпус прежних серий снят контрактом, где
# ветка одного исполнения мерила весь вызов, а ветка нескольких - только
# исполнение (prelaunch-ревью, B01). Строки без этой колонки - строки до
# среза C1, и это видно по самой пустоте колонки.
_ENDPOINT = {}
TIMER_CONTRACT_ID = "timer/c1-exec"
TIMER_CONTRACT_CALL = "timer/c1-call"

def _dtypes(row) -> str:
    """Фактические типы первой строки - главная улика деградации типов:
    эмуляции и текстовые форматы отдают DateTime и Decimal строками, и это
    должно попасть в CSV, а не потеряться."""
    if row is None:
        return ""
    if isinstance(row, dict):
        row = list(row.values())
    return ";".join(type(v).__name__ for v in row)


def _dtypes_first(rows) -> str:
    return _dtypes(rows[0]) if len(rows) else ""


def _note_opaque(dtypes: str) -> str:
    """Громкая строка в лог, когда клиент довез значение НЕ типом (F24).

    PATH-04: adbc-driver-postgresql отдает numeric текстом внутри Arrow
    (extension arrow.opaque), а метка клетки при этом обещает mat-typed -
    ровно та подмена, из-за которой прайс типа Д10 у ADBC назывался ценой
    Decimal. Колонку client_dtype мы пишем как есть, а здесь называем
    расхождение вслух: пост-ран список расходящихся типов собирает разбор.
    """
    if dtypes and "opaque" in dtypes:
        print(f"# типы: непрозрачный тип в выдаче при mat_level="
              f"{os.environ.get('BENCH_MAT_LEVEL', '') or '-'}: {dtypes}",
              file=sys.stderr)
    return dtypes


def _single(rec: dict):
    """Перевод записи пути в одиночный контракт _measure: (rows, ttfb, extra,
    extras). wall в этой форме считает обвязка, и в него входит ВЕСЬ вызов
    пути - вместе с установкой и закрытием соединения.

    Срез C1: это ДРУГОЙ контракт измерения, чем у _measured_exec, и строка
    обязана называть его своим именем (колонка timer_contract). Так идут
    пути, у которых соединения в нашем распоряжении нет вовсе (connectorx
    поднимает его внутри своей библиотеки, выгрузка в файл, чтение файла).
    Клетки разных контрактов не кладутся в одну таблицу молча.
    """
    # C7 (RS-02): единая воронка одиночных записей - здесь ловится путь,
    # который ось приемного буфера получил, но поставить не смог
    _assert_sock_rcvbuf_applied()
    _extras(rec, codec_level=_cell_codec_level(),
            timer_contract=TIMER_CONTRACT_CALL,
            extra_stage=extra_stage_of() if rec.get("extra_s") else "")
    extras = {k: rec[k] for k in EXTRA_COLS if k in rec}
    return rec["rows"], rec.get("ttfb_s"), rec.get("extra_s"), extras


def _drain(units, t0, rows_of=None, bytes_of=None, klass="rows", path=None,
           delay_every=1):
    """Единая семантика drain для всех путей: юнит потока посчитан и тут же
    отпущен, не накапливается НИЧЕГО.

    rows_of=None означает, что строки в потоке не различимы (сырые байты) -
    тогда берется число строк, заданное холстом. Считать переводы строк или
    конвертировать memoryview в bytes ради счетчика мы не будем: это работа
    клиента, которой в уровне "доставка" быть не должно.

    klass идет в client_dtype: raw (сырые байты), arrow (батчи Arrow), rows
    (драйвер строит строки, мы их сразу отпускаем), buffered (клиент физически
    не стримит - клетка не сравнивается по памяти и ttfb, см. CONTRACT).

    path (F4) заполняет колонку drain_class объявленным классом слива - той
    же функцией, что печатает класс раннер. Без path класс берется из klass:
    у путей, которые еще не назвали себя, колонка остается прежней.

    delay_every и ось BENCH_CLIENT_DELAY_US (C1) делают клиента медленным:
    после каждых delay_every юнитов поток спит заданное число микросекунд.
    Так меряется "сервер ждет клиента" - на нативном потоке юнит это строка,
    поэтому его путь просит усыплять раз в блок, а не раз в строку.
    """
    rows = nbytes = 0
    ttfb = None
    seen = 0
    delay_s = CLIENT_DELAY_US / 1e6 if CLIENT_DELAY_US > 0 else 0.0
    for unit in units:
        if ttfb is None:
            ttfb = time.perf_counter() - t0
        if rows_of is not None:
            rows += rows_of(unit)
        if bytes_of is not None:
            nbytes += bytes_of(unit)
        del unit
        if delay_s:
            seen += 1
            if seen % delay_every == 0:
                # намеренно sleep, а не активное ожидание: занятый цикл
                # съел бы ядро и подменил бы предмет замера (сервер ждет
                # медленного клиента) нагрузкой на процессор клиента
                time.sleep(delay_s)
    if rows_of is None:
        rows = _expected_rows()
    if nbytes:
        # в CSV колонки под прикладные байты нет - на проводе меряется дельта
        # rx_bytes интерфейса (методика всего корпуса); это число в лог для
        # перекрестной проверки
        print(f"# drain {klass}: прикладных байт {nbytes}", file=sys.stderr)
    if delay_s:
        print(f"# drain {klass}: задержка клиента {CLIENT_DELAY_US} мкс на "
              f"каждые {delay_every} юнитов потока", file=sys.stderr)
    rec = {"rows": rows, "ttfb_s": ttfb, "client_dtype": f"drain:{klass}"}
    return _extras(rec, drain_class=(drain_class_of(path) if path else klass))


# Транспортные кодеки HTTP, которые ClickHouse объявляет в Content-Encoding.
_WIRE_CODECS = ("lz4", "zstd")


def _http_encoding(reader) -> str:
    """Content-Encoding ответа в нижнем регистре.

    Для файлов, генераторов и всего, что не HTTP-ответ, - пустая строка:
    через _byte_blocks проходят и выгрузки в файл (путь аналитика).
    """
    headers = getattr(reader, "headers", None)
    get = getattr(headers, "get", None)
    if get is None:
        return ""
    return (get("content-encoding") or "").strip().lower()


def _wire_chunks(reader, size):
    """Куски ответа КАК НА ПРОВОДЕ: decode_content=False снимает молчаливую
    распаковку urllib3, чтобы оба кодека оси проходили через один наш код."""
    stream = getattr(reader, "stream", None)
    if stream is not None:  # публичный потоковый API urllib3
        yield from stream(size, decode_content=False)
        return
    while True:
        chunk = reader.read(size)
        if not chunk:
            return
        yield chunk


def _decode_wire(chunks, encoding: str):
    """Распаковка транспортного кодека потоком, кадр за кадром.

    ClickHouse отдает не один кадр, а цепочку: буфер сжатия закрывается по
    флашу, и хвост unused_data - это начало следующего кадра. Односоставная
    распаковка тихо отдала бы только первый кадр (проверено на lz4 и zstd),
    то есть клетка сжатия показала бы неполную выдачу без единой ошибки.

    Отдельно считаем байты провода и прикладные байты: их отношение - улика
    того, что сжатие действительно применилось, прямо в паспорте клетки.
    """
    if encoding == "lz4":
        new = lz4.frame.LZ4FrameDecompressor
    elif encoding == "zstd":
        ctx = zstandard.ZstdDecompressor()
        new = ctx.decompressobj
    else:
        raise SystemExit(
            f"сервер сжал ответ кодеком {encoding!r} - распаковать его нечем; "
            f"ось сжатия работает с {_WIRE_CODECS}")
    dec = new()
    wire = plain = 0
    tail = b""
    for chunk in chunks:
        wire += len(chunk)
        data = tail + chunk if tail else chunk
        tail = b""
        while data:
            if getattr(dec, "eof", False):
                # предыдущий кадр закрыт: распаковщик одноразовый, повторный
                # вызов у zstandard - прямая ошибка, у lz4 неопределенность
                dec = new()
            block = dec.decompress(data)
            if block:
                plain += len(block)
                yield block
            rest = getattr(dec, "unused_data", b"") or b""
            if not rest:
                break
            if len(rest) >= len(data):
                # распаковщик не съел ни байта: остаток уходит в начало
                # следующего куска, а не крутится на месте
                tail = rest
                break
            data = rest          # хвост - начало следующего кадра
    if wire:
        print(f"# codec {encoding} уровень {CODEC_LEVEL}: провод {wire} байт "
              f"-> прикладных {plain} (x{plain / wire:.2f})", file=sys.stderr)
    if wire and (tail or not getattr(dec, "eof", True)):
        # оборванный кадр молча отдал бы укороченную выдачу: строки посчитаны
        # по холсту, ошибки нет, а половины ответа не было
        raise SystemExit(
            f"кодек {encoding}: ответ оборван на незакрытом кадре "
            f"(прикладных байт {plain}, хвост {len(tail)}) - "
            "выдача клетки неполная")


def _byte_blocks(reader, size=HTTP_BLOCK_BYTES):
    """Байтовые блоки из потокового ответа.

    raw_stream в clickhouse-connect 0.8 отдает файлоподобный объект: итерация
    по нему шла бы ПОСТРОЧНО, а для Native, Arrow и Parquet такая нарезка
    бессмысленна - читаем блоками фиксированного размера.

    Транспортный кодек снимается здесь же. raw_stream отдает ответ как есть, и
    без распаковки клетка сжатия мерила бы доставку без ее цены: процессор
    получателя в счет не попал бы, прикладные байты разошлись бы с none-парой,
    а разбор формата (JSON, CSV) получил бы на вход сжатый блоб.
    """
    encoding = _http_encoding(reader)
    if encoding:
        yield from _decode_wire(_wire_chunks(reader, size), encoding)
        return
    read = getattr(reader, "read", None)
    if read is None:
        yield from reader
        return
    while True:
        chunk = read(size)
        if not chunk:
            return
        yield chunk


def _close(obj) -> None:
    closer = getattr(obj, "close", None)
    if closer is None:
        return
    try:
        closer()
    except Exception:  # поток мог умереть в самом замере - это не сбой закрытия
        pass


def shuffle_keys(keys):
    """Порядок обстрела ключей П4: 'случайный', но воспроизводимый - seeded
    shuffle детерминированной выборки. Настоящий random дал бы каждой серии
    свой набор попаданий в кеш, и раунды перестали бы сходиться."""
    keys = list(keys)
    if not keys:
        raise SystemExit(
            "выборка ключей точечных запросов пуста - таблица не залита или "
            "выборка ключей сломана; клетка П4 без ключей не имеет смысла")
    random.Random(20260101).shuffle(keys)
    return keys


# Ключи, которые обвязка _measure принимает в списочном контракте помимо
# EXTRA_COLS. Все остальное из записи пути отбрасывается ГРОМКО (строка в
# stderr): молча уехавший в никуда ключ - это потерянная колонка серии.
_LIST_BASE_KEYS = ("wall_s", "rows", "ttfb_s", "extra_s", "bytes_rx_mb",
                   "exec_no", "peak_rss_mb")


def _filter_record(rec: dict) -> dict:
    allowed = set(_LIST_BASE_KEYS) | set(EXTRA_COLS)
    out = {}
    for key, value in rec.items():
        if key in allowed:
            out[key] = value
        else:
            print(f"# extras: колонки {key} нет в схеме _measure - "
                  "значение отброшено", file=sys.stderr)
    return out


# =========================================================================
# F15 (C2): query_id зачетного запроса клетки
# =========================================================================
# Серверный слой брал на клетку ОДИН самый тяжелый запрос окна, и клетка со
# 100 исполнениями отдавала серверное время одного из них против wall всех
# ста. Лечение - явный query_id на каждое исполнение по общему формату
#   standf:<метка клетки>:r<раунд>:e<номер исполнения>
# Раннер собирает записи query_log с этим префиксом и СУММИРУЕТ их (ch_calls,
# ch_execs_expected, srv_scope=cell_sum). Ставим там, где драйвер это
# позволяет: clickhouse-driver (параметр query_id), clickhouse-connect
# (транспортная настройка query_id). Flight SQL и эмуляции id не пробрасывают
# - там остается прежняя атрибуция по окну.
_QID_SAFE = re.compile(r"[^A-Za-z0-9_.:-]")

# номер текущего исполнения внутри вызова пути: у одиночного контракта он 1,
# в списочном режиме его двигает _execs. Отдельного счетчика у путей нет
# сознательно - два счетчика разъехались бы с колонкой exec_no
_EXEC_NO = [1]


def current_exec_no() -> int:
    return _EXEC_NO[0]


def _cell_label() -> str:
    """Метка клетки - ровно та, что уезжает в колонку `path` сырья.

    Сначала MEASURE_LABEL: раннер задает им метку каждой клетки, и по этой же
    строке серверный слой сшивает записи query_log со строкой сырья.
    BENCH_SCENARIO_ID тут запасной вход, а не первый: у сетки он один на весь
    сценарий (`09_gen_paths`), и клетки одного раунда получили бы ОДИН
    query_id - серверная сумма склеила бы разные клетки.
    """
    return (os.environ.get("MEASURE_LABEL", "").strip()
            or os.environ.get("BENCH_SCENARIO_ID", "").strip()
            or os.environ.get("BENCH_PATH_KIND", "").strip()
            or "cell")


def cell_query_id(exec_no: int = None) -> str:
    """query_id зачетного запроса: standf:<метка>:r<раунд>:e<исполнение>."""
    label = _QID_SAFE.sub("_", _cell_label())
    rnd = _QID_SAFE.sub("_", os.environ.get("BENCH_ROUND", "").strip() or "0")
    n = current_exec_no() if exec_no is None else exec_no
    return f"standf:{label}:r{rnd}:e{int(n)}"


def ch_qid_settings(settings: dict = None, exec_no: int = None) -> dict:
    """Настройки запроса clickhouse-connect с проброшенным query_id.

    Настройка транспортная: имя уезжает в параметры HTTP-запроса, на план и
    на формат ответа не влияет. Служебные запросы клетки (выборка ключей,
    system.settings) через эту функцию НЕ идут - у них свой маркер setup, и
    попади они под тот же id, серверная сумма клетки включила бы подготовку.
    """
    out = dict(settings or {})
    out["query_id"] = cell_query_id(exec_no)
    return out


def observe_endpoint(conn):
    """Фактические адрес, порт и режим шифрования соединения (шаг 6 C1).

    Наблюдение, а не объявление. До среза C1 строка сырья несла только то,
    что раннер ОБЕЩАЛ переменными окружения: имя точки входа, порт из
    конфигурации, "TLS off" по имени переменной. Клетка, уехавшая на другой
    порт (общий LANE_PG_PORT вместо порта адаптера) или поднявшая TLS сама,
    выглядела бы точно так же (prelaunch-ревью, B08).

    Здесь мы спрашиваем САМО соединение и записываем, чем именно спросили
    (колонка tls_observed_by). Чего спросить не удалось, остается пустым:
    пустая колонка честнее выдуманного значения.
    """
    out = {}
    # psycopg 3: conn.info знает адрес, порт и факт шифрования
    info = getattr(conn, "info", None)
    if info is not None and hasattr(info, "port"):
        try:
            out["peer_addr"] = str(getattr(info, "hostaddr", "")
                                   or getattr(info, "host", "") or "")
            out["peer_port"] = str(info.port)
            enc = None
            for attr in ("ssl_in_use", "ssl_attribute"):
                if hasattr(conn, "pgconn") and attr == "ssl_in_use":
                    enc = bool(getattr(conn.pgconn, "ssl_in_use", False))
                    break
            if enc is not None:
                out["tls_observed"] = "on" if enc else "off"
                out["tls_observed_by"] = "libpq:PQsslInUse"
        except Exception:                                      # noqa: BLE001
            pass
        return out
    # клиенты поверх сокета (clickhouse-driver): сокет знает, куда он ходит
    sock = getattr(conn, "socket", None) or getattr(conn, "_socket", None)
    if sock is not None and hasattr(sock, "getpeername"):
        try:
            peer = sock.getpeername()
            out["peer_addr"] = str(peer[0])
            out["peer_port"] = str(peer[1])
            out["tls_observed"] = "on" if hasattr(sock, "cipher") else "off"
            out["tls_observed_by"] = "socket:getpeername"
        except Exception:                                      # noqa: BLE001
            pass
    return out


def _measured_exec(conn, consume, exec_no: int):
    """ОДНО исполнение в открытом соединении под полной обвязкой окна.

    Единственное место, где снимается окно клетки - и при одном исполнении,
    и при нескольких. До среза C1 этих мест было два: ветка BENCH_EXECS=1
    отдавала кортеж, и окно ей мерила обвязка _measure вокруг ВСЕГО вызова
    пути, то есть вместе с установкой и закрытием соединения. Ветка
    BENCH_EXECS>1 мерила только исполнение. Клетки одной пары приходили в
    корпус по разным контрактам, и разница контрактов читалась как разница
    протоколов (prelaunch-ревью, B01).

    Порядок здесь - и есть контракт измерения TIMER_CONTRACT_ID:
      1. соединение уже установлено, ось приемного буфера уже проверена;
      2. базовые счетчики и сброс пика памяти - ПОСЛЕ них;
      3. начало окна - после базовых счетчиков;
      4. время, счетчики процессора и сети, пик памяти - ПРИ ЖИВОМ объекте
         результата, до его освобождения и до закрытия соединения;
      5. освобождение объекта - отдельной величиной cleanup_s ПОСЛЕ окна.
    """
    _EXEC_NO[0] = exec_no          # C2: тот же номер, что в колонке exec_no
    net0 = _net_counters()
    steal0 = steal_ticks()
    reset_peak_rss()
    ru0 = resource.getrusage(resource.RUSAGE_SELF)
    ts0 = _now_ms()
    start = time.perf_counter()    # соединение уже установлено
    rec = consume(conn, start)
    rec["wall_s"] = time.perf_counter() - start
    threads = rec.pop("polars_threads", None)
    if threads is not None:
        note = f"polars_threads={threads}"
        base = str(rec.get("proto_mode") or "")
        rec["proto_mode"] = f"{base}+{note}" if base else note
    ts1 = _now_ms()
    ru1 = resource.getrusage(resource.RUSAGE_SELF)
    steal1 = steal_ticks()
    net1 = _net_counters()
    # F07: пик снимается ПРИ ЖИВОЙ структуре
    rec.setdefault("peak_rss_mb", peak_rss_mb())
    # F07: часы остановлены, пик снят - только теперь отпускаем holder.
    # В списочном режиме обвязка _measure этого не делает: она видит уже
    # готовые записи
    cleanup_s = release_live()
    if cleanup_s is not None:
        rec.setdefault("cleanup_s", cleanup_s)
    rec.setdefault("ts", ts0)
    rec.setdefault("ts_end", ts1)
    if not rec.get("cpu_user_s") and rec.get("cpu_user_s") != 0.0:
        rec["cpu_user_s"] = ru1.ru_utime - ru0.ru_utime
        rec["cpu_sys_s"] = ru1.ru_stime - ru0.ru_stime
        rec["cpu_scope"] = "exec"
    if net0 is not None and net1 is not None:
        rec.setdefault("bytes_rx_mb",
                       (net1["rx_bytes"] - net0["rx_bytes"]) / 2**20)
        rec.setdefault("bytes_tx_mb",
                       (net1["tx_bytes"] - net0["tx_bytes"]) / 2**20)
        rec.setdefault("packets_rx", net1["rx_packets"] - net0["rx_packets"])
        rec.setdefault("packets_tx", net1["tx_packets"] - net0["tx_packets"])
    if steal0 is not None and steal1 is not None:
        rec.setdefault("steal_ticks", steal1 - steal0)
    _extras(rec, codec_level=_cell_codec_level(),
            timer_contract=TIMER_CONTRACT_ID,
            extra_stage=extra_stage_of() if rec.get("extra_s") else "",
            **_ENDPOINT)
    return _filter_record(rec)


def _execs(connect, consume):
    """Каркас пути: одно соединение, BENCH_EXECS исполнений на нем.

    consume(conn, t0) -> dict с ключами rows, ttfb_s, extra_s и любыми из
    EXTRA_COLS.

    Отдает СПИСОК записей при любом BENCH_EXECS, в том числе при одном
    исполнении: окно замера снимает _measured_exec одним и тем же кодом, и в
    него не входят ни установка, ни закрытие соединения. Цена соединения -
    отдельная колонка connect_s на первой записи, цена освобождения объекта -
    отдельная колонка cleanup_s.

    До среза C1 при BENCH_EXECS=1 каркас отдавал кортеж, и обвязка _measure
    мерила окно вокруг всего вызова пути. Это и была правка B01: контракт
    измерения обязан быть один, а какой именно - написано в колонке
    timer_contract каждой строки.
    """
    t0 = time.perf_counter()
    # C2: номер исполнения нужен query_id ЗАЧЕТНОГО запроса. Соединение и
    # подготовка идут до первого исполнения - на них номер еще первый, но их
    # запросы помечены маркером setup и в серверную сумму клетки не входят
    _EXEC_NO[0] = 1
    conn = connect()
    connect_s = time.perf_counter() - t0
    # C7 (RS-02): ось приемного буфера ставится внутри connect (только у
    # клиентов на psycopg). Если соединение поднял кто-то другой, падаем
    # ДО замера - клетка с чужой меткой хуже отсутствующей
    _assert_sock_rcvbuf_applied()
    # фактическая точка входа наблюдается ОДИН раз на соединение и уезжает
    # в каждую строку окна
    _ENDPOINT.clear()
    _ENDPOINT.update(observe_endpoint(conn))
    try:
        records = []
        for i in range(EXECS):
            rec = _measured_exec(conn, consume, i + 1)
            if i == 0:
                # готовность соединения до отправки первого запроса
                rec.setdefault("connect_s", connect_s)
            records.append(rec)
        return records
    finally:
        # у драйверов метод называется по-разному (close у DB-API,
        # disconnect у clickhouse-driver)
        for name in ("close", "disconnect"):
            closer = getattr(conn, name, None)
            if closer is None:
                continue
            try:
                closer()
            except Exception:  # соединение могло умереть в самом замере
                pass
            break


# =========================================================================
# СЕКЦИЯ 2. Канонический digest (полный и усеченный)
# =========================================================================

def _sampled_rows(rows, sample_n: int):
    """Усеченная выборка для digest на сверхбольших объемах.

    В канон входят: первые sample_n строк; каждая sample_n-я из остальных
    (детерминированный шаг, не случайность); финальный маркер с общим числом
    строк - усечение не должно спрятать потерю хвоста. Контрольные серверные
    агрегаты (sum/min/max) снимаются отдельной пробой раннера, не здесь.
    """
    total = 0
    for idx, row in enumerate(rows):
        total += 1
        if idx < sample_n or idx % sample_n == 0:
            yield row
        else:
            del row
    yield (f"__digest_sample__:n={sample_n}", f"rows_total={total}")


# --- логическая схема клетки (срез C1) -------------------------------------
# Кодек canonizes значения ПО ОБЪЯВЛЕННОЙ схеме, а не по тому, что вернул
# драйвер: иначе uint32 DateTime у Arrow-пути ClickHouse и datetime у
# кортежного дают разные digest на одних и тех же данных. Схема ширины
# выведена из манифеста раздачи и DDL один раз и лежит в репозитории.
_SCHEMAS_CACHE = [None]


def _logical_schemas_path():
    env = os.environ.get("STANDF_LOGICAL_SCHEMAS", "").strip()
    if env:
        return Path(env)
    here = Path(__file__).resolve().parent            # standf/scenarios
    return (here.parent.parent / "analysis" / "run-profiles" / "focused-talk"
            / "logical-schemas.json")


def cell_logical_schema():
    """Объявленная логическая схема клетки либо None.

    Ширина берется из имени таблицы клетки (BENCH_PROFILE: cb_w10_1m ->
    w10). Схемы нет - digest считается по тегам типов самих значений
    (режим auto кодека): это законно для служебных проб, но для зачетной
    сверки путей схема обязана быть, и ее отсутствие видно по пустой
    колонке schema_sig.
    """
    # на холсте narrow имя таблицы приходит в BENCH_TABLE, на gen - в
    # BENCH_PROFILE (там это имя профиля генерации, и таблицы cb_* не бывает)
    table = (os.environ.get("BENCH_TABLE", "").strip()
             or os.environ.get("BENCH_PROFILE", "").strip())
    m = re.match(r"^cb_(w\d+)_", table)
    if not m:
        return None
    if _SCHEMAS_CACHE[0] is None:
        path = _logical_schemas_path()
        try:
            _SCHEMAS_CACHE[0] = json.loads(path.read_text(encoding="utf-8"))
        except OSError:
            print(f"# digest: нет файла логических схем {path} - канонизация "
                  "пойдет по тегам типов значений, а не по объявленной схеме",
                  file=sys.stderr)
            _SCHEMAS_CACHE[0] = {}
    widths = (_SCHEMAS_CACHE[0] or {}).get("widths") or {}
    body = widths.get(m.group(1))
    if not body:
        return None
    return _codec.LogicalSchema.parse(body["columns"])


def _refuse_sampling_if_declared(schema) -> str:
    """Режим кодека: усечение запрещено там, где идет ЗАЧЕТНАЯ сверка (D43).

    Граница проходит по объявленной логической схеме клетки, и это не
    формальность. Схема есть ровно у рабочих таблиц профиля (cb_*) - там
    сверка сравнивает выдачу с эталоном таблицы, и усеченный проход называл
    бы сверкой то, что пропускает расхождение в хвосте. У синтетических
    холстов (gen, shape) схемы нет: там digest - гейт идентичности стенда на
    заведомо огромном профиле, и выборка законна.

    Чтобы усеченный digest нельзя было сравнить с полным по ошибке, РЕЖИМ
    входит в само значение digest: "codec/v1:sampled:1000:..." никогда не
    совпадет с "codec/v1:full:...".
    """
    if DIGEST_SAMPLE <= 0:
        return _codec.CODEC_MODE_FULL
    if schema is not None:
        raise SystemExit(
            f"BENCH_DIGEST_SAMPLE={DIGEST_SAMPLE} на клетке с объявленной "
            "логической схемой: усеченная сверка значений запрещена срезом "
            "C1 - полная сверка идет один раз на пару путь-таблица за "
            "сессию, и резать ее нечем")
    print(f"# digest: усеченный режим, sample={DIGEST_SAMPLE} - это гейт "
          "идентичности на синтетическом холсте, не зачетная сверка",
          file=sys.stderr)
    return f"sampled:{DIGEST_SAMPLE}"


def digest_rows(rows):
    """Digest выдачи кодеком C1 (codec/v1): мультимножество записей."""
    schema = cell_logical_schema()
    mode = _refuse_sampling_if_declared(schema)
    if mode != _codec.CODEC_MODE_FULL:
        rows = _sampled_rows(iter(rows), DIGEST_SAMPLE)
    return _codec.digest_rows(rows, schema, mode=mode)


def digest_arrow(table):
    """Digest Arrow-выдачи: тот же кодек, векторная реализация.

    На 10M x 105 построчная канонизация в окно сессии не помещается
    (prelaunch-ревью, B04), поэтому Arrow-пути идут ядрами pyarrow и numpy.
    Результат обязан совпадать с построчным - это проверяется тестами, а не
    подразумевается.
    """
    schema = cell_logical_schema()
    mode = _refuse_sampling_if_declared(schema)
    if mode != _codec.CODEC_MODE_FULL:
        # усечение Arrow-выдачи идет через построчную ветку: резать батчи
        # детерминированным шагом и при этом остаться векторным нельзя, а
        # выборка бывает только на синтетических холстах, где объем невелик
        return _codec.digest_rows(_sampled_rows(iter(_arrow_to_tuples(table)),
                                                DIGEST_SAMPLE),
                                  schema, mode=mode)
    return _codec.digest_arrow(table, schema)


# =========================================================================
# СЕКЦИЯ 3. Целевые структуры клиента и гейты (dfgate, rows, arrow, polars)
# =========================================================================

def _require_polars() -> None:
    """Отсутствие polars - пустая клетка окружения, а не сбой замера. Падаем
    внятно и только на самой клетке: импорт модуля отсутствие пакета не
    ломает (см. try/except наверху)."""
    if pl is None:
        raise SystemExit(
            "BENCH_TARGET_STRUCT=polars, а пакет polars не установлен - "
            "это пустая клетка окружения, а не сбой замера "
            "(pip install polars)")


def _polars_frame(obj, columns=None):
    """polars из Arrow-таблицы или из уже собранных питоновских кортежей.

    Две физики одного имени оси, и обе живут тут же:
      из Arrow  - pl.from_arrow забирает готовые буферы, копии значений нет;
      из строк  - каждое значение уже стало объектом питона, и polars только
                  перекладывает их к себе.
    Разница в цене между этими двумя ветками и есть смысл клетки polars: имя
    целевой структуры одно, а платит за нее клиент совершенно по-разному.
    """
    _require_polars()
    if isinstance(obj, (pa.Table, pa.RecordBatch)):
        return pl.from_arrow(obj)
    return pl.DataFrame(obj, schema=columns, orient="row")


def _polars_from_columns(cols, names):
    """polars из колоночной выдачи драйвера (clickhouse-driver columnar=True,
    clickhouse-connect result_columns): значения уже объекты питона, polars
    только перекладывает их к себе - класс 1, как из кортежей."""
    _require_polars()
    return pl.DataFrame({n: list(c) for n, c in zip(names, cols)})


def _declared_names():
    """Имена колонок объявленной схемы клетки: по логической схеме ширины
    таблицы (cb_w50_1m -> w50), иначе по BENCH_DF_SCHEMA. None - схемы нет."""
    schema = cell_logical_schema()
    if schema is not None:
        return [c.name for c in schema.columns]
    if DF_SCHEMA:
        from _dfschema import SCHEMAS, load_width_schemas  # noqa: PLC0415
        load_width_schemas()
        decl = SCHEMAS.get(DF_SCHEMA)
        if decl:
            return [n for n, _ in decl]
    return None


# --- парсеры табличных библиотек над байтами ответа (D52) -----------------
# Смысл клеток "формат до датафрейма": разбирает библиотека, которой
# пользуется аналитик (pandas.read_csv, polars.read_csv, read_json,
# read_ndjson, pyarrow.json), а не модуль csv/json Python через кортежи.
# Иначе клетка мерила бы парсер Python под меткой формата - та же ловушка,
# что F80 у приведения к схеме. extra_s у этих клеток - время самого
# разбора: приход байт стоит до t1, приведение к схеме - в extra2_s.

def _declared_decl():
    """Объявление колонок клетки [(имя, объявление pandas)] либо None."""
    if not DF_SCHEMA:
        return None
    from _dfschema import SCHEMAS, load_width_schemas      # noqa: PLC0415
    load_width_schemas()
    decl = SCHEMAS.get(DF_SCHEMA)
    return list(decl) if decl else None


def _text_dtypes(kind: str, strings_only: bool = False):
    """Подсказки типов парсеру текстового формата из объявленной схемы.

    Инференс парсера по первым строкам - не свойство формата, а свойство
    парсера, и на широкой таблице он либо падает (polars: "invalid primitive
    value" на колонке, где после тысяч чисел встречается буква), либо тихо
    портит значения ("007" становится 7, и приведение к строке дает "7" -
    сверка с эталоном разошлась бы). Поэтому строковые колонки объявляются
    строками ЯВНО, целые - целыми; моменты времени остаются строками и
    приводятся общей воронкой (extra2_s), как у всех df-клеток.
    strings_only - для JSON: там 64-битные целые ClickHouse пишет строками,
    и объявление Int64 парсеру их не прочитает; целые приводит воронка.
    """
    decl = _declared_decl()
    if not decl:
        return None
    out = {}
    for name, d in decl:
        if d == "str":
            out[name] = "string" if kind == "pandas" else pl.String
        elif d in ("int64", "float64") and not strings_only:
            if kind == "pandas":
                out[name] = "int64" if d == "int64" else "float64"
            else:
                out[name] = pl.Int64 if d == "int64" else pl.Float64
        elif d.startswith("datetime64"):
            out[name] = "string" if kind == "pandas" else pl.String
    return out


def _csv_header_names(blob):
    """Имена колонок из строки заголовка csv (COPY ... HEADER)."""
    import csv as _csv                                     # noqa: PLC0415
    import io as _io                                       # noqa: PLC0415
    first = bytes(blob).split(b"\n", 1)[0].decode("utf-8", "replace")
    return next(_csv.reader(_io.StringIO(first)))


def _match_names(dtypes, actual):
    """Подсказки типов под ФАКТИЧЕСКИЕ имена колонок потока: PostgreSQL
    пишет заголовок в нижнем регистре (watchid), объявление - как в DDL
    (WatchID). Сопоставление без учета регистра, как у приведения к схеме."""
    if not dtypes or not actual:
        return dtypes
    low = {k.lower(): v for k, v in dtypes.items()}
    return {name: low[name.lower()] for name in actual if name.lower() in low}


def _df_from_csv(blob, names=None, header=False, path=""):
    import io                                              # noqa: PLC0415
    t1 = time.perf_counter()
    kw = {"header": 0 if header else None}
    if names and not header:
        kw["names"] = names
    dtypes = _text_dtypes("pandas")
    if dtypes:
        actual = _csv_header_names(blob) if header else names
        kw["dtype"] = _match_names(dtypes, actual)
    df = pd.read_csv(io.BytesIO(bytes(blob)), **kw)
    return _finish_df(df, t1, path)


def _polars_from_csv(blob, names=None, header=False, path=""):
    _require_polars()
    t1 = time.perf_counter()
    kw = {"has_header": bool(header)}
    if names and not header:
        kw["new_columns"] = list(names)
    overrides = _text_dtypes("polars")
    if overrides:
        actual = _csv_header_names(blob) if header else names
        kw["schema_overrides"] = _match_names(overrides, actual)
    frame = pl.read_csv(bytes(blob), **kw)
    return _finish_polars(frame, t1, path)


def _df_from_ndjson(blob, path=""):
    import io                                              # noqa: PLC0415
    t1 = time.perf_counter()
    kw = {"lines": True}
    dtypes = _text_dtypes("pandas", strings_only=True)
    if dtypes:
        kw["dtype"] = dtypes
    df = pd.read_json(io.BytesIO(bytes(blob)), **kw)
    return _finish_df(df, t1, path)


def _polars_from_ndjson(blob, path=""):
    _require_polars()
    t1 = time.perf_counter()
    kw = {}
    overrides = _text_dtypes("polars", strings_only=True)
    if overrides:
        kw["schema_overrides"] = overrides
    frame = pl.read_ndjson(bytes(blob), **kw)
    return _finish_polars(frame, t1, path)


def _arrow_from_ndjson(blob, path=""):
    import io                                              # noqa: PLC0415
    import pyarrow.json as pjson                           # noqa: PLC0415
    table = pjson.read_json(io.BytesIO(bytes(blob)))
    return _arrow_result(table, path=path)


def _frame_dtypes(frame) -> str:
    """Фактические типы колонок готового датафрейма (pandas или polars).

    Колонка client_dtype - единственное место, где видно, что пути блока
    аналитика собрали РАЗНЫЕ фреймы: object против int64, python-Decimal
    против decimal128, строка против datetime. Без нее сравнение скоростей
    незаконно, потому что сравнивались бы разные вещи.
    """
    return ";".join(str(t) for t in frame.dtypes)


def _polars_read_database(conn, query: str, path: str = ""):
    """polars забирает соединение и вычитывает курсор сам.

    Именно так эту строчку пишет аналитик, и именно поэтому клетка ценна: имя
    целевой структуры то же, что на Arrow-путях, а физика под ним другая.
    Поверх DB-API соединения (не Arrow-источника) polars получает от драйвера
    ОБЫЧНЫЕ питоновские кортежи и лишь собирает из них фрейм своими силами -
    класс 1, а не класс 3. Слайдовая формулировка ровно такая: одно имя
    функции, две физики и разница в цене в разы.

    Кадрирование и кодировку значений выбирает polars, а не мы, поэтому
    BENCH_FMT здесь обязан быть пустым или simple_text (проверяет вызывающий).
    """
    _require_polars()
    t1 = time.perf_counter()
    frame = pl.read_database(query, conn)
    # F07: возврат идет ЧЕРЕЗ общую воронку, как и остальные materialize.
    # Мимо нее клетка an-psycopg-polars освобождала бы фрейм внутри wall, а
    # ее пара по оси (an-cx_pg-binary-polars, an-*-arrow-polars) - уже вне
    # его: это ровно та кросс-путевая асимметрия, ради которой заведен
    # cleanup_s, только внутри одного языка
    return _finish_polars(frame, t1, path)


def _assert_polars_wire(path: str, fmt: str = None) -> None:
    """BENCH_FMT на клетке polars: только пустой или simple_text.

    pl.read_database открывает курсор сам и ни prepare=True, ни binary=True
    ему не передашь. Молча проигнорировать BENCH_FMT нельзя: в CSV колонка fmt
    заполняется из окружения раннером, и клетка уехала бы в графики под чужой
    меткой кодировки.
    """
    effective = FMT if fmt is None else fmt
    if effective not in ("", "simple_text"):
        raise SystemExit(
            f"BENCH_FMT={effective!r} на клетке BENCH_TARGET_STRUCT=polars "
            f"пути {path}: polars открывает курсор сам и кодировку значений "
            "не выбирает - клетка уехала бы в графики под чужой меткой формата")


def _dfgate(df, build_s=None) -> dict:
    """Гейт идентичности датафрейма: хеш содержимого и число колонок.

    Числа колонок в хеш НЕ подмешиваем - при расхождении должно быть сразу
    видно, разошлись данные или ширина. Хеш считается по значениям, а не по
    питоновским типам (см. _canon в _srvcontrol): у разных путей один и тот же
    миллион приезжает разными классами, и это нормально.

    В client_dtype идет РОВНО маркер гейта, без типов: гейт сравнивает клетки
    между собой строкой, а типы у путей блока аналитика законно разные, и
    подмешивание их сюда превратило бы гейт в вечный красный. Фактические типы
    печатаются в stderr (там их видит лог серии) и попадают в CSV в режиме
    materialize, где им и место.

    Хеш гейта всегда ПОЛНЫЙ: гейт снимается на малом объеме, усеченный digest
    (BENCH_DIGEST_SAMPLE) на него не распространяется.
    """
    if DF_SCHEMA:
        # E1: гейт сверяет датафреймы ПОСЛЕ приведения к объявленной схеме -
        # uint32 эпохи и datetime до приведения дали бы разные хеши при
        # одинаковых значениях, а сверяется полезный финиш
        df, _ = normalize_df(df, DF_SCHEMA)
    print(f"# dfgate dtypes: {_frame_dtypes(df)}", file=sys.stderr)
    cols = list(df.columns)
    rows_iter = (tuple(_dfgate_cell(c, v) for c, v in zip(cols, row))
                 for row in df.itertuples(index=False, name=None))
    if DF_SCHEMA:
        # E1: полное чтение таблицы без ORDER BY - порядок строк у двух путей
        # законно разный (Native и HTTP отдают блоки в своем порядке, стенд E1
        # 2026-09-13: хеши разошлись при тех же данных). Сверяется
        # мультимножество строк: канонические строки сортируются перед хешем
        rows_iter = iter(sorted(rows_iter))
    return {
        "rows": int(len(df)),
        "extra_s": build_s,
        "digest": sha256_digest(rows_iter),
        "client_dtype": f"dfgate:cols={len(df.columns)}",
    }


def _dfgate_cell(col, v):
    """Смысловая канонизация ячейки датафрейма для гейта датафрейма.

    Пути ЗАКОННО строят разные типы: дата приезжает epoch-числом
    (clickhouse-connect), Timestamp с зоной и без, с точностью ns/us/ms/s;
    Decimal - то Decimal, то float, то строкой. Наивный хеш по значениям дает
    на этом месте несколько групп хешей при идентичных байтах сервера. Гейт
    обязан сверять СМЫСЛ, а разница типов - находка блока, она видна в
    dtypes-строках лога и в client_dtype режима materialize.
    """
    import math as _math
    if v is None or v is pd.NaT or (isinstance(v, float) and _math.isnan(v)):
        return "\\N"
    if col in ("ts", "d"):
        if isinstance(v, bool):
            return _canon(v)
        if isinstance(v, (int, float)):
            # дата приехала числом эпохи - смысл тот же (данные холста в UTC)
            v = _dt.datetime.fromtimestamp(int(v), _dt.timezone.utc)
        if isinstance(v, str):
            v = v.strip().replace("T", " ")
            return v.split(".")[0] if col == "ts" else v.split(" ")[0]
        ts = pd.Timestamp(v)
        if ts.tzinfo is not None:
            ts = ts.tz_convert("UTC").tz_localize(None)
        py = ts.to_pydatetime().replace(microsecond=0)
        if col == "d":
            return py.date().isoformat()
        return py.isoformat(sep=" ")
    if col == "dec":
        try:
            if isinstance(v, float):
                return _canon_decimal(Decimal(repr(v)))
            if isinstance(v, str):
                return _canon_decimal(Decimal(v))
        except Exception:
            return _canon(v)
    return _canon(v)


def _roundtrips_int(sample) -> bool:
    """Строки колонки - целые БЕЗ потери формы: str(int(x)) == x. Ловит
    зачищенные нулями строки (s8/s16 холста gen): int их прочитает, но
    обратно "00001234" уже не соберет - такая колонка остается строковой,
    иначе восстановление само стало бы деградацией."""
    try:
        return all(str(int(v)) == v for v in sample)
    except (TypeError, ValueError):
        return False


def restore_types(df):
    """Доводка mat-untyped датафрейма до типизированного (ось Д2).

    Текстовые форматы (CSV, TSV, JSON с кавычками, pg-эмуляция) довозят
    значения строками; аналитик доплачивает за типы уже в клиенте - эта
    доплата и есть колонка extra2_s. Правила детерминированные, по данным
    колонки, а не по ее имени (профили и таблицы разные):
      целое      -> astype int64 (при NULL - nullable Int64), только если
                    строка восстановима обратно (см. _roundtrips_int);
      десятичное -> Decimal поэлементно: тот же класс, что у типизированных
                    путей, float здесь молча терял бы значение (g1decbound);
      дата/время -> pd.to_datetime;
      остальное  -> остается строкой.
    Возвращает (df, секунды). Сверка результата с типизированным путем -
    гейтом датафрейма: клетка Д2 гоняется и в BENCH_MODE=dfgate.
    """
    t1 = time.perf_counter()
    for col in df.columns:
        series = df[col]
        if series.dtype != object:
            continue
        sample = series.dropna().head(1000)
        if sample.empty or not isinstance(sample.iloc[0], str):
            continue
        if _roundtrips_int(sample):
            df[col] = series.astype("Int64" if series.isna().any()
                                    else "int64")
            continue
        try:
            # тот же принцип восстановимости формы, что у целых: "00012"
            # разобрался бы Decimal-ом, но обратно уже не собирается -
            # значит колонка строковая, а не десятичная
            if all(str(Decimal(v)) == v for v in sample):
                df[col] = series.map(
                    lambda v: Decimal(v) if isinstance(v, str) else v)
                continue
        except Exception:
            pass
        try:
            # проба на выборке отсекает текстовые колонки дешево, полная
            # конверсия по-прежнему строгая (errors по умолчанию raise).
            # UserWarning "could not infer format" глушится: он ожидаем на
            # каждой НЕ-датной строковой колонке, а stdout занят CSV
            import warnings as _warnings
            with _warnings.catch_warnings():
                _warnings.simplefilter("ignore")
                pd.to_datetime(sample, utc=False)
                df[col] = pd.to_datetime(series, utc=False)
        except (ValueError, TypeError):
            continue
    return df, time.perf_counter() - t1


def _finish_df(df, t1, path=""):
    """Единая воронка готового датафрейма (E1): объявленная схема -> хук
    восстановления типов -> holder.

    extra_s - сборка датафрейма путем (от t1), extra2_s - приведение к
    объявленной схеме BENCH_DF_SCHEMA (внутри окна wall: потребитель платит
    за это в том же вызове). client_dtype - типы, которые клетка довезла
    ПОСЛЕ приведения. Не привести к схеме - клетка падает классом df_schema,
    а не отдает молча другой объект под общей меткой."""
    rec = {"rows": len(df), "extra_s": time.perf_counter() - t1}
    if DF_SCHEMA:
        try:
            df, norm_s = normalize_df(df, DF_SCHEMA)
        except DfSchemaError as exc:
            raise PathExit("df_schema", f"{path}: {exc}") from exc
        rec["extra2_s"] = norm_s
    rec["client_dtype"] = _frame_dtypes(df)
    return _maybe_retained(_maybe_restore(rec, df), df, "df")


def _finish_polars(frame, t1, path=""):
    """Единая воронка готового фрейма polars - зеркало _finish_df.

    Колонки те же и значат то же: extra_s - сборка фрейма путем (от t1),
    extra2_s - приведение к объявленной схеме ВНУТРИ окна wall, client_dtype -
    типы ПОСЛЕ приведения. Без этой воронки клетка polars не приводила фрейм
    вовсе, и ось целевой структуры сравнивала бы плечо с приведением против
    плеча без него: на w105/10m это разница в 50 секунд, то есть весь
    измеряемый эффект целиком.
    """
    rec = {"rows": len(frame), "extra_s": time.perf_counter() - t1}
    if DF_SCHEMA:
        try:
            frame, norm_s = normalize_polars(frame, DF_SCHEMA)
        except DfSchemaError as exc:
            raise PathExit("df_schema", f"{path}: {exc}") from exc
        rec["extra2_s"] = norm_s
    rec["client_dtype"] = _frame_dtypes(frame)
    # D52: фактический пул потоков polars - в строку сырья. Ключ служебный,
    # _measured_exec переносит его в proto_mode как polars_threads=N: колонка
    # proto_mode - аннотация пути, ее никто не сверяет на равенство
    try:
        rec["polars_threads"] = int(pl.thread_pool_size())
    except Exception:                                      # noqa: BLE001
        pass
    return _maybe_retained(rec, frame, "polars")


def _maybe_restore(rec: dict, df) -> dict:
    """Хук BENCH_RESTORE_TYPES=1 для веток, собравших датафрейм: время
    восстановления - в extra2_s, фактические типы в client_dtype - уже
    ПОСЛЕ восстановления (клетка отчитывается тем, что довезла)."""
    if not RESTORE_TYPES:
        return rec
    df, restore_s = restore_types(df)
    rec["extra2_s"] = restore_s
    rec["client_dtype"] = _frame_dtypes(df)
    return rec


def _retained_mb(struct, kind: str):
    """Вес структуры по ЕЕ СОБСТВЕННОЙ версии (схема 11.9): у датафрейма -
    memory_usage(deep), у Arrow - nbytes, у polars - estimated_size, у numpy -
    nbytes массивов (массив, список или словарь массивов); кортежи и
    колоночная выдача из объектов питона своей бухгалтерии не ведут - оценка
    по выборке getsizeof. Разница rss_settled - retained и есть цена
    аллокатора пути."""
    try:
        if kind == "raw":
            return len(struct) / 2**20
        if kind == "df":
            return float(struct.memory_usage(deep=True).sum()) / 2**20
        if kind == "arrow":
            return struct.nbytes / 2**20
        if kind == "polars":
            return struct.estimated_size() / 2**20
        if kind == "np":
            # ndarray, список массивов (clickhouse-driver columnar) или
            # словарь массивов (duckdb fetchnumpy) - сумма nbytes буферов
            if hasattr(struct, "nbytes"):
                return struct.nbytes / 2**20
            arrays = struct.values() if isinstance(struct, dict) else struct
            return sum(a.nbytes for a in arrays) / 2**20
        if kind == "columnar":
            # список колонок из объектов питона: у колонки бухгалтерии нет -
            # оценка по выборке, как у кортежей, только поколоночно
            total = float(sys.getsizeof(struct))
            for col in struct:
                n = len(col)
                if not n:
                    continue
                sample = col[:1000]
                per_val = sum(sys.getsizeof(v) for v in sample) / len(sample)
                total += per_val * n + sys.getsizeof(col)
            return total / 2**20
        if kind == "tuples":
            n = len(struct)
            if not n:
                return 0.0
            sample = struct[:1000]
            per_row = sum(
                sys.getsizeof(r) + sum(sys.getsizeof(v) for v in r)
                for r in sample) / len(sample)
            return (per_row * n + sys.getsizeof(struct)) / 2**20
    except Exception as exc:
        print(f"# retained: не посчитан ({type(exc).__name__})",
              file=sys.stderr)
    return None


def _maybe_retained(rec: dict, struct, kind: str) -> dict:
    """Единая воронка materialize-возвратов: holder плюс хук BENCH_RETAINED=1.

    F07: ПЕРВОЕ, что здесь происходит, - структура уходит в holder обвязки
    (keep_alive). До этого среза последняя ссылка на кортежи и датафреймы
    умирала на возврате из пути, то есть освобождение попадало внутрь wall у
    python и не попадало у Java (там held жив до записи строки), и
    кросс-языковая пара сравнивала два разных контракта. Теперь граница одна:
    структура жива в момент stop, цена освобождения - колонка cleanup_s.

    Дальше - хук BENCH_RETAINED=1 (пересъемка Д6): retained_mb по версии
    структуры плюс rss_settled_mb при ЖИВОЙ структуре. Обвязка _measure покой
    не меряет вовсе, так что обе колонки заполняются ТОЛЬКО отсюда. Оба
    замера идут внутри окна wall (в кортежном контракте другого места нет),
    поэтому wall клеток Д6 не цитируется - клетки существуют ради колонок
    памяти.

    Режимы digest и dfgate через воронку НЕ идут сознательно: у них другой
    контракт - в зачет идет хеш или вердикт гейта, а не доставленная
    потребителю структура, и holder добавил бы к их окну цену освобождения
    промежуточного объекта, которого в замерной клетке нет.
    """
    keep_alive(struct)
    if not RETAINED:
        return rec
    value = _retained_mb(struct, kind)
    if value is not None:
        rec["retained_mb"] = value
    settled = settled_rss_mb()
    if settled is not None:
        rec["rss_settled_mb"] = settled
    return rec


def _raw_result(blob, t1, rows=None, rows_src="expected"):
    """Единая воронка сырого потока (COPY, HTTP FORMAT в байтах, файл): F07
    для целевой структуры raw.

    До этого среза буфер сырого потока был локальной переменной пути:
    последняя ссылка умирала на возврате, освобождение попадало ВНУТРЬ окна,
    holder оставался пуст, и колонка cleanup_s у всех клеток raw была пустой
    (независимое ревью 2026-09-13, P1-3; сырье G3 снято с этим дефектом -
    F44). Теперь буфер живет до stop, как кортежи и датафреймы соседей.

    rows_src - откуда взято число строк: counted (посчитано по потоку),
    lines (посчитаны переводы строк, у CSV с кавычками это оценка сверху),
    expected (взято из холста: строки в потоке не различимы без разбора).
    Источник пишется в client_dtype, а не в отдельную колонку: схема сырья
    не меняется, а в таблицах видно, чему верить."""
    rec = {"rows": _expected_rows() if rows is None else rows,
           "extra_s": time.perf_counter() - t1,
           "client_dtype": f"raw:{len(blob)}B:rows_{rows_src}"}
    return _maybe_retained(rec, blob, "raw")


def _frame_digest(frame, kind: str) -> dict:
    """Digest КОНЕЧНОГО фрейма клетки до датафрейма (D52, ревью п.5 и 10.3).

    Сверка значений для целей df и polars идет не по кортежам или Arrow
    пути, а по тому объекту, который клетка отдает потребителю: фрейм
    приводится к объявленной схеме той же воронкой, что в замере,
    переводится в Arrow и канонизируется кодеком по логической схеме
    таблицы. Совпадение с эталоном таблицы (посчитан из parquet раздачи,
    независимо от любого пути) и есть доказательство, что pandas и polars
    довезли те же значения, а не только те же байты и число строк.
    """
    if kind == "polars":
        frame, _ = normalize_polars(frame, DF_SCHEMA)
        table = frame.to_arrow()
    else:
        frame, _ = normalize_df(frame, DF_SCHEMA)
        table = pa.Table.from_pandas(frame, preserve_index=False)
    return {"rows": table.num_rows, "digest": digest_arrow(table),
            "codec_id": _codec.CODEC_ID,
            "schema_sig": _codec.schema_signature_arrow(table),
            "client_dtype": f"digest_of:{kind}:" + ";".join(
                str(x) for x in frame.dtypes)}


def _rows_result(rows, columns=None, path=""):
    """Разбор режима для путей, отдающих список кортежей."""
    if MODE == "digest" and TARGET in ("df", "polars") and DF_SCHEMA:
        if TARGET == "polars":
            return _frame_digest(_polars_frame(rows, columns), "polars")
        return _frame_digest(pd.DataFrame(rows, columns=columns), "df")
    if MODE == "digest":
        return {"rows": len(rows), "digest": digest_rows(rows),
                "codec_id": _codec.CODEC_ID,
                "schema_sig": _codec.schema_signature_tuples(
                    rows, names=columns),
                "client_dtype": _dtypes_first(rows)}
    if MODE == "dfgate":
        t1 = time.perf_counter()
        df = pd.DataFrame(rows, columns=columns)
        build_s = time.perf_counter() - t1
        if RESTORE_TYPES:
            # гейт сверяет ВОССТАНОВЛЕННЫЙ фрейм с типизированным путем -
            # в этом и смысл сверки Д2
            df, restore_s = restore_types(df)
            rec = _dfgate(df, build_s)
            rec["extra2_s"] = restore_s
            return rec
        return _dfgate(df, build_s)
    if TARGET == "tuples":
        return _maybe_retained(
            {"rows": len(rows), "client_dtype": _dtypes_first(rows)},
            rows, "tuples")
    if TARGET == "df":
        t1 = time.perf_counter()
        return _finish_df(pd.DataFrame(rows, columns=columns), t1, path)
    if TARGET == "polars":
        # сюда попадают только пути, у которых соединения на руках нет
        # (clickhouse-driver) или у которых своя ось в самом курсоре
        # (pg_server_cursor, pg_execs): отдать такое соединение
        # pl.read_database значило бы подменить измеряемую механику
        t1 = time.perf_counter()
        return _finish_polars(_polars_frame(rows, columns), t1, path)
    raise SystemExit(
        f"BENCH_TARGET_STRUCT={TARGET}: путь {path} отдает питоновские строки, "
        "из них законны tuples, df и polars. Колоночную выдачу, Arrow, np и "
        "df_arrowdtype этот интерфейс не строит: получилась бы конверсия уже "
        "готовых объектов питона, а ось меряет МЕСТО СБОРКИ датафрейма")


def _arrow_to_tuples(table):
    """Arrow-таблица -> список питоновских кортежей (ось BENCH_ARROW_TO_ROWS).

    F11 (PATH-02). Два маршрута одного результата:
      pylist  - как в серии F: to_pylist строит СЛОВАРЬ на каждую строку и
                только потом из него берутся значения. Словарный слой стоит
                около 300 МиБ пика на миллионе строк g10 и около 16% времени
                конверсии, и оба числа приписывались формату Arrow;
      colwise - те же значения поколоночно: каждая колонка разворачивается
                один раз, кортежи собирает zip. Значения побитово те же
                (проверено на NULL, Decimal, date, timestamp с зоной и на
                многочанковой таблице), поэтому digest не сдвигается.
    Дефолт pylist держит мост с F: четыре клетки a1-fmt-*-mat обязаны
    сойтись с корпусом, честная цена снимается новыми клетками -colwise.

    Побочно у колоночного маршрута нет ловушки словаря: одноименные колонки
    в to_pylist схлопнулись бы в один ключ, в zip - нет.
    """
    if ARROW_TO_ROWS == "colwise":
        return list(zip(*[col.to_pylist() for col in table.columns]))
    return [tuple(r.values()) for r in table.to_pylist()]


def _arrow_result(table, path=""):
    """Разбор режима для путей, отдающих Arrow-таблицу."""
    if MODE == "digest":
        # срез C1: Arrow-выдача сверяется ВЕКТОРНОЙ реализацией кодека, без
        # разбора в питоновские кортежи. Прежний код строил из таблицы
        # список кортежей ради того же хеша: на 10M x 105 это десятки
        # гигабайт объектов и время, в окно сессии не помещающееся
        if TARGET in ("df", "polars") and DF_SCHEMA:
            if TARGET == "polars":
                return _frame_digest(_polars_frame(table), "polars")
            return _frame_digest(table.to_pandas(), "df")
        return {"rows": table.num_rows, "digest": digest_arrow(table),
                "codec_id": _codec.CODEC_ID,
                "schema_sig": _codec.schema_signature_arrow(table),
                "client_dtype": _note_opaque(
                    ";".join(str(f.type) for f in table.schema))}
    if MODE == "dfgate":
        t1 = time.perf_counter()
        df = table.to_pandas()
        return _dfgate(df, time.perf_counter() - t1)
    if TARGET == "arrow":
        # Подпись схемы результата пишется у ЛЮБОЙ Arrow-клетки, а не только
        # в режиме сверки: численная пара публикуется лишь при одинаковой
        # ФАКТИЧЕСКОЙ схеме обоих плеч (имена, типы, nullability, единица
        # времени, зона), и проверять это надо на каждом объеме, а не на том
        # одном, где случилась сверка.
        return _maybe_retained(
            {"rows": table.num_rows,
             "schema_sig": _codec.schema_signature_arrow(table),
             "client_dtype": _note_opaque(
                 ";".join(str(f.type) for f in table.schema))},
            table, "arrow")
    if TARGET in ("df", "df_arrowdtype"):
        t1 = time.perf_counter()
        # df_arrowdtype - тот же провод и тот же Arrow, но pandas оставляет
        # буферы Arrow и не копирует их в numpy. Это обязательная ТРЕТЬЯ точка
        # оси: без нее вывод звучит как "конверсия дорогая", с ней - как
        # "дорога не конверсия, дорог выбор целевого типа"
        df = table.to_pandas(types_mapper=pd.ArrowDtype) \
            if TARGET == "df_arrowdtype" else table.to_pandas()
        return _finish_df(df, t1, path)
    if TARGET == "polars":
        # вторая физика имени polars тут не нужна: данные уже в Arrow, и
        # pl.from_arrow забирает те же буферы без копии
        t1 = time.perf_counter()
        return _finish_polars(_polars_frame(table), t1, path)
    if TARGET == "tuples":
        t1 = time.perf_counter()
        rows = _arrow_to_tuples(table)
        return _maybe_retained(
            {"rows": len(rows), "extra_s": time.perf_counter() - t1,
             "client_dtype": _dtypes_first(rows)}, rows, "tuples")
    raise SystemExit(f"BENCH_TARGET_STRUCT={TARGET} у пути {path} не бывает")


# =========================================================================
# СЕКЦИЯ 4. Подключения PostgreSQL и pg-эмуляции
# =========================================================================

def _pg_password() -> str:
    """Пароль PostgreSQL. Реализация одна и живет в _session - оттуда ее
    зовет и гейт сессии раннера; здесь остается прежнее имя для путей."""
    return _session.pg_password()


def _assert_pg_codec() -> None:
    """Пустая клетка по построению: сжатия в pg-wire нет ни у одного клиента,
    потому что его нет в протоколе (находка идет на слайд)."""
    if CODEC != "none":
        raise SystemExit(
            f"BENCH_CODEC={CODEC}: сжатия в pg-wire нет ни у одного клиента - "
            "его нет в протоколе. Это пустая клетка, а не сбой замера")


def _pg_session(conn):
    """Настройки сессии по контракту - ОДНИМ набором PG_SESSION_SETTINGS.

    F02 (срез stand-g-v1.0): механика переехала в _session.pg_connect, чтобы
    гейт сессии раннера открывал соединение ТОЙ ЖЕ фабрикой, что и замер.
    Здесь остается прежнее имя для вызовов мимо сетки (пол провода, c1_run,
    служебные пробы), которые получили соединение сами.

    F8 (DATA-02, PATH-01, PGSESS-01). До среза v1.5 набор был разложен по
    трем местам: две настройки в _srvcontrol, зона времени и синхронные сканы
    здесь, и синхронные сканы - только на холстах hits. Из-за этого клиенты
    расходились между собой (ADBC и connectorx не получали вообще ничего), а
    psycopg получал разный набор на разных холстах. Теперь источник один:
    pg_prepare_session ставит ВСЕ четыре ключа и читает их обратно в паспорт,
    а клиенты без SET получают ту же строку стартовыми ключами libpq
    (pg_startup_options).

    Почему synchronize_seqscans выключен везде, а не только на hits: с
    синхронными сканами LIMIT без ORDER BY читает разный миллион строк от
    раунда к раунду (в серии F bytes_rx анкера 1M плавали на 7-12%). Условие
    по холсту делало бы набор у psycopg и у ADBC разным на gen и narrow -
    это новая асимметрия на 2227 строках корпуса.
    """
    pg_prepare_session(conn)
    if not conn.autocommit:
        conn.commit()
    return conn


def _pg_connect():
    """Соединение psycopg по контракту сессии - ЕДИНОЙ фабрикой _session.

    F02: фабрика теперь одна на замер и на гейт. Порядок сохранен дословно:
    connect, затем ось приемного буфера сокета (C7/RS-02: до первого запроса
    и тем более до первого чтения ответа), затем SET контракта и сверка
    прочитанных с сервера значений с ожидаемыми - расхождение роняет клетку
    классом session:<ключ>, а не уезжает в корпус молча.
    """
    _assert_pg_codec()
    return _session.pg_connect(
        "psycopg", canvas=CANVAS, verify=True,
        on_connect=lambda conn: apply_sock_rcvbuf(
            conn, os.environ.get("BENCH_PATH_KIND", "pg")))


def _pg_emu_connect():
    _assert_pg_codec()
    # sslmode=disable обязателен: на SSLRequest эмуляция не отвечает.
    # Настройки сессии тут НЕ выставляем: на том конце ClickHouse, он про jit
    # и TimeZone постгресовым SET не отвечает.
    conn = psycopg.connect(
        host=os.environ.get("CH_EMU_HOST", CH["host"]),
        port=int(os.environ.get("CH_EMU_PORT", "9005")),
        user=CH["user"], password=CH["password"], dbname=CH["db"],
        sslmode="disable",
    )
    # C7 (RS-02): клиент тут тот же psycopg, ось на нем работает так же -
    # эмуляция отличается только тем, кто на том конце провода
    apply_sock_rcvbuf(conn, os.environ.get("BENCH_PATH_KIND", "ch_pg_emu"))
    return conn


# =========================================================================
# СЕКЦИЯ 5. Подключения ClickHouse и план сжатия
# =========================================================================

# План сжатия считается ОДИН раз из единой таблицы _srvcontrol - той же,
# которую валидирует гейт кодеков раннера. Контракт: при непустом кодеке
# двух слоев сразу быть не должно.
# HC3: слой выбирает ОСЬ, а не формат. До среза v1.5 у Arrow, ArrowStream и
# Parquet кодек молча уходил внутрь формата, а у всех остальных форматов - на
# транспорт, и клетка "цена сжатия" отвечала сразу на два вопроса. Дефолт
# transport общий для всех форматов, форматный слой берется явно и назван в
# метке клетки.
CODEC_PLAN = ch_codec_plan(FMT, CODEC, CH_CODEC_LAYER)

# Уровень живет ровно на одном слое - транспортном. У сжатия ВНУТРИ формата
# (Arrow, ArrowStream, Parquet) свои настройки метода, и до них
# http_zlib_compression_level не доезжает: клетка с уровнем в extra_env
# снялась бы дефолтом библиотеки, а метка обещала бы точку оси. Молчаливый
# холостой ход дороже падения: окно короткое, второго прогона не будет.
if CODEC_LEVEL_SET and CODEC_PLAN["layer"] != "transport":
    raise SystemExit(
        f"BENCH_CH_CODEC_LEVEL задан, а сжатие клетки живет на слое "
        f"{CODEC_PLAN['layer']} (codec={CODEC}, формат {FMT or '-'}) - "
        "уровень туда не применяется. Ось уровня снимается только на "
        "транспортном сжатии HTTP: Native, JSON и прочие форматы без "
        "собственного сжатия")


def _codec_layer() -> str:
    """Где именно включено сжатие: none, транспорт (wire) или формат."""
    return {"none": "none", "format": "format",
            "transport": "wire"}[CODEC_PLAN["layer"]]


def _wire_codec() -> str:
    """Кодек транспортного слоя (пустая строка, если провод не сжимается).

    Именно это значение уходит в Accept-Encoding и ровно его сервер обязан
    вернуть в Content-Encoding - у CH имя кодека на проводе совпадает с
    именем оси (в отличие от форматного слоя, где lz4 у Arrow это lz4_frame).
    """
    return CODEC if _codec_layer() == "wire" else ""


def _ch_settings(http: bool = True) -> dict:
    """Серверные настройки, общие для всех путей CH.

    Прибиваются ЯВНО (контракт) - иначе представление задается дефолтом
    сборки, а не замером:
      - output_format_parallel_formatting из BENCH_PARALLEL_FMT. По умолчанию
        на сервере 1, и тогда смена одного слова FORMAT молча меняет число
        серверных потоков (построчные текстовые его поддерживают, Native, Arrow
        и Parquet - нет). Константа вместо переменной делала бы контрольную
        пару 0 против 1 двумя одинаковыми замерами;
      - string_as_string у Arrow и Parquet: иначе строки уезжают в binary и
        сравнивать байты с текстовыми форматами нельзя;
      - quote_decimals и quote_64bit_integers у JSON: дефолты у них разные, и
        без явного значения JSON-клетки меряют не формат, а сборку сервера;
      - session_timezone='UTC' - вторая половина пары к timestamptz у PG.
    """
    settings = {
        "output_format_parallel_formatting": int(PARALLEL_FMT),
        "output_format_arrow_string_as_string": 1,
        "output_format_parquet_string_as_string": 1,
        "output_format_json_quote_decimals": 1,
        "output_format_json_quote_64bit_integers": 1,
        "session_timezone": "UTC",
    }
    if MAX_THREADS:
        settings["max_threads"] = int(MAX_THREADS)
    if BATCH_SET:
        # ось размера порции: то же имя ручки, что itersize и fetchSize
        settings["max_block_size"] = BATCH

    # настройки кодеков - из единого плана: сначала все форматные методы
    # гасятся в none, потом включается ровно один (или ни одного)
    settings.update(CODEC_PLAN["settings"])
    if http:
        settings["enable_http_compression"] = 1 if CODEC_PLAN["transport"] else 0
        if CODEC_PLAN["transport"]:
            # Уровень транспортного кодека прибивается так же явно, как все
            # остальное: без него цифру "сжатие экономит столько-то" задает
            # дефолт сборки, и remote-контур может считать ее по своему
            # профилю - пара remote/self перестанет быть сравнимой.
            # Значение берется из ОСИ (BENCH_CH_CODEC_LEVEL), а не из
            # константы: ручка одна на все методы, и для кадрового LZ4
            # уровень >= 3 означает LZ4HC. Дефолт оси - 3, чтобы уже снятые
            # клетки корпуса воспроизводились байт в байт.
            settings["http_zlib_compression_level"] = CODEC_LEVEL
    elif CODEC_LEVEL_SET:
        # Нативный провод сжимает своими ручками (network_compression_method,
        # network_zstd_compression_level), http_zlib_compression_level он не
        # смотрит. Клетка снялась бы дефолтом под меткой с уровнем.
        raise SystemExit(
            "BENCH_CH_CODEC_LEVEL задан на нативном проводе: "
            "http_zlib_compression_level - ручка HTTP-слоя, у нативного "
            "протокола свои; ось уровня на TCP снимается не этой переменной")
    return settings


def _assert_ch_codec(obj, attr: str, settings: dict, headers=None) -> None:
    """Сжатие живет на четырех уровнях сразу - Accept-Encoding (compress у
    клиента), enable_http_compression на сервере и два внутриформатных метода.
    Проверяем, что включен РОВНО ТОТ, который мы назвали, а остальные сняты -
    иначе в CSV уедет цифра со сжатого провода под меткой none или двойное
    сжатие под меткой одного кодека.

    Атрибут драйвера - НАМЕРЕНИЕ, а не факт: у clickhouse-connect compress
    выставляется всегда, когда сервер разрешает enable_http_compression, но
    заголовок Accept-Encoding ставит только путь query/query_df. Поэтому у
    HTTP-клиента проверяется еще и словарь заголовков, а окончательная улика -
    Content-Encoding в ответе сервера (_assert_ch_wire на каждом raw-запросе).
    Ассерт по одному атрибуту дал зеленый на восьми клетках A6 в ночь
    2026-09-02, где сжатия на проводе не было вовсе.

    Отдельно ловим случай, когда драйвер переименовал атрибут: молча
    ослепший ассерт хуже отсутствующего.
    """
    probe = object()
    value = getattr(obj, attr, probe)
    if value is probe:
        raise SystemExit(
            f"драйвер не отдает атрибут {attr} - ассерт сжатия ослеп; "
            "чинить надо ассерт, а не замер")
    layer = _codec_layer()
    fmt_keys = ("output_format_arrow_compression_method",
                "output_format_parquet_compression_method")
    accept = ""
    if headers is not None:
        accept = str(headers.get("Accept-Encoding", "")).strip().lower()

    if layer == "none":
        if value:
            raise SystemExit(f"запрошен codec=none, а драйвер сжимает: {value!r}")
        if accept:
            raise SystemExit(
                f"запрошен codec=none, а клиент просит Accept-Encoding="
                f"{accept} - none-пара перестала быть точкой отсчета")
        for key in ("enable_http_compression",) + fmt_keys:
            if key in settings and str(settings[key]) not in ("0", "none"):
                raise SystemExit(f"запрошен codec=none, а {key}={settings[key]!r}")
        return
    if layer == "wire":
        if not value:
            raise SystemExit(f"запрошен codec={CODEC}, а драйвер сжатие не включил")
        if headers is not None:  # HTTP: заголовок и серверная ручка - пара
            if accept != CODEC:
                raise SystemExit(
                    f"запрошен codec={CODEC} на транспорте, а в заголовках "
                    f"клиента Accept-Encoding={accept or '(нет)'} - "
                    "raw_stream и raw_query ушли бы на провод без сжатия")
            if str(settings.get("enable_http_compression", "")) != "1":
                raise SystemExit(
                    f"codec={CODEC} на транспорте, а enable_http_compression="
                    f"{settings.get('enable_http_compression')!r} - сервер "
                    "сжимать не будет, одного заголовка мало")
            # уровень - такая же ось, как кодек: клетка с уровнем 1 в метке и
            # уровнем 3 в настройках мерила бы LZ4HC под именем обычного lz4
            level = settings.get("http_zlib_compression_level")
            if str(level) != str(CODEC_LEVEL):
                raise SystemExit(
                    f"codec={CODEC} на транспорте, а уровень в настройках "
                    f"{level!r} против оси {CODEC_LEVEL} - клетка снялась бы "
                    "не на своей точке оси")
        for key in fmt_keys:  # два слоя сразу запрещены контрактом
            if str(settings.get(key, "none")) != "none":
                raise SystemExit(
                    f"codec={CODEC} на транспорте, а {key}={settings[key]!r} - "
                    "это два слоя сжатия сразу, цифра не значит ничего")
        return
    if value or accept:  # layer == "format"
        raise SystemExit(
            f"codec={CODEC} задан внутри формата {FMT}, а транспорт сжимает "
            f"тоже ({(value or accept)!r}) - два слоя сразу запрещены")


def _ch_set_accept_encoding(client) -> dict:
    """Прибить Accept-Encoding в заголовки САМОГО клиента и вернуть их.

    clickhouse-connect ставит заголовок только внутри query/query_df, а
    raw_query и raw_stream уходят с одними клиентскими заголовками. Ось A6 -
    целиком drain, то есть целиком raw_stream: ночь 2026-09-02 отдала восемь
    клеток, где compress у клиента стоял, а на проводе байты были ровно как у
    none-пары. Кладем заголовок туда, откуда его берет ЛЮБОЙ запрос клиента.

    Словарь заголовков в 0.8 - обычный атрибут, в 1.x - свойство поверх
    бэкенда, поэтому пишем через присваивание и тут же перечитываем: молча не
    принятый заголовок вернул бы ровно тот же ложный зеленый.
    """
    codec = _wire_codec()
    if not codec:
        # none-клетка обязана уйти БЕЗ заголовка: иначе сервер сожмет ответ и
        # точка отсчета по байтам съедет
        return dict(getattr(client, "headers", None) or {})
    headers = dict(getattr(client, "headers", None) or {})
    headers["Accept-Encoding"] = codec
    client.headers = headers
    actual = dict(getattr(client, "headers", None) or {})
    if actual.get("Accept-Encoding") != codec:
        raise SystemExit(
            "клиент clickhouse-connect не принял заголовок Accept-Encoding - "
            "чинить надо этот код, а не замер")
    return actual


def _assert_ch_wire(response) -> None:
    """Факт транспортного сжатия берется из ОТВЕТА сервера.

    Content-Encoding - единственная улика, доступная внутри самой клетки:
    заголовок ушел, сервер его принял, ответ сжат. Сравнение байтов сжатой
    клетки с none-парой живет в сводке (гейт кодеков summarize) - там есть обе
    клетки, здесь только одна, и дублировать это тут нечем.
    """
    want = _wire_codec()
    got = _http_encoding(response)
    if want and got != want:
        raise SystemExit(
            f"codec={CODEC} на транспорте, а сервер ответил Content-Encoding="
            f"{got or '(нет)'} - сжатия на проводе не было, "
            "клетка недействительна")
    if not want and got:
        raise SystemExit(
            f"codec={CODEC}, а сервер сжал ответ ({got}) - "
            "байты клетки не про тот слой")


def _ch_raw_stream(client, sql, fmt=None, settings=None):
    """raw_stream плюс проверка факта сжатия по ответу.

    Ответ не трогаем: распаковка идет потоком в _byte_blocks, чтобы клетка
    доставки платила за нее там же, где принимает байты.
    """
    stream = client.raw_stream(sql, fmt=fmt, settings=settings)
    try:
        _assert_ch_wire(stream)
    except BaseException:
        _close(stream)
        raise
    return stream


def _ch_raw_query(client, sql, fmt=None, settings=None) -> bytes:
    """raw_query с распаковкой транспортного кодека.

    Без сжатия - ровно прежний вызов драйвера, байт в байт как в снятых
    сериях. Со сжатием прежний путь вернул бы сжатый блоб: urllib3 про lz4 не
    знает, а объект ответа raw_query не отдает, и проверить Content-Encoding
    было бы негде - поэтому там же идем потоком.
    """
    if not _wire_codec():
        return client.raw_query(sql, fmt=fmt, settings=settings)
    stream = _ch_raw_stream(client, sql, fmt=fmt, settings=settings)
    try:
        return b"".join(_byte_blocks(stream))
    finally:
        _close(stream)


def _ch_connect_http():
    settings = _ch_settings()
    client = clickhouse_connect.get_client(
        host=CH["host"], port=CH["http_port"],
        username=CH["user"], password=CH["password"], database=CH["db"],
        compress=(CODEC if _codec_layer() == "wire" else False),
        settings=settings,
    )
    headers = _ch_set_accept_encoding(client)
    _assert_ch_codec(client, "compression", settings, headers=headers)
    # паспорт клетки: слой и точка оси уровня. Колонки под уровень в схеме CSV
    # нет (схема заморожена), в корпус он попадает меткой клетки - а здесь
    # остается улика в логе полосы, по которой метку можно перепроверить
    print(f"# паспорт клетки: codec={CODEC} слой={_codec_layer()} уровень="
          f"{CODEC_LEVEL if CODEC_PLAN['transport'] else '-'}",
          file=sys.stderr)
    return client


def _ch_connect_native(use_numpy: bool = False):
    settings = _ch_settings(http=False)
    if use_numpy:
        # Класс результата clickhouse-driver (QueryResult против
        # NumpyQueryResult) выбирается ОДИН раз - в конструкторе Client, по
        # use_numpy из ЕГО settings. use_numpy в settings запроса меняет
        # только чтение колонок из блока, а QueryResult потом сворачивает
        # numpy-массивы обратно в кортежи. В серии G клетка ch_native/np так
        # и уехала с client_dtype=tuple;tuple;... - метка обещала numpy, а
        # структура была кортежной. Ключ просит только клетка np: остальные
        # клетки ch_native получают прежний клиент
        settings = dict(settings, use_numpy=True)
    client = CHClient(
        host=CH["host"], port=CH["tcp_port"],
        user=CH["user"], password=CH["password"], database=CH["db"],
        compression=(CODEC if _codec_layer() == "wire" else False),
        settings=settings,
    )
    # F3 (MEAS-04, STAT-02): конструктор CHClient сокета не открывает -
    # clickhouse-driver соединяется лениво, первым запросом. Из-за этого окно
    # коннекта закрывалось ДО рукопожатия, и в CSV уезжал измеренный ноль
    # (309 строк ровно 0.0000 в 64 клетках серии F), а рукопожатие пряталось
    # в первом исполнении. force_connect гоняет ровно Hello/HelloResponse -
    # тот же обмен, что и так ехал, только теперь он посчитан там, где
    # происходит.
    # ЛОВУШКА clickhouse-driver 0.2.9 (приемка финала 2026-09-06): force_connect
    # после свежего connect оставляет is_query_executing=True, и первый же
    # execute падает PartiallyConsumedQueryError ("Simultaneous queries on
    # single connection"). Честное рукопожатие дает сам connect(): Hello /
    # HelloResponse, флаг остается снятым - воспроизведено на стенде
    # (force_connect -> True, connect -> False).
    if not client.connection.connected:
        client.connection.connect()
    _assert_ch_codec(client.connection, "compression", settings)
    return client


# =========================================================================
# СЕКЦИЯ 6. Режим srvcost: серверная цена двумя числами
# =========================================================================

def _require_srvcost_label() -> None:
    """Метка кейса обязана содержать слово srvcost (контракт).

    В этом режиме wall_s не меряется вовсе, а rows нулевой - строка, попавшая
    в общий график под обычной меткой, выглядела бы как мгновенный замер нуля
    строк.
    Проверка тут, а не в раннере: раннеров может стать несколько, файл один.
    """
    label = os.environ.get("MEASURE_LABEL", "")
    if "srvcost" not in label:
        raise SystemExit(
            f"BENCH_MODE=srvcost, а MEASURE_LABEL={label!r} - в метке обязано "
            "быть слово srvcost, иначе серверные строки попадут в общие графики")


def _srvcost(engine: str):
    """Обертка режима srvcost.

    Возвращаем СПИСОК из одной записи, и это не украшательство: в списочном
    контракте _measure байты на проводе берутся из самой записи (мы их не
    кладем - колонка остается пустой), а не считаются обвязкой вокруг пути.
    Иначе в bytes_rx_mb уехала бы вычитка system.query_log и EXPLAIN.

    wall_s в списочном контракте обязан быть числом, пустым его положить
    нельзя, поэтому кладем 0.0 - признак "не мерилось" (зафиксировано в
    CONTRACT). Настоящие числа режима лежат в server_time_s, serialize_time_s
    и out_kb.
    """
    _require_srvcost_label()
    if engine == "pg":
        conn = _pg_connect()
        try:
            rec = _srvcost_pg(conn)
        finally:
            _close(conn)
    else:
        rec = _srvcost_ch()
    rec["wall_s"] = 0.0
    # F4: srvcost - отдельный класс строки, а не слив: wall в ней не мерился
    # вовсе, и сравнивать ее с клетками доставки нельзя ни по одной колонке
    _extras(rec, drain_class=drain_class_of("", mode="srvcost"),
            codec_level=_cell_codec_level())
    return [rec]


def _srvcost_pg(conn, _t0=None):
    """Две цифры на сервере: исполнение без сериализации и добавка
    сериализации. На слайд идет неравенство, а не вычитание - в потоковом
    протоколе генерация перекрывается с доставкой, поэтому измеренная
    серверная цена это верхняя граница добавки.

    Цену сериализации берем прямым числом (time= в строке Serialization), а
    не разностью двух прогонов: разность шумит на величину самой разности.
    """
    serialize = {"simple_text": "text", "ext_text": "text", "copy_text": "text",
                 "copy_csv": "text", "ext_binary": "binary",
                 "copy_binary": "binary"}.get(FMT or "simple_text")
    if serialize is None:
        raise SystemExit(
            f"BENCH_FMT={FMT!r} не из набора pg-wire: {PG_FMTS + PG_COPY_FMTS}")
    if CANVAS == "gen":
        # гейт падает сам, если в плане нет ProjectSet, есть Function Scan
        # или ненулевые временные файлы
        pg_check_generator_plan(conn, SQL_PG)
    base = pg_server_cost(conn, SQL_PG, serialize="none")
    full = pg_server_cost(conn, SQL_PG, serialize=serialize)
    ser_s = full.get("serialize_time_s")
    if ser_s is None:
        ser_s = max(full["server_time_s"] - base["server_time_s"], 0.0)
    return {
        "rows": 0,  # EXPLAIN не отдает строк клиенту - это уровень сервера
        "server_time_s": base["server_time_s"],
        "serialize_time_s": ser_s,
        "out_kb": full["out_kb"],
        "client_dtype": "srvcost",
    }


def _srvcost_ch(_conn=None, _t0=None):
    """Две пробы на том же сервере: FORMAT Null (исполнил, но не форматировал
    и не отправил) и боевой FORMAT из BENCH_FMT. Отдельного события
    "сериализация" в ClickHouse нет, поэтому вторая полоска серверной работы
    считается разностью, и на слайд она идет неравенством.

    Боевой формат снимается ПОТОКОМ с выбрасыванием блоков, а не raw_query:
    raw_query втянул бы весь ответ в память замерного процесса, и десять
    миллионов строк в Parquet легли бы в пик RSS замерялки, ничего при этом
    не измеряя.

    Лог добирается ПОСЛЕ обеих проб: SYSTEM FLUSH LOGS и чтение query_log
    сами едут по тому же проводу и внутри окна замера запрещены.
    """
    client = _ch_connect_http()
    try:
        settings = _ch_settings()
        null_cost = ch_server_cost(client, SQL_CH, settings=settings,
                                   collect=False)
        fmt = FMT if FMT in CH_FMTS else "Native"
        qid = new_query_id("bench-fmt")
        opts = ch_query_settings(client, qid, settings)
        stream = _ch_raw_stream(client, f"{ch_marker(qid)} {SQL_CH}",
                                fmt=fmt, settings=opts)
        try:
            wire = sum(len(chunk) for chunk in _byte_blocks(stream))
        finally:
            _close(stream)
        logs = ch_collect_log(client, [null_cost["query_id"], qid])
    finally:
        _close(client)

    null_log = logs.get(null_cost["query_id"]) or {}
    fmt_cost = ch_cost_from_log(logs.get(qid))
    # без строки лога остается запасной вариант ch_server_cost: у FORMAT Null
    # ответа нет, поэтому wall клиента и есть время сервера плюс два конца
    null_s = (null_log.get("query_duration_ms") or 0) / 1000.0 \
        or null_cost["server_time_s"]
    fmt_s = fmt_cost.get("server_time_s") or 0.0
    # out_kb - СТРОГО NetworkSendBytes боевого FORMAT (правило единственного
    # источника живет в ch_cost_from_log, тут оно только берется): на
    # result_bytes полоска "бинарное не значит меньше байт" не держится -
    # он от смены FORMAT почти не меняется. Фолбэк без строки лога - байты,
    # принятые самим потоком (wire). result_bytes - отдельное число сверки
    # объема работы; колонки под него в схеме нет, поэтому оно уезжает в
    # stderr-паспорт клетки строкой ниже
    out_kb = fmt_cost.get("out_kb")
    if out_kb is None and wire:
        out_kb = wire / 1024.0
    print(f"# srvcost ch: net_send_bytes={fmt_cost.get('net_send_bytes')} "
          f"result_bytes={fmt_cost.get('result_bytes')} wire={wire}",
          file=sys.stderr)
    return {
        "rows": 0,
        "server_time_s": null_s,
        "serialize_time_s": max(fmt_s - null_s, 0.0) if fmt_s else None,
        "out_kb": out_kb,
        "client_dtype": f"srvcost:{fmt}",
    }
