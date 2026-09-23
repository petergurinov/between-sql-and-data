#!/usr/bin/env python3
"""Серверный контроль серии STAND-E: где кончается движок и начинается провод.

Зачем модуль. Претензия к прошлым сериям звучит так: "вы померили движок, а не
протокол". Ответ на нее - не оговорка в отчете, а два числа рядом с каждым
замером, которые назвал сам сервер:
    server_time_s    - сервер ИСПОЛНИЛ запрос, но ничего не сформатировал
                       (PG: EXPLAIN ANALYZE SERIALIZE NONE; CH: FORMAT Null);
    out_kb           - сколько байт сервер СФОРМАТИРОВАЛ И ОТДАЛ на выдачу
                       (PG: output= из строки Serialization; CH: ProfileEvents
                       NetworkSendBytes - см. ch_cost_from_log, там же почему
                       не result_bytes).
Все, что осталось между этой полоской и временем пути, движком не объясняется.

Формулировка на сцене - НЕРАВЕНСТВО, а не вычитание (развилка 5 дизайна):
в потоковом протоколе исполнение перекрывается с доставкой, поэтому измеренная
серверная цена - верхняя граница добавки, а не слагаемое.

Публичный API (контракт зафиксирован до написания, менять нельзя):
    pg_server_cost(conn, sql, serialize="none"|"text"|"binary") -> dict
    ch_server_cost(client, sql)                                 -> dict
    sha256_digest(rows)                                         -> str

Вокруг него - то, без чего эти три функции невалидны:
    pg_prepare_session       - настройки сессии, которые надо выставить И
                               записать в паспорт серии;
    pg_startup_options       - тот же контракт сессии строкой стартовых ключей
                               libpq для клиентов, которые SET не делают
                               (pg_adbc, cx_pg, java) - F8;
    pg_check_generator_plan  - предполетный гейт холста генерации (ProjectSet
                               вместо Function Scan, нулевой temp);
    pg_check_plan_profiles   - тот же гейт на РЕАЛЬНОМ тексте запроса каждого
                               зачетного профиля (гейт B контракта);
    pg_timing_overhead       - плата за инструментирование EXPLAIN ANALYZE;
    ch_marker / ch_query_settings / new_query_id / ch_collect_log /
    ch_cost_from_log         - разметка запросов и ОТЛОЖЕННАЯ вычитка
                               system.query_log: SYSTEM FLUSH LOGS внутри окна
                               замера испортил бы и время, и байты на проводе;
    ch_codec_plan / ch_check_codecs - словарь кодеков и предполетная проверка
                               их значений на живом сервере (гейт D контракта);
    check_digests_equal      - сверка sha256 выдачи (гейты A и C контракта);
    check_wide_over_narrow (+ pg_check_serialize_none, ch_check_format_null) -
                               проверка, что режим "не форматировать вывод" не
                               выродился в "ничего не считать".

Ожидания по типам: conn - открытое соединение psycopg 3, client - клиент
clickhouse-connect (нужны методы raw_query / query / command).
"""

import datetime as _dt
import hashlib
import os
import re
import sys
import time
import uuid
from decimal import Decimal
from urllib.parse import quote as _urlquote


class SrvControlError(RuntimeError):
    """Сервер ответил не тем, что мы умеем разобрать."""


class PreflightError(SrvControlError):
    """Предполетный гейт не пройден - серию стартовать нельзя."""


class ModeInvalidError(SrvControlError):
    """Режим серверного контроля выродился: он больше ничего не измеряет."""


# ---------------------------------------------------------------- digest ----

_DIGEST_FS = b"\x1f"  # разделитель значений внутри строки
_DIGEST_RS = b"\x1e"  # разделитель строк


def _canon_decimal(value: Decimal) -> str:
    """Decimal без экспоненты и без хвостовых нулей: 1.500 и 1.5 - одно число.
    normalize() не годится - он превращает 100 в 1E+2."""
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in ("", "-", "-0") else text


def _canon(value) -> str:
    """Каноническая текстовая форма значения.

    Задача - сравнить ВЫДАЧУ двух разных движков, а не их питоновские типы:
    PostgreSQL и ClickHouse отдают одно и то же число разными классами. Поэтому
    хешируется не repr объекта, а нормализованный текст.
    """
    if value is None:
        return "\\N"                      # как NULL в COPY TEXT
    if isinstance(value, bool):           # проверять ДО int: bool - подкласс int
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)                # кратчайшая форма с round-trip
    if isinstance(value, Decimal):
        return _canon_decimal(value)
    if isinstance(value, str):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).decode("utf-8", "backslashreplace")
    if isinstance(value, _dt.datetime):
        # Предполет финала 2026-09-06: на холстах hits/narrow PostgreSQL отдает
        # timestamp без зоны (наивный), а ClickHouse после DDL DateTime('UTC')
        # (F14) - осведомленный, и isoformat расходился хвостом "+00:00" при
        # одинаковых значениях. Данные стенда - UTC по построению (гейт дат,
        # TimeZone=UTC у сессий), поэтому наивное время читается как UTC, а
        # осведомленное приводится к UTC: одинаковый момент - одинаковый текст.
        # Пины серии F не меняются: на холсте gen оба движка отдавали
        # осведомленное UTC, его текст остается "...+00:00".
        if value.tzinfo is None:
            value = value.replace(tzinfo=_dt.timezone.utc)
        else:
            value = value.astimezone(_dt.timezone.utc)
        return value.isoformat()
    if isinstance(value, (_dt.date, _dt.time)):
        return value.isoformat()
    if isinstance(value, dict):
        return "{" + ",".join(f"{k}={_canon(value[k])}"
                              for k in sorted(value, key=str)) + "}"
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(_canon(v) for v in value) + "]"
    return str(value)


def sha256_digest(rows) -> str:
    """sha256 канонической выдачи - гейт идентичности данных перед серией.

    Принимает list[tuple], list[list], list[dict], список скаляров и объекты
    pyarrow с to_pylist (Table, RecordBatch). Хеш считается потоково: на 10M
    строк промежуточная склейка в одну строку стоила бы гигабайты.

    ВАЖНО: у dict-строк (JSONEachRow) ключи сортируются, у кортежей порядок
    колонок значащий - то есть кросс-путевое сравнение законно только при
    одинаковом порядке колонок в запросе. Это и есть условие холста A5.
    """
    if hasattr(rows, "to_pylist"):        # pyarrow.Table / RecordBatch
        rows = rows.to_pylist()
    digest = hashlib.sha256()
    for row in rows:
        if isinstance(row, dict):
            cells = [f"{k}={_canon(row[k])}" for k in sorted(row, key=str)]
        elif isinstance(row, (list, tuple)):
            cells = [_canon(v) for v in row]
        else:                             # одноколоночная выдача
            cells = [_canon(row)]
        digest.update(_DIGEST_FS.join(
            c.encode("utf-8", "surrogatepass") for c in cells))
        digest.update(_DIGEST_RS)         # число строк входит в хеш неявно
    return digest.hexdigest()


# ------------------------------------------------------------ PostgreSQL ----

# Выставляются перед КАЖДОЙ пробой и читаются обратно в паспорт серии.
# jit=off - иначе компиляция выражений добавляет к серверному времени случайную
# величину, зависящую от стоимости плана, а не от протокола.
# max_parallel_workers_per_gather=0 - без него число воркеров пляшет от ширины
# профиля, и "серверная доля" перестает быть сопоставимой между точками.
# BENCH_PG_PARALLEL - ось цены самого потолка: по умолчанию потолок стоит
# (0 воркеров), но контрольная клетка может его снять и показать числом то,
# что иначе остается оговоркой "снятие потолка дало бы до 19% wall".
# Значение уходит в ту же сессию, читается обратно и попадает в паспорт -
# то есть клетка не может молча уехать в корпус под чужой меткой.
_PG_PARALLEL = os.environ.get("BENCH_PG_PARALLEL", "0").strip() or "0"
if not _PG_PARALLEL.isdigit():
    raise SystemExit(f"BENCH_PG_PARALLEL={_PG_PARALLEL!r}: ожидается число воркеров")

# F8 (DATA-02, PATH-01, PGSESS-01): jit - такая же ОСЬ, как потолок
# параллелизма, а не константа. В F шестьдесят одна клетка ADBC и connectorx
# уехала при jit=on, потому что эти клиенты шли мимо контракта сессии, и
# объяснить сдвиг было нечем. Ось дает контрольную пару jit on/off на одном
# клиенте. Переменная читается тут же, рядом с BENCH_PG_PARALLEL и по той же
# причине: _srvcontrol не зависит от bench_axes (его импортируют и tools,
# где осевых валидаций нет), а имя переменной - общее по контракту.
_PG_JIT = os.environ.get("BENCH_PG_JIT", "off").strip().lower() or "off"
if _PG_JIT not in ("on", "off"):
    raise SystemExit(f"BENCH_PG_JIT={_PG_JIT!r}: ожидается on или off")

# F8: ЕДИНСТВЕННЫЙ источник контракта сессии PostgreSQL. Из этого же кортежа
# собираются стартовые ключи libpq для клиентов, которые SET не делают
# (pg_adbc, cx_pg) и строка для java - иначе набор снова разъедется по
# клиентам, как в серии F.
#   jit                              - компиляция выражений добавляет к
#       серверному времени величину, зависящую от плана, а не от протокола;
#   max_parallel_workers_per_gather  - без потолка число воркеров пляшет от
#       ширины профиля, и "серверная доля" перестает быть сопоставимой;
#   synchronize_seqscans=off         - на ВСЕХ холстах (PGSESS-01 п.2): при
#       включенных синхронных сканах LIMIT без ORDER BY читает разный
#       миллион строк от раунда к раунду. Условие "только hits" делало бы
#       набор у psycopg и у ADBC разным на gen и narrow - это новая
#       асимметрия на 2227 строках корпуса;
#   TimeZone=UTC                     - колонка ts отдается как timestamptz, и
#       без явной зоны текстовые байты разошлись бы с ClickHouse, у которого
#       зона задана в типе. В F зона PostgreSQL держалась на дефолте образа.
PG_SESSION_SETTINGS = (
    ("jit", _PG_JIT),
    ("max_parallel_workers_per_gather", _PG_PARALLEL),
    ("synchronize_seqscans", "off"),
    ("TimeZone", "UTC"),
)


def pg_startup_options(canvas: str = None) -> str:
    """Контракт сессии строкой стартовых ключей libpq (options=...).

    F8: pg_adbc и cx_pg соединяются мимо _pg_session (SET им никто не делает),
    а pgJDBC до этого среза нес свою копию двух ключей из четырех. Строка
    собирается из ТОГО ЖЕ кортежа PG_SESSION_SETTINGS, поэтому разъехаться
    наборам больше негде.

    canvas принимается для читаемости вызова (клиент называет холст, на
    котором открывает соединение), но на набор не влияет: по PGSESS-01 п.2
    контракт одинаков на всех холстах.
    """
    options = " ".join(f"-c {name}={value}"
                       for name, value in PG_SESSION_SETTINGS)
    _assert_axes_agree(options)
    return options


def _assert_axes_agree(options: str) -> None:
    """Сверка со вторым списком настроек, если он уже поднят в процессе (F8).

    Второй реализации больше нет: bench_axes берет PG_SESSION_SETTINGS
    отсюда (встречный запрос axes круга 2 закрыт), оси BENCH_PG_JIT и
    BENCH_PG_PARALLEL остались там разбором значений. Сверка держится
    сторожем на случай, если копия заведется снова - молчаливое расхождение
    двух списков стоило серии F шестидесяти одной клетки.
    Импортировать bench_axes отсюда нельзя:
    _srvcontrol зовут и гейты раннера, и tools, где осевого окружения нет
    вовсе, а валидации bench_axes падают ПРИ ИМПОРТЕ. Поэтому сверка делается
    по факту: если модуль уже в процессе (все замерные пути его импортируют),
    строки обязаны совпасть. Молчаливое расхождение двух списков - ровно тот
    дефект F8, из-за которого ADBC и connectorx ехали при jit=on рядом с
    psycopg при jit=off.
    """
    axes = sys.modules.get("bench_axes")
    other = getattr(axes, "pg_startup_options", None) if axes else None
    # то же самое имя, реэкспортированное отсюда, - это и есть цель правки:
    # сверять нечего, а звать саму себя нельзя (рекурсия)
    if other is None or other is pg_startup_options:
        return
    theirs = other()
    if theirs != options:
        raise SystemExit(
            "контракт сессии PostgreSQL разошелся между модулями: "
            f"_srvcontrol дает {options!r}, bench_axes - {theirs!r}. Списков "
            "настроек обязан быть один (F8): источник - PG_SESSION_SETTINGS "
            "в _srvcontrol, bench_axes.pg_startup_options должна быть его "
            "оберткой")


def pg_startup_options_uri(canvas: str = None, sep: str = " ") -> str:
    """Та же строка, percent-encoded для параметра options в URI libpq.

    Пробелы и знаки равенства внутри значения параметра обязаны быть
    закодированы, иначе libpq разберет строку как несколько параметров.

    sep - разделитель токенов. Смоук финала 2026-09-06: у connectorx пробел
    (%20) на python-стороне перекодируется в '+', а rust-postgres '+' назад
    не декодирует - в сервер уезжало "-c+jit=off" и он падал на параметре
    "+jit". Бэкенд режет options по любому пробельному символу, поэтому для
    connectorx токены разделяются табуляцией (%09 переживает обе стороны).
    """
    options = pg_startup_options(canvas)
    if sep != " ":
        options = options.replace(" ", sep)
    return _urlquote(options, safe="")

_PG_EXEC_RE = re.compile(r"^\s*Execution Time:\s*([\d.]+)\s*ms", re.M)
_PG_PLANT_RE = re.compile(r"^\s*Planning Time:\s*([\d.]+)\s*ms", re.M)
# Фактический формат строки PG 18 (explain.c): два пробела между полями,
# kB вплотную к числу:
#   "Serialization: time=1.234 ms  output=5678kB  format=text"
# При TIMING OFF поля time= в ней НЕТ вовсе - ловушка 2 дизайна.
_PG_SER_RE = re.compile(r"^\s*Serialization:\s*(?P<tail>.+)$", re.M)
_PG_SER_TIME_RE = re.compile(r"\btime=([\d.]+)\s*ms")
_PG_SER_OUT_RE = re.compile(r"\boutput=([\d.]+)\s*kB")
_PG_SER_FMT_RE = re.compile(r"\bformat=(\w+)")
_PG_TEMP_RE = re.compile(r"\btemp\s+read=(\d+)\s+written=(\d+)")

_PG_SERIALIZE_MODES = ("none", "text", "binary")


def _pg_finish(conn) -> None:
    """Закрыть транзакцию пробы: соединение не должно висеть idle in
    transaction и держать снимок все время серии. Именно commit, а не rollback:
    SET в PostgreSQL транзакционен, откат снял бы настройки сессии."""
    if not getattr(conn, "autocommit", True):
        conn.commit()


def _pg_explain(conn, options, sql: str) -> str:
    """Один EXPLAIN, текст плана целиком. Точка ветвления всех PG-функций."""
    stmt = f"EXPLAIN ({', '.join(options)}) {sql.strip().rstrip(';')}"
    with conn.cursor() as cur:
        cur.execute(stmt)
        rows = cur.fetchall()
    _pg_finish(conn)
    return "\n".join(str(r[0]) for r in rows)


def _pg_head(plan: str, lines: int = 12) -> str:
    """Голова плана для текста ошибки - чтобы падение было разбираемым."""
    head = plan.strip().splitlines()[:lines]
    return "\n".join(head)


def pg_prepare_session(conn) -> dict:
    """Выставить настройки сессии и прочитать их ОБРАТНО.

    Возвращает {имя: фактическое значение} - это идет в паспорт стенда. Читаем
    обратно, а не верим SET: сервер мог быть собран без jit, и тогда честнее
    записать в паспорт то, что действительно в силе.
    """
    applied = {}
    with conn.cursor() as cur:
        # F15: служебные SET и SHOW идут по тому же соединению, что зачетный
        # запрос, и без маркера попадали бы в серверную статистику клетки
        for name, value in PG_SESSION_SETTINGS:
            # имена - константы модуля
            cur.execute(setup_sql(f"SET {name} = {value}"))
        for name, _ in PG_SESSION_SETTINGS:
            cur.execute(setup_sql(f"SHOW {name}"))
            applied[name] = cur.fetchone()[0]
    _pg_finish(conn)
    return applied


def pg_server_cost(conn, sql: str, serialize: str = "none",
                   timing: bool = True, buffers: bool = False) -> dict:
    """Серверная цена запроса по версии самого PostgreSQL.

        serialize="none"   - сервер исполнил запрос и выбросил результат;
        serialize="text"   - плюс текстовая кодировка выдачи;
        serialize="binary" - плюс двоичная кодировка выдачи.

    Возвращает (контракт):
        server_time_s     - Execution Time плана, секунды;
        out_kb            - output= из строки Serialization (None при "none");
        plan              - текст плана целиком, идет в артефакты серии.
    Дополнительно: serialize_time_s (time= из той же строки - ПРЯМОЙ замер
    сериализации, а не разность двух прогонов), planning_time_s, ser_format,
    mode. Разность Execution Time между text|binary и none считает вызывающий,
    если ему нужна именно она.

    Настройки сессии выставляются на каждой пробе (два дешевых SET против
    EXPLAIN ANALYZE, который на порядки дороже) - иначе забытый SET молча
    поменял бы то, что мы называем серверной долей.

    Тайминг НЕ выключаем: с TIMING OFF из строки Serialization исчезает поле
    time= (ловушка 2 дизайна, explain.c). Плата за инструментирование снимается
    отдельной парой - pg_timing_overhead.
    """
    mode = str(serialize).strip().lower()
    if mode not in _PG_SERIALIZE_MODES:
        raise ValueError(f"serialize={serialize!r}, ожидается одно из "
                         f"{_PG_SERIALIZE_MODES}")

    with conn.cursor() as cur:
        for name, value in PG_SESSION_SETTINGS:
            cur.execute(f"SET {name} = {value}")

    options = ["ANALYZE", f"TIMING {'ON' if timing else 'OFF'}",
               f"SERIALIZE {mode.upper()}"]
    if buffers:
        options.append("BUFFERS")
    plan = _pg_explain(conn, options, sql)

    exec_m = _PG_EXEC_RE.search(plan)
    if exec_m is None:
        # Execution Time печатается ВСЕГДА при ANALYZE - если его нет, мы
        # разбираем не то, что думаем (другой мажор, FORMAT JSON, ошибка).
        raise SrvControlError(
            "в плане нет строки Execution Time - разбирать нечего:\n"
            + _pg_head(plan))
    plan_m = _PG_PLANT_RE.search(plan)

    out = {
        "server_time_s": float(exec_m.group(1)) / 1000.0,
        "out_kb": None,
        "plan": plan,
        "serialize_time_s": None,
        "planning_time_s": float(plan_m.group(1)) / 1000.0 if plan_m else None,
        "ser_format": None,
        "mode": mode,
        "timing": timing,
    }
    if mode == "none":
        return out                        # строки Serialization тут нет и быть не должно

    ser_m = _PG_SER_RE.search(plan)
    if ser_m is None:
        raise SrvControlError(
            f"SERIALIZE {mode.upper()} задан, а строки Serialization в плане "
            f"нет - сервер не поддерживает опцию или план не тот:\n"
            + _pg_head(plan))
    tail = ser_m.group("tail")

    out_m = _PG_SER_OUT_RE.search(tail)
    if out_m is None:                     # без output= вся проба бессмысленна
        raise SrvControlError(
            f"в строке Serialization нет поля output=: {tail!r}")
    out["out_kb"] = float(out_m.group(1))

    time_m = _PG_SER_TIME_RE.search(tail)
    if time_m is not None:
        out["serialize_time_s"] = float(time_m.group(1)) / 1000.0
    elif timing:
        # Тайминг включен, а time= нет - значит формат строки не тот, под
        # который написан парсер. Молчаливый None тут хуже падения: он утек бы
        # в CSV пустой клеткой и мы бы этого не заметили.
        raise SrvControlError(
            f"тайминг включен, но поля time= в строке Serialization нет: "
            f"{tail!r}")

    fmt_m = _PG_SER_FMT_RE.search(tail)
    if fmt_m is not None:
        out["ser_format"] = fmt_m.group(1)
    return out


def pg_timing_overhead(conn, sql: str, serialize: str = "none") -> dict:
    """Плата за инструментирование: та же проба с таймингом и без него.

    Гонять серию мы обязаны с включенным таймингом (иначе теряется output= и
    time= в Serialization), поэтому цену самого измерения снимаем отдельной
    парой и проговариваем на слайде числом, а не словом "накладные".

    Порядок прогонов - сначала OFF, потом ON: так более дорогой режим не
    пользуется прогретым кешем предыдущего. Повторы - на стороне вызывающего.
    """
    off = pg_server_cost(conn, sql, serialize, timing=False)["server_time_s"]
    on = pg_server_cost(conn, sql, serialize, timing=True)["server_time_s"]
    return {
        "timing_off_s": off,
        "timing_on_s": on,
        "overhead_s": on - off,
        "overhead_ratio": (on / off) if off > 0 else None,
    }


def pg_check_generator_plan(conn, sql: str, strict: bool = True) -> dict:
    """Предполетный гейт холста генерации (ловушка 1 дизайна).

    generate_series в списке FROM - это FunctionScan, а он вычитывает функцию
    в tuplestore ЦЕЛИКОМ до первой строки: получится замер материализации, а не
    потока, да еще и с уходом во временные файлы на больших N. Правильная форма
    холста - FROM (SELECT generate_series(1, N) AS g) s, на ней планировщик
    ставит ProjectSet и строки идут потоком.

    Проверяем три вещи разом: ProjectSet есть, Function Scan нет, temp
    read/written нулевые. При провале по умолчанию поднимаем PreflightError -
    серия на невалидном холсте не стартует.
    """
    plan = _pg_explain(conn, ["ANALYZE", "BUFFERS", "SERIALIZE NONE"], sql)

    problems = []
    if "ProjectSet" not in plan:
        problems.append("в плане нет ProjectSet - генератор не стримит")
    if "Function Scan" in plan:
        problems.append("в плане есть Function Scan - generate_series стоит в "
                        "FROM и материализуется в tuplestore целиком")
    temp = _PG_TEMP_RE.findall(plan)
    temp_read = sum(int(a) for a, _ in temp)
    temp_written = sum(int(b) for _, b in temp)
    if temp_read or temp_written:
        problems.append(f"ненулевые временные файлы: temp read={temp_read} "
                        f"written={temp_written}")

    verdict = {
        "ok": not problems,
        "problems": problems,
        "temp_read": temp_read,
        "temp_written": temp_written,
        "plan": plan,
    }
    if problems and strict:
        raise PreflightError(
            "холст генерации невалиден: " + "; ".join(problems) + "\n"
            + _pg_head(plan))
    return verdict


def pg_check_plan_profiles(conn, sqls, strict: bool = True) -> dict:
    """Гейт B контракта: тот же план, но на КАЖДОМ зачетном профиле.

    Один прогон на одном профиле ничего не гарантирует: форма холста у профилей
    разная (одна колонка, десять, пятьдесят), и планировщик может сорваться в
    Function Scan или уехать во временные файлы только на широком.

    sqls - {имя профиля: реальный текст запроса} либо последовательность пар.
    Тексты берутся у сценария через --print-sql: запасной эталонной строки по
    контракту быть не должно, иначе гейт проверяет наши намерения, а не то, что
    поедет в серию. Пустой текст - сразу отказ, а не тихий пропуск профиля.
    """
    items = list(sqls.items()) if isinstance(sqls, dict) else \
        [tuple(pair) for pair in sqls]
    if not items:
        raise PreflightError(
            "гейт B: не передано ни одного профиля - проверять нечего")

    verdicts, failed = {}, []
    for name, sql in items:
        if not str(sql or "").strip():
            raise PreflightError(
                f"гейт B: профиль {name} отдал пустой SQL - сценарий не "
                "поддержал --print-sql, а запасной эталонной строки по "
                "контракту быть не должно")
        verdict = pg_check_generator_plan(conn, sql, strict=False)
        verdicts[name] = verdict
        if not verdict["ok"]:
            failed.append(f"{name}: " + "; ".join(verdict["problems"]))

    out = {"ok": not failed, "failed": failed, "profiles": verdicts}
    if failed and strict:
        raise PreflightError("гейт B: план невалиден на профилях - "
                             + " | ".join(failed))
    return out


# ------------------------------------------------------------- ClickHouse ----

# Прибиваем константой: по умолчанию 1, и тогда смена FORMAT молча меняет число
# серверных потоков (построчные текстовые форматы параллельное форматирование
# поддерживают, Native, Arrow и Parquet - нет). Ось A1 без этого меряет не
# формат, а параллелизм. Пара 0 против 1 снимается отдельной находкой.
CH_PROBE_SETTINGS = {"output_format_parallel_formatting": 0}

# Хвост "FORMAT X" из запроса срезаем: пробе нужен ровно тот же SELECT, но с
# FORMAT Null. Точка с запятой тоже мешает - драйвер дописывает формат в конец.
_CH_FORMAT_TAIL_RE = re.compile(r"\s+FORMAT\s+\w+\s*;?\s*$", re.I)
_QID_RE = re.compile(r"^[A-Za-z0-9_.:-]+$")
# Имя таблицы для служебных операций блока 0 (OPTIMIZE FINAL, system.parts):
# подставляется в SQL литералом, поэтому набор символов узкий - буквы, цифры,
# подчеркивание и одна точка разделителем базы
_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")


def new_query_id(prefix: str = "stande") -> str:
    """Идентификатор запроса для последующей вычитки system.query_log."""
    return f"{prefix}-{uuid.uuid4().hex}"


# F15 (RUN-04, SRVEXEC-01): служебные запросы клетки - выборка ключей П4,
# чтение system.settings, SHOW и прочая подготовка - идут по тому же
# соединению и в том же окне, что и зачетный запрос. Серверный слой брал их
# за top-1 по времени и приписывал клетке ЧУЖОЙ запрос (20 клеток серии F).
# Маркер один на все служебное: сборщик srvstats выкидывает по нему строки,
# а не гадает по тексту.
SETUP_MARKER = "/*bench:setup*/"


def setup_sql(sql: str) -> str:
    """Пометить служебный запрос маркером SETUP_MARKER (F15).

    Комментарий стоит ПЕРВЫМ: и PostgreSQL, и ClickHouse отдают текст запроса
    в свои системные таблицы целиком, вместе с ведущим комментарием, поэтому
    фильтр сборщика ловит его подстрокой без разбора SQL.
    """
    return f"{SETUP_MARKER} {sql}"


def ch_marker(query_id: str) -> str:
    """Комментарий-маркер, который вставляется в текст запроса.

    Страховка на случай, если драйвер не пробросит query_id: по маркеру запрос
    находится в query_log и без него. Форма именно комментарием - тогда сам
    запрос к query_log (в нем query_id перечислены строками) под собственный
    фильтр не попадает и не затирает настоящую строку лога.
    """
    _check_qid(query_id)
    return f"/* {query_id} */"


def _check_qid(query_id: str) -> None:
    if not _QID_RE.match(str(query_id)):
        # id подставляются в SQL литералом, поэтому набор символов узкий
        raise ValueError(f"недопустимый query_id: {query_id!r}")


def _strip_format(sql: str) -> str:
    return _CH_FORMAT_TAIL_RE.sub("", sql.strip().rstrip(";")).strip()


def ch_query_settings(client, query_id: str, extra: dict = None) -> dict:
    """Настройки запроса с проброшенным query_id.

    Тот же набор используют и пробы, и боевые замеры сценария - иначе проба
    мерила бы сервер в одном режиме, а замер шел бы в другом. query_id кладем
    только если драйвер умеет его пробрасывать (у clickhouse-connect это
    транспортная настройка); если не умеет - остается маркер в тексте.
    """
    settings = dict(CH_PROBE_SETTINGS)
    settings.update(extra or {})
    if "query_id" in (getattr(client, "valid_transport_settings", ()) or ()):
        settings["query_id"] = query_id
    return settings


def ch_server_cost(client, sql: str, query_id: str = None,
                   settings: dict = None, collect: bool = True) -> dict:
    """Серверная цена запроса в ClickHouse: тот же SELECT, но FORMAT Null.

    Отдельного события "сериализация" в ClickHouse нет, поэтому уровней два:
    FORMAT Null (сервер исполнил, но не форматировал и не отправил) и боевой
    FORMAT (все вместе). Разложить их точнее сервер не дает - на слайд идет
    неравенство, не вычитание.

    Возвращает (контракт): server_time_s, out_kb, result_bytes. Плюс query_id,
    wall_s, source ("query_log" или "wall") и сетевые ProfileEvents.

    out_kb у ЭТОЙ пробы пуст по построению: FORMAT Null ничего не форматирует.
    То, что сервер все же отправил в сокет, - накладные протокола, они уходят
    отдельным полем proto_overhead_kb: пустая клетка не должна превратиться в
    "полкилобайта выдачи". Осмысленный out_kb берется у БОЕВОГО замера через
    ch_collect_log + ch_cost_from_log - это и есть вторая полоска серверной
    работы.

    collect=False - режим для горячего цикла, и это ЕДИНСТВЕННЫЙ допустимый
    режим внутри окна замера: SYSTEM FLUSH LOGS и чтение query_log сами по себе
    едут по тому же проводу и попали бы в байты замера (мина 6 аудита).
    Собирайте query_id в список и добирайте лог одним ch_collect_log ПОСЛЕ
    блока. collect=True оставлен для предполетных проб, где окна замера нет.
    """
    qid = query_id or new_query_id("stande-null")
    _check_qid(qid)
    probe = f"{ch_marker(qid)} {_strip_format(sql)}"
    opts = ch_query_settings(client, qid, settings)

    t0 = time.perf_counter()
    client.raw_query(probe, settings=opts, fmt="Null")
    wall = time.perf_counter() - t0

    out = {
        "server_time_s": wall,   # запасной вариант: ответа нет, wall = сервер
        "out_kb": None,
        "proto_overhead_kb": None,
        "result_bytes": None,
        "query_id": qid,
        "wall_s": wall,
        "source": "wall",
    }
    if collect:
        cost = ch_cost_from_log(ch_collect_log(client, [qid]).get(qid))
        # у FORMAT Null форматированной выдачи нет, поэтому NetworkSendBytes
        # тут - не выдача, а цена самого разговора: заголовки ответа и кадры
        # протокола. Кладем их отдельно, out_kb оставляем пустым
        cost["proto_overhead_kb"] = cost.pop("out_kb", None)
        out.update(cost)
    return out


def ch_cost_from_log(log: dict) -> dict:
    """Перевод строки system.query_log в поля серверной цены.

    Почему out_kb берется из ProfileEvents NetworkSendBytes, а НЕ из
    result_bytes. Это два разных числа:
        result_bytes - размер результата во внутреннем представлении сервера,
                       то есть память под колонки перед форматированием. От
                       смены FORMAT он почти не меняется: те же значения, тот
                       же тип, другая только упаковка на выходе;
        NetworkSendBytes - сколько байт сервер фактически записал в сокет,
                       то есть уже сформатированную выдачу (и сжатую, если
                       кодек включен).
    Парная полоска к постгресовому output= - вторая. На result_bytes находки
    "сервер сам назвал цену" и "бинарное не значит меньше байт" держались бы на
    числе, которое от формата не зависит, - то есть не держались бы вовсе.

    Оговорка, которую надо проговаривать вслух: в NetworkSendBytes входят и
    накладные протокола (заголовки ответа, кадры блоков) - на объемах серии это
    сотни байт против мегабайт, но у пробы FORMAT Null это ВСЕ, что там есть
    (см. ch_server_cost).

    result_bytes остается отдельным полем - по нему сверяется, что сравниваемые
    форматы отдали один и тот же результат, а не разный объем работы.
    """
    if not log:
        return {}
    result_bytes = log.get("result_bytes")
    net_send_bytes = log.get("net_send_bytes")
    return {
        "server_time_s": (log.get("query_duration_ms") or 0) / 1000.0,
        # ноль здесь - это "сервер ничего не отправил" (FORMAT Null или строка
        # лога без ProfileEvents), а не "измерили ноль килобайт"
        "out_kb": (net_send_bytes / 1024.0) if net_send_bytes else None,
        "out_bytes": net_send_bytes,
        "result_bytes": result_bytes,      # для сверки, не для колонки out_kb
        "source": "query_log",
        "read_rows": log.get("read_rows"),
        "read_bytes": log.get("read_bytes"),
        "result_rows": log.get("result_rows"),
        "memory_usage": log.get("memory_usage"),
        "net_send_bytes": net_send_bytes,
        "net_send_s": (log.get("net_send_us") or 0) / 1e6,
    }


def ch_collect_log(client, query_ids, flush: bool = True) -> dict:
    """Добрать system.query_log по пачке query_id ОДНИМ запросом.

    Вызывается ПОСЛЕ блока замеров, никогда внутри окна: SYSTEM FLUSH LOGS -
    это работа сервера, а чтение лога - трафик по тому же соединению, и то и
    другое попало бы в метрики (мина 6 аудита).

    Возвращает {query_id: {...}} с query_duration_ms, result_rows/result_bytes,
    read_rows/read_bytes, memory_usage и ProfileEvents NetworkSendBytes /
    NetworkSendElapsedMicroseconds. Запросы, которых в логе не нашлось, в ответе
    просто отсутствуют - вызывающий решает, ошибка это или нет.

    Сырую строку в поля серверной цены переводит ch_cost_from_log - через него,
    а не руками: там же решается, что out_kb это NetworkSendBytes, а не
    result_bytes.
    """
    ids = [str(q) for q in dict.fromkeys(query_ids)]   # без дублей, порядок цел
    for qid in ids:
        _check_qid(qid)
    if not ids:
        return {}
    if flush:
        # F15: чтение лога - служебная работа клетки, и в серверную статистику
        # она входить не должна
        client.command(setup_sql("SYSTEM FLUSH LOGS"))

    ids_lit = "[" + ", ".join(f"'{q}'" for q in ids) + "]"
    markers_lit = "[" + ", ".join(f"'{ch_marker(q)}'" for q in ids) + "]"
    # key: если драйвер пробросил query_id - берем его, иначе ищем, чей маркер
    # стоит в тексте запроса. Строки самого этого запроса под фильтр не
    # попадают - в нем id перечислены без обрамления комментарием, плюс явное
    # исключение читателей лога.
    sql = f"""
SELECT if(has({ids_lit}, query_id),
          query_id,
          arrayFirst(x -> position(query, concat('/* ', x, ' */')) > 0,
                     {ids_lit}))                       AS key,
       query_id,
       toString(type)                                  AS type,
       query_duration_ms,
       read_rows,
       read_bytes,
       result_rows,
       result_bytes,
       memory_usage,
       ProfileEvents['NetworkSendBytes']               AS net_send_bytes,
       ProfileEvents['NetworkSendElapsedMicroseconds'] AS net_send_us,
       exception
FROM system.query_log
WHERE event_date >= today() - 1
  AND type != 'QueryStart'
  AND query NOT LIKE '%system.query_log%'
  AND (has({ids_lit}, query_id) OR multiSearchAny(query, {markers_lit}))
ORDER BY event_time_microseconds
"""
    result = client.query(setup_sql(sql))
    names = list(result.column_names)
    out = {}
    for row in result.result_rows:
        rec = dict(zip(names, row))
        key = rec.pop("key", "")
        if key:            # более поздняя строка перекрывает раннюю: последняя
            out[key] = rec  # попытка - зачетная
    return out


# -------------------------------------------------------------- кодеки ----

# Значения оси сжатия из контракта (раздел 3): BENCH_CODEC = none | lz4 | zstd.
CH_CODECS = ("none", "lz4", "zstd")

# У каких форматов сжатие живет ВНУТРИ формата, а не на транспорте. Для
# остальных (Native, RowBinary, CSV, JSON*) собственного кодека нет - там
# BENCH_CODEC означает транспортное сжатие.
CH_FORMAT_CODEC_SETTING = {
    "arrow": "output_format_arrow_compression_method",
    "arrowstream": "output_format_arrow_compression_method",
    "parquet": "output_format_parquet_compression_method",
    "orc": "output_format_orc_compression_method",
}

# Имя одного и того же кодека у каждого формата свое: у Arrow lz4 называется
# lz4_frame, значение lz4 даст BAD_ARGUMENTS (мина 4 дизайна). Именно поэтому
# гейт D нужен ДО прогона: иначе серия падает на середине или, что хуже, идет
# на кодеке, который сервер понял иначе, чем мы.
CH_CODEC_VALUE = {
    "output_format_arrow_compression_method": {
        "none": "none", "lz4": "lz4_frame", "zstd": "zstd"},
    "output_format_parquet_compression_method": {
        "none": "none", "lz4": "lz4", "zstd": "zstd"},
    "output_format_orc_compression_method": {
        "none": "none", "lz4": "lz4", "zstd": "zstd"},
}

# Кодек, который сервер обязан ОТВЕРГНУТЬ. Проверка от обратного: если он его
# съел, значит настройка не та, что мы думаем, и все замеры сжатия Arrow
# меряют что-то другое.
CH_CODEC_MUST_REJECT = ("output_format_arrow_compression_method", "lz4")


def ch_codec_plan(fmt: str, codec: str, layer: str = None) -> dict:
    """Разложить BENCH_CODEC на конкретные настройки ОДНОГО слоя (гейт D).

    Контракт, раздел 6: при codec=none сжатие снимается на всех уровнях сразу,
    при непустом кодеке нельзя включать два слоя одновременно - транспортное
    сжатие и сжатие буферов формата дали бы байты, которые не объясняются ни
    тем, ни другим.

    layer - ось BENCH_CH_CODEC_LAYER (HC3): у Arrow, ArrowStream и Parquet
    один и тот же lz4 можно включить ДВУМЯ разными способами, и знак цены у
    них разный - внутри формата сжимаются буферы колонок
    (output_format_*_compression_method), на транспорте сжимается весь ответ
    (Accept-Encoding у HTTP, network_compression_method у нативного TCP).
    В серии F выбор делал формат, а не замер: Arrow всегда уходил в форматный
    слой, и клетка "цена сжатия" отвечала на два вопроса сразу. Дефолт оси -
    transport: он общий для ВСЕХ форматов, то есть точки оси сравнимы между
    собой; форматный слой берется явно (метка клетки несет слой).

    Возвращает:
        codec     - как просили (none | lz4 | zstd);
        layer     - none | format | transport, где сжатие включено;
        value     - фактическое значение для сервера (lz4 у Arrow это lz4_frame);
        transport - включать ли транспортное сжатие у клиента;
        settings  - настройки формата, которые надо прибить явно ВСЕГДА, в том
                    числе нулями: дефолт сборки не должен решать за замер;
        label     - что писать в колонку codec, чтобы слой был виден.
    """
    codec = str(codec or "none").strip().lower()
    if codec not in CH_CODECS:
        raise PreflightError(
            f"BENCH_CODEC={codec!r} не из набора {CH_CODECS} - "
            "по контракту ось сжатия задается только этими значениями")
    want_layer = str(layer or "transport").strip().lower()
    if want_layer not in ("transport", "format"):
        raise PreflightError(
            f"BENCH_CH_CODEC_LAYER={want_layer!r}: ожидается transport или "
            "format - слоев сжатия у ClickHouse ровно два")

    setting = CH_FORMAT_CODEC_SETTING.get(str(fmt or "").strip().lower())
    # сначала гасим все форматные кодеки, потом включаем ровно один
    settings = {name: "none" for name in CH_CODEC_VALUE}
    plan = {"codec": codec, "layer": "none", "value": "none",
            "transport": False, "settings": settings, "label": "none"}
    if codec == "none":
        return plan
    if want_layer == "format":
        if not setting:
            # у Native, JSON и текстовых форматов своего сжатия нет вовсе:
            # клетка снялась бы транспортом под меткой форматного слоя
            raise PreflightError(
                f"BENCH_CH_CODEC_LAYER=format при BENCH_FMT={fmt!r}: своего "
                f"сжатия у этого формата нет, форматный слой есть только у "
                f"{tuple(CH_FORMAT_CODEC_SETTING)}")
        value = CH_CODEC_VALUE[setting][codec]
        settings[setting] = value
        plan.update(layer="format", value=value, label=f"{value}@format")
    else:
        plan.update(layer="transport", value=codec, transport=True,
                    label=f"{codec}@transport")
    return plan


def ch_check_codecs(client, codecs=CH_CODECS, strict: bool = True) -> dict:
    """Гейт D: сервер понимает те значения кодеков, которыми мы собрались гнать.

    Проверка на живом сервере и до первого зачетного замера. Порядок клауз в
    ClickHouse строгий: SETTINGS идет ДО FORMAT. Значение перечислимой настройки
    разбирается при разборе запроса, поэтому FORMAT Null достаточно - выводить
    ничего не нужно, проба стоит миллисекунды.
    """
    accepted, problems = {}, []
    for setting, table in CH_CODEC_VALUE.items():
        for codec in codecs:
            value = table.get(str(codec).strip().lower())
            if value is None:
                problems.append(f"кодек {codec!r} не определен для {setting}")
                continue
            probe = f"SELECT 1 SETTINGS {setting}='{value}' FORMAT Null"
            try:
                client.command(probe)
                accepted[f"{setting}={value}"] = True
            except Exception as exc:                      # noqa: BLE001
                accepted[f"{setting}={value}"] = False
                problems.append(f"сервер не принял {setting}={value}: {exc}")

    bad_setting, bad_value = CH_CODEC_MUST_REJECT
    try:
        client.command(
            f"SELECT 1 SETTINGS {bad_setting}='{bad_value}' FORMAT Null")
    except Exception:                                     # noqa: BLE001
        accepted[f"{bad_setting}={bad_value}"] = False    # так и должно быть
    else:
        accepted[f"{bad_setting}={bad_value}"] = True
        problems.append(
            f"сервер принял {bad_setting}={bad_value} - это не та настройка, "
            "что мы думаем: у Arrow кодек называется lz4_frame")

    verdict = {"ok": not problems, "problems": problems, "accepted": accepted}
    if problems and strict:
        raise PreflightError("гейт D (кодеки): " + "; ".join(problems))
    return verdict


def ch_server_timezone(client) -> str:
    """Зона времени СЕРВЕРА ClickHouse (HC5, запрос grid п.5).

    Тройка зоны сервера снимается перезапуском контейнера по механике оси
    VER - это процедура блока 0, а не настройка клетки, и переменной среза
    для нее нет. Но факт, при котором сняты обе половины квадрата, обязан
    быть записан ЦИФРОЙ: разница между клеткой на UTC-сервере и клеткой на
    Europe/Moscow иначе неотличима от разницы между двумя ночами. Строку
    зовет паспорт прогона до и после перезапуска; проекция w10tz закрывает
    вторую половину квадрата уже внутри одной сессии.
    """
    return str(client.command(setup_sql("SELECT timezone()"))).strip()


def ch_optimize_final(client, table: str, parts: bool = True) -> dict:
    """OPTIMIZE TABLE ... FINAL и число кусков до и после (F14).

    Пара "до и после" - это два прогона ОДНИХ И ТЕХ ЖЕ меток вокруг
    служебной операции, а не секция сетки: слияние кусков меняет объем
    чтения, и клетка hits после него - другая клетка под тем же именем.
    Поэтому операцию делает сессия блока 0, а число кусков пишется в
    паспорт: без него утверждение "разница от слияния" ничем не подперто.

    Возвращает parts_before / parts_after (штуки) и wall_s самой операции.
    Имя таблицы подставляется литералом, поэтому набор символов узкий -
    посторонний SQL сюда не проедет.
    """
    if not _TABLE_RE.match(table or ""):
        raise ValueError(f"недопустимое имя таблицы: {table!r}")
    db, _, name = table.rpartition(".")
    db_cond = f"database = '{db}'" if db else "database = currentDatabase()"
    count_sql = setup_sql(
        "SELECT count() FROM system.parts WHERE active AND "
        f"{db_cond} AND table = '{name}'")

    def _parts() -> int:
        if not parts:
            return -1
        try:
            return int(str(client.command(count_sql)).strip())
        except Exception as exc:                          # noqa: BLE001
            # число кусков - паспорт, а не гейт: недоступная system.parts
            # (прав нет на управляемом контуре) не повод не делать слияние
            print(f"# system.parts недоступна: {exc}", file=sys.stderr)
            return -1

    before = _parts()
    t0 = time.perf_counter()
    client.command(setup_sql(f"OPTIMIZE TABLE {table} FINAL"))
    wall = time.perf_counter() - t0
    after = _parts()
    print(f"# OPTIMIZE {table} FINAL: кусков {before} -> {after}, "
          f"{wall:.3f} с", file=sys.stderr)
    return {"table": table, "parts_before": before, "parts_after": after,
            "wall_s": wall}


# ------------------------------------------------------ валидация режима ----

def check_digests_equal(digests: dict, label: str = "",
                        strict: bool = True) -> dict:
    """Гейты A и C контракта: у всех сверяемых выдач один и тот же sha256.

    Гейт A ставит рядом два движка на одной точке, гейт C - все пути блока
    аналитика на ста тысячах строк. Механика одна: пока хеши не сошлись,
    сравнивать скорости незаконно - пути могли принести разные данные.

    digests - {имя пути или движка: хеш}. Один участник - это не сверка, а
    самообман, поэтому меньше двух непустых значений считается провалом.
    """
    filled = {k: v for k, v in digests.items() if v}
    empty = sorted(k for k, v in digests.items() if not v)
    distinct = sorted(set(filled.values()))

    problems = []
    if empty:
        problems.append("хеш не заполнен у: " + ", ".join(empty))
    if len(filled) < 2:
        problems.append(f"сверять нечего: непустых хешей {len(filled)}")
    elif len(distinct) > 1:
        pairs = ", ".join(f"{k}={v[:12]}" for k, v in sorted(filled.items()))
        problems.append(f"выдачи РАЗНЫЕ ({len(distinct)} хеша): {pairs}")

    verdict = {
        "ok": not problems,
        "problems": problems,
        "digests": dict(digests),
        "distinct": distinct,
        "label": label,
    }
    if problems and strict:
        raise PreflightError(
            f"гейт идентичности данных{' ' + label if label else ''}: "
            + "; ".join(problems))
    return verdict


def check_wide_over_narrow(narrow_s: float, wide_s: float, label: str = "",
                           min_ratio: float = 1.5,
                           strict: bool = True) -> dict:
    """Режим "не форматировать вывод" валиден только если работа осталась.

    И SERIALIZE NONE, и FORMAT Null разрешают оптимизатору выбросить вычисление
    выражений, которые никто не читает. Если это случилось, серверное время
    перестает зависеть от ширины профиля - и вся полоска серверной работы
    становится измерением пустоты, причем правдоподобным на вид.

    Проверка прямая: широкий профиль обязан считаться заметно дольше узкого.
    Равенство - это не "движок быстрый", это невалидный режим, и сообщать о нем
    надо громко.
    """
    ratio = (wide_s / narrow_s) if narrow_s > 0 else None
    ok = ratio is not None and ratio >= min_ratio
    verdict = {
        "ok": ok,
        "narrow_s": narrow_s,
        "wide_s": wide_s,
        "ratio": ratio,
        "min_ratio": min_ratio,
        "label": label,
    }
    if ok:
        return verdict

    message = (f"!!! режим серверного контроля невалиден{' ' + label if label else ''}: "
               f"узкий профиль {narrow_s:.4f} с, широкий {wide_s:.4f} с, "
               f"отношение {ratio if ratio is None else round(ratio, 2)} "
               f"< {min_ratio} - оптимизатор срезал выражения, "
               f"серверная доля ничего не измеряет")
    print(message, file=sys.stderr, flush=True)
    if strict:
        raise ModeInvalidError(message)
    return verdict


def pg_check_serialize_none(conn, sql_narrow: str, sql_wide: str,
                            min_ratio: float = 1.5,
                            strict: bool = True) -> dict:
    """То же для PostgreSQL: SERIALIZE NONE на узком против широкого профиля."""
    narrow = pg_server_cost(conn, sql_narrow, "none")["server_time_s"]
    wide = pg_server_cost(conn, sql_wide, "none")["server_time_s"]
    return check_wide_over_narrow(narrow, wide, "PG SERIALIZE NONE",
                                  min_ratio, strict)


def ch_check_format_null(client, sql_narrow: str, sql_wide: str,
                         min_ratio: float = 1.5, strict: bool = True) -> dict:
    """То же для ClickHouse: FORMAT Null на узком против широкого профиля."""
    narrow = ch_server_cost(client, sql_narrow)["server_time_s"]
    wide = ch_server_cost(client, sql_wide)["server_time_s"]
    return check_wide_over_narrow(narrow, wide, "CH FORMAT Null",
                                  min_ratio, strict)
