#!/usr/bin/env python3
"""Codec C1: типизированная канонизация выдачи и digest мультимножества.

Зачем модуль (D43). Прежний sha256_digest (_srvcontrol) склеивал значения в
ОДНУ строку через разделитель и хешировал ее потоково. Отсюда три класса
ложных равенств и одно ложное различие:

  1) разделитель. Значения приводились к тексту и соединялись байтом-
     разделителем. Строка "a\\x1fb" в одной колонке и пара колонок ("a","b")
     давали один и тот же поток байт: перенос границы между полями хеш не
     менял. То же с разделителем записей внутри значения;
  2) тип. "1" (строка) и 1 (целое) канонизировались в один текст "1".
     b"1", True и "1" - туда же. Сверка кортежного пути с Arrow-путем
     проходила при разных типах одного значения;
  3) порядок. Хеш считался потоково по порядку строк, поэтому иное разбиение
     на батчи не мешало, а вот иной порядок строк (законный для SQL без
     ORDER BY) ронял сверку, которая обязана быть про ЗНАЧЕНИЯ;
  4) момент времени. Arrow-путь ClickHouse отдает DateTime как uint32, а
     кортежный - как datetime. Первое канонизировалось как целое "1700000000",
     второе как "2023-11-14T22:13:20+00:00": одни и те же данные давали разные
     digest, и сверка путей была невозможна без ручной оговорки.

Контракт нового кодека:

  поле      -> тег логического типа (1 байт) + длина содержимого (4 байта,
               big-endian) + содержимое. Самоограничивающая запись: перенос
               границы между полями и разделитель внутри значения байты
               меняют;
  запись    -> sha256 конкатенации закодированных полей;
  выдача    -> digest МУЛЬТИМНОЖЕСТВА: сумма хешей записей по модулю 2^256
               плюс счетчик строк. Не зависит ни от порядка строк, ни от
               разбиения на батчи; потеря и дубль строки его меняют.

Канонизация идет по ОБЪЯВЛЕННОЙ логической схеме клетки (LogicalSchema):
колонка типа "timestamp" приводится к мгновению независимо от того, отдал ее
драйвер целым числом секунд или datetime. Наивное время читается как UTC
(данные стенда - UTC по построению: гейт дат, TimeZone=UTC у сессий),
осведомленное - приводится к UTC. Одинаковое мгновение - одинаковые байты.

Публичный API:
    CODEC_ID                                   - идентификатор формата
    LogicalSchema.parse(spec)                  - объявленная схема клетки
    digest_rows(rows, schema=None)             - эталонная построчная реализация
    DigestAccumulator                          - потоковая сверка по батчам
    digest_arrow(table_or_batches, schema)     - векторная реализация для Arrow
    schema_signature_arrow / _dataframe / _tuples - подпись схемы результата

Эталонная и векторная реализации ОБЯЗАНЫ давать один digest; это утверждение
проверяется тестами (tests/test_scenarios_codec.py), а не подразумевается.
"""

import datetime as _dt
import hashlib
import ipaddress as _ipaddress
import math
import os
import uuid as _uuid
from decimal import Decimal

# Версия формата входит в идентификатор: корпус, снятый разными версиями
# кодека, сравнивать нельзя, и это должно быть видно в строке сырья.
CODEC_ID = "codec/v1"

# Режим проверки. Усеченная выборка при сверке значений ЗАПРЕЩЕНА (D43):
# режим существует только чтобы его имя попало в строку сырья и в паспорт.
CODEC_MODE_FULL = "full"

_MOD = 1 << 256

# --- теги логических типов -------------------------------------------------
# Один байт на тег; менять значения нельзя без смены CODEC_ID.
TAG_NULL = b"N"
TAG_BOOL = b"L"
TAG_INT = b"I"
TAG_FLOAT = b"F"
TAG_DEC = b"C"
TAG_STR = b"S"
TAG_BYTES = b"B"
TAG_TS = b"T"        # мгновение, наносекунды от эпохи UTC
TAG_DATE = b"D"      # дни от эпохи
TAG_TIME = b"H"      # наносекунды от полуночи
TAG_LIST = b"A"
TAG_MAP = b"M"
TAG_UUID = b"U"      # 16 байт канонического представления
TAG_IP = b"P"        # адрес в упакованном виде (4 или 16 байт)

_EPOCH = _dt.datetime(1970, 1, 1, tzinfo=_dt.timezone.utc)
_EPOCH_DATE = _dt.date(1970, 1, 1)

# Множители единиц времени в наносекунды: тот же словарь читают обе
# реализации, и смена единицы (секунды против микросекунд) обязана менять
# digest, потому что это РАЗНЫЕ мгновения при одном и том же целом.
TIME_UNIT_NS = {"s": 1_000_000_000, "ms": 1_000_000, "us": 1_000, "ns": 1}


class CodecError(ValueError):
    """Значение не канонизируется по объявленной логической схеме."""


# ---------------------------------------------------------------- схема ----

class Column:
    """Колонка объявленной логической схемы клетки."""

    __slots__ = ("name", "logical", "unit", "tz")

    def __init__(self, name: str, logical: str, unit: str = None,
                 tz: str = None):
        logical = logical.strip().lower()
        if logical not in _LOGICAL:
            raise CodecError(
                f"колонка {name!r}: неизвестный логический тип {logical!r}; "
                f"допустимы {sorted(_LOGICAL)}")
        if logical == "timestamp":
            unit = (unit or "us").strip().lower()
            if unit not in TIME_UNIT_NS:
                raise CodecError(
                    f"колонка {name!r}: единица времени {unit!r} не из "
                    f"{sorted(TIME_UNIT_NS)}")
        self.name = name
        self.logical = logical
        self.unit = unit
        self.tz = tz

    def __repr__(self):
        tail = f",{self.unit}" if self.unit else ""
        tail += f",{self.tz}" if self.tz else ""
        return f"Column({self.name!r},{self.logical!r}{tail})"

    def as_spec(self) -> str:
        spec = f"{self.name}:{self.logical}"
        if self.unit:
            spec += f"[{self.unit}"
            spec += f",{self.tz}]" if self.tz else "]"
        return spec


class LogicalSchema:
    """Объявленная логическая схема клетки: порядок и типы колонок.

    Схема не выводится из данных - она ОБЪЯВЛЯЕТСЯ клеткой, иначе канонизация
    зависела бы от того, что вернул драйвер, а сверка путей теряла бы смысл.
    """

    __slots__ = ("columns",)

    def __init__(self, columns):
        self.columns = list(columns)

    def __len__(self):
        return len(self.columns)

    def __iter__(self):
        return iter(self.columns)

    def __repr__(self):
        return f"LogicalSchema({[c.as_spec() for c in self.columns]})"

    @property
    def names(self):
        return [c.name for c in self.columns]

    def as_spec(self) -> str:
        """Строка схемы для паспорта клетки и строки сырья."""
        return ";".join(c.as_spec() for c in self.columns)

    @classmethod
    def parse(cls, spec):
        """Разобрать объявление схемы.

        Формы: строка "id:int;ts:timestamp[s,UTC];name:string",
        список строк, список Column, список кортежей (name, logical[, unit[,
        tz]]) и список словарей.
        """
        if spec is None:
            return None
        if isinstance(spec, cls):
            return spec
        if isinstance(spec, str):
            parts = [p for p in spec.split(";") if p.strip()]
        else:
            parts = list(spec)
        cols = []
        for part in parts:
            if isinstance(part, Column):
                cols.append(part)
            elif isinstance(part, dict):
                cols.append(Column(part["name"], part.get("logical")
                                   or part.get("type"),
                                   part.get("unit"), part.get("tz")))
            elif isinstance(part, (list, tuple)):
                cols.append(Column(*part))
            else:
                cols.append(_parse_column_spec(str(part)))
        return cls(cols)


def _parse_column_spec(text: str) -> Column:
    """"ts:timestamp[s,UTC]" -> Column."""
    text = text.strip()
    if ":" not in text:
        raise CodecError(f"объявление колонки {text!r}: ожидается имя:тип")
    name, _, rest = text.partition(":")
    rest = rest.strip()
    unit = tz = None
    if rest.endswith("]") and "[" in rest:
        rest, _, args = rest.partition("[")
        args = args[:-1]
        bits = [b.strip() for b in args.split(",") if b.strip()]
        if bits:
            unit = bits[0]
        if len(bits) > 1:
            tz = bits[1]
    return Column(name.strip(), rest.strip(), unit, tz)


_LOGICAL = {"int", "float", "decimal", "string", "bytes", "bool",
            "timestamp", "date", "time", "list", "map", "auto"}


# ------------------------------------------------------- кодирование поля ---

def _frame(tag: bytes, content: bytes) -> bytes:
    """тег + длина (4 байта big-endian) + содержимое."""
    n = len(content)
    if n > 0xFFFFFFFF:
        raise CodecError(f"поле длиной {n} байт не кодируется 4-байтовой длиной")
    return tag + n.to_bytes(4, "big") + content


def _enc_int(value) -> bytes:
    return _frame(TAG_INT, str(int(value)).encode("ascii"))


def _enc_float(value) -> bytes:
    f = float(value)
    if math.isnan(f):
        text = "nan"
    elif math.isinf(f):
        text = "inf" if f > 0 else "-inf"
    else:
        text = repr(f)          # кратчайшая форма с round-trip
    return _frame(TAG_FLOAT, text.encode("ascii"))


def _enc_decimal(value) -> bytes:
    d = Decimal(value)
    if d.is_nan():
        text = "nan"
    elif d.is_infinite():
        text = "inf" if d > 0 else "-inf"
    else:
        # нормализуем: 1.50 и 1.5 - одно число, хвостовые нули не значат
        d = d.normalize()
        text = format(d, "f")
        if text in ("-0", "-0.0"):
            text = "0"
    return _frame(TAG_DEC, text.encode("ascii"))


def _enc_str(value) -> bytes:
    return _frame(TAG_STR, str(value).encode("utf-8", "surrogatepass"))


def _enc_bytes(value) -> bytes:
    return _frame(TAG_BYTES, bytes(value))


_TEXT_TRUE = {"1", "true", "t", "yes", "y"}
_TEXT_FALSE = {"0", "false", "f", "no", "n", ""}


def _enc_bool(value) -> bytes:
    if isinstance(value, str):
        # текстовые форматы: PostgreSQL пишет t/f, ClickHouse - 0/1 или
        # true/false; канон один - бит
        s = value.strip().lower()
        if s in _TEXT_TRUE:
            value = True
        elif s in _TEXT_FALSE:
            value = False
        else:
            raise CodecError(f"строка {value!r} не читается как логическое")
    return _frame(TAG_BOOL, b"\x01" if value else b"\x00")


def _ts_ns(value, unit: str) -> int:
    """Мгновение в наносекундах от эпохи UTC.

    Целое читается как число единиц unit (ClickHouse отдает DateTime как
    uint32 секунд). datetime без зоны читается как UTC, с зоной - приводится
    к UTC: одинаковое мгновение дает одинаковые байты.
    """
    if isinstance(value, bool):
        raise CodecError("логическое значение в колонке типа timestamp")
    if isinstance(value, _dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=_dt.timezone.utc)
        delta = value - _EPOCH
        return ((delta.days * 86400 + delta.seconds) * 1_000_000_000
                + delta.microseconds * 1000)
    if isinstance(value, _dt.date):
        return (value - _EPOCH_DATE).days * 86400 * 1_000_000_000
    if isinstance(value, (int,)):
        return int(value) * TIME_UNIT_NS[unit]
    if isinstance(value, float):
        return int(round(value * TIME_UNIT_NS[unit]))
    if isinstance(value, Decimal):
        return int(value * TIME_UNIT_NS[unit])
    if isinstance(value, str):
        # Срез финального прогона: текстовые форматы (COPY text/csv у
        # PostgreSQL, CSV/JSONEachRow/TabSeparated у ClickHouse) везут момент
        # строкой "2013-07-01 20:00:05[.ffffff][+00]". Канон тот же, что у
        # datetime: без зоны читается как UTC. Без этой ветки сверка
        # текстовой клетки с эталоном таблицы была бы невозможна по
        # построению, а не по расхождению данных.
        return _ts_ns(_parse_text_datetime(value), unit)
    raise CodecError(f"значение {type(value).__name__} не читается как момент "
                     "времени")


def _parse_text_datetime(text: str) -> _dt.datetime:
    s = text.strip()
    if not s:
        raise CodecError("пустая строка вместо момента времени")
    # ClickHouse и PostgreSQL пишут пробел между датой и временем; ISO 8601
    # допускает T. Суффикс зоны '+00' (PostgreSQL timestamptz) - без минут,
    # fromisoformat 3.11 его не читает
    s = s.replace("T", " ", 1) if "T" in s and " " not in s else s
    if len(s) >= 3 and s[-3] in "+-" and s[-2:].isdigit():
        s = s + ":00"
    try:
        return _dt.datetime.fromisoformat(s)
    except ValueError as exc:
        raise CodecError(f"строка {text!r} не читается как момент времени: {exc}")


def _enc_ts(value, unit: str) -> bytes:
    return _frame(TAG_TS, str(_ts_ns(value, unit)).encode("ascii"))


def _enc_date(value) -> bytes:
    if isinstance(value, _dt.datetime):
        value = value.date()
    if isinstance(value, _dt.date):
        days = (value - _EPOCH_DATE).days
    elif isinstance(value, int) and not isinstance(value, bool):
        days = int(value)
    elif isinstance(value, str):
        # текстовые форматы везут дату строкой "2013-07-01"
        try:
            days = (_dt.date.fromisoformat(value.strip()) - _EPOCH_DATE).days
        except ValueError as exc:
            raise CodecError(f"строка {value!r} не читается как дата: {exc}")
    else:
        raise CodecError(f"значение {type(value).__name__} не читается как дата")
    return _frame(TAG_DATE, str(days).encode("ascii"))


def _enc_time(value) -> bytes:
    if isinstance(value, _dt.time):
        ns = ((value.hour * 3600 + value.minute * 60 + value.second)
              * 1_000_000_000 + value.microsecond * 1000)
    elif isinstance(value, _dt.timedelta):
        ns = (value.days * 86400 + value.seconds) * 1_000_000_000 \
            + value.microseconds * 1000
    elif isinstance(value, int) and not isinstance(value, bool):
        ns = int(value)
    else:
        raise CodecError(f"значение {type(value).__name__} не читается как "
                         "время суток")
    return _frame(TAG_TIME, str(ns).encode("ascii"))


def _enc_list(value, col: Column) -> bytes:
    inner = b"".join(_encode_value(v, _AUTO_COL) for v in value)
    return _frame(TAG_LIST, inner)


def _enc_map(value, col: Column) -> bytes:
    items = []
    for k in value:
        items.append((_encode_value(k, _AUTO_COL),
                      _encode_value(value[k], _AUTO_COL)))
    items.sort(key=lambda kv: kv[0])     # порядок ключей не значащий
    return _frame(TAG_MAP, b"".join(k + v for k, v in items))


def _encode_auto(value) -> bytes:
    """Канонизация без объявленного типа: тег берется от типа значения.

    Порядок проверок значащий: bool - подкласс int, а memoryview/bytearray -
    не bytes. "auto" годится для клеток, где логическая схема не объявлена
    (служебные пробы), но для зачетной сверки путей схема обязательна:
    только она приводит uint32 ClickHouse и datetime psycopg к одному
    мгновению.
    """
    if value is None:
        return _frame(TAG_NULL, b"")
    if isinstance(value, bool):
        return _enc_bool(value)
    if isinstance(value, int):
        return _enc_int(value)
    if isinstance(value, float):
        return _enc_float(value)
    if isinstance(value, Decimal):
        return _enc_decimal(value)
    if isinstance(value, str):
        return _enc_str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return _enc_bytes(value)
    if isinstance(value, _uuid.UUID):
        # у UUID свой тег: строковая форма совпала бы со строкой того же
        # текста, а это разные значения разных типов
        return _frame(TAG_UUID, value.bytes)
    if isinstance(value, (_ipaddress.IPv4Address, _ipaddress.IPv6Address)):
        return _frame(TAG_IP, value.packed)
    if isinstance(value, (_ipaddress.IPv4Network, _ipaddress.IPv6Network)):
        return _frame(TAG_IP, value.network_address.packed
                      + bytes([value.prefixlen]))
    if isinstance(value, _dt.datetime):
        return _enc_ts(value, "us")
    if isinstance(value, _dt.date):
        return _enc_date(value)
    if isinstance(value, (_dt.time, _dt.timedelta)):
        return _enc_time(value)
    if isinstance(value, dict):
        return _enc_map(value, _AUTO_COL)
    if isinstance(value, (list, tuple, set, frozenset)):
        seq = sorted(value, key=repr) if isinstance(value, (set, frozenset)) \
            else value
        return _enc_list(seq, _AUTO_COL)
    raise CodecError(f"значение типа {type(value).__name__} не канонизируется")


def _encode_value(value, col: Column) -> bytes:
    """Канонизировать значение по объявленному логическому типу колонки."""
    if value is None:
        return _frame(TAG_NULL, b"")
    kind = col.logical
    if kind == "auto":
        return _encode_auto(value)
    try:
        if kind == "int":
            if isinstance(value, bool):
                raise CodecError("логическое значение в колонке типа int")
            return _enc_int(value)
        if kind == "float":
            return _enc_float(value)
        if kind == "decimal":
            return _enc_decimal(value)
        if kind == "string":
            if isinstance(value, (bytes, bytearray, memoryview)):
                return _frame(TAG_STR, bytes(value))
            return _enc_str(value)
        if kind == "bytes":
            if isinstance(value, str):
                return _frame(TAG_BYTES, value.encode("utf-8", "surrogatepass"))
            return _enc_bytes(value)
        if kind == "bool":
            return _enc_bool(value)
        if kind == "timestamp":
            return _enc_ts(value, col.unit or "us")
        if kind == "date":
            return _enc_date(value)
        if kind == "time":
            return _enc_time(value)
        if kind == "list":
            return _enc_list(value, col)
        if kind == "map":
            return _enc_map(value, col)
    except CodecError:
        raise
    except Exception as exc:                       # noqa: BLE001
        raise CodecError(f"колонка {col.name!r} ({kind}): значение "
                         f"{type(value).__name__} не канонизируется: {exc}")
    raise CodecError(f"колонка {col.name!r}: тип {kind!r} не поддержан")


_AUTO_COL = Column("__auto__", "auto")


def encode_row(row, schema: LogicalSchema = None) -> bytes:
    """Канонический байтовый образ одной записи."""
    if isinstance(row, dict):
        if schema is None:
            keys = sorted(row, key=str)
            return b"".join(_encode_value(row[k], _AUTO_COL) for k in keys)
        return b"".join(_encode_value(row.get(c.name), c) for c in schema)
    if not isinstance(row, (list, tuple)):
        row = (row,)
    if schema is None:
        return b"".join(_encode_auto(v) for v in row)
    if len(row) != len(schema):
        raise CodecError(f"в записи {len(row)} полей, в объявленной схеме "
                         f"{len(schema)}")
    return b"".join(_encode_value(v, c) for v, c in zip(row, schema.columns))


def row_hash(row, schema: LogicalSchema = None) -> bytes:
    """sha256 канонического образа записи."""
    return hashlib.sha256(encode_row(row, schema)).digest()


# ------------------------------------------- digest мультимножества ---------

class DigestAccumulator:
    """Потоковый digest мультимножества записей.

    Сумма хешей записей по модулю 2^256 плюс счетчик строк. Сложение
    коммутативно, поэтому digest не зависит ни от порядка строк, ни от
    разбиения на батчи; потеря строки и дубль строки его меняют.

    Проверочный проход потоковый по построению: аккумулятор не держит ни
    одной записи - только два целых числа.
    """

    __slots__ = ("_sum", "_rows", "schema", "codec_id", "mode")

    def __init__(self, schema: LogicalSchema = None,
                 mode: str = CODEC_MODE_FULL):
        self._sum = 0
        self._rows = 0
        self.schema = LogicalSchema.parse(schema) if schema is not None else None
        self.codec_id = CODEC_ID
        self.mode = mode

    @property
    def rows(self) -> int:
        return self._rows

    def add_row(self, row) -> None:
        self.add_hash(row_hash(row, self.schema))

    def add_hash(self, digest_bytes: bytes) -> None:
        self._sum = (self._sum + int.from_bytes(digest_bytes, "big")) % _MOD
        self._rows += 1

    def add_rows(self, rows) -> None:
        schema = self.schema
        acc = self._sum
        n = self._rows
        sha = hashlib.sha256
        for row in rows:
            acc = (acc + int.from_bytes(
                sha(encode_row(row, schema)).digest(), "big")) % _MOD
            n += 1
        self._sum = acc
        self._rows = n

    def merge(self, other: "DigestAccumulator") -> None:
        """Слить частичный digest другого батча или потока."""
        if other.codec_id != self.codec_id:
            raise CodecError(f"digest другого кодека: {other.codec_id} против "
                             f"{self.codec_id}")
        self._sum = (self._sum + other._sum) % _MOD
        self._rows += other._rows

    def hexdigest(self) -> str:
        return f"{self._sum:064x}"

    def value(self) -> str:
        """Строка для сырья и паспорта: кодек, сумма, число строк."""
        return f"{self.codec_id}:{self.mode}:{self.hexdigest()}:{self._rows}"

    def __eq__(self, other):
        return (isinstance(other, DigestAccumulator)
                and self.codec_id == other.codec_id
                and self._sum == other._sum and self._rows == other._rows)

    def __repr__(self):
        return f"DigestAccumulator({self.value()})"


def digest_rows(rows, schema=None, mode: str = CODEC_MODE_FULL) -> str:
    """Эталонная построчная реализация digest мультимножества.

    Принимает list/итератор кортежей, списков, словарей и скаляров, а также
    объекты pyarrow с to_pylist. Возвращает строку вида
    "codec/v1:full:<64 hex>:<число строк>".
    """
    if hasattr(rows, "to_pylist"):
        rows = rows.to_pylist()
    acc = DigestAccumulator(schema, mode=mode)
    acc.add_rows(rows)
    return acc.value()


# ------------------------------------------- векторная реализация Arrow -----

def _np():
    import numpy as np
    return np


def _pa():
    import pyarrow as pa
    return pa


def _column_content_binary(array, col: Column):
    """Содержимое поля колонки как Arrow binary: без тега и длины.

    Все преобразования - ядрами pyarrow и numpy, ни одного цикла по строкам:
    на 10M x 105 построчное форматирование не помещается в окно сессии
    (prelaunch-ревью, B04). Результат обязан байт в байт совпадать с
    эталонной реализацией - это и проверяется тестами.
    """
    pa = _pa()
    pc = __import__("pyarrow.compute", fromlist=["compute"])
    kind = col.logical

    if kind == "auto":
        kind = _logical_from_arrow(array.type)

    if kind == "timestamp":
        ns = _timestamp_ns_array(array, col)
        return pc.cast(pc.cast(ns, pa.string()), pa.binary()), TAG_TS
    if kind == "date":
        if pa.types.is_date32(array.type) or pa.types.is_date64(array.type):
            days = pc.cast(pc.cast(array, pa.date32()), pa.int32())
        elif pa.types.is_timestamp(array.type):
            # D52: дата, приехавшая моментом полуночи (нормализованный
            # датафрейм держит date как datetime64[us, UTC]) - дни от эпохи
            # целочисленным делением наносекунд, как _enc_date(datetime)
            # делает построчно через .date(). До этой ветки cast в int64
            # давал микросекунды вместо дней, и digest конечного фрейма
            # расходился с эталоном на любой таблице с колонкой date
            ns = _timestamp_ns_array(array, col)
            days = pc.divide(ns, pa.scalar(86_400_000_000_000, pa.int64()))
        else:
            days = pc.cast(array, pa.int64())
        return pc.cast(pc.cast(days, pa.string()), pa.binary()), TAG_DATE
    if kind == "time":
        ns = _time_ns_array(array)
        return pc.cast(pc.cast(ns, pa.string()), pa.binary()), TAG_TIME
    if kind == "int":
        ints = pc.cast(array, pa.int64()) if not pa.types.is_integer(array.type) \
            else array
        return pc.cast(pc.cast(ints, pa.string()), pa.binary()), TAG_INT
    if kind == "bool":
        # NULL обязан остаться NULL: _framed_buffer подменит ему тег на N,
        # а превращение его в b"\x00" слило бы NULL и false
        vals = pc.cast(array, pa.bool_()).to_pylist()
        raw = [None if v is None else (b"\x01" if v else b"\x00") for v in vals]
        return pa.array(raw, type=pa.binary()), TAG_BOOL
    if kind == "string":
        # Колонка УЖЕ байтовая - берем байты как есть, без приведения к
        # строке. Текстовые колонки ClickBench (Title, URL, Referer,
        # SearchPhrase) лежат в parquet как binary и НЕ всегда валидный
        # UTF-8: приведение binary -> string на таких значениях либо падает
        # ("Invalid UTF8 payload"), либо молча зависит от версии ядра.
        # Эталонная построчная реализация на bytes в колонке типа string
        # тоже берет байты как есть (_encode_value), поэтому обе ветки
        # совпадают побайтово - это и проверяется тестами
        if pa.types.is_binary(array.type) or pa.types.is_large_binary(
                array.type):
            return array, TAG_STR
        return pc.cast(pc.cast(array, pa.string()), pa.binary()), TAG_STR
    if kind == "bytes":
        return pc.cast(array, pa.binary()), TAG_BYTES
    # float и decimal канонизируются построчно: короткая форма float с
    # round-trip и нормализация Decimal ядрами Arrow не выражаются
    enc = _encode_value
    vals = array.to_pylist()
    out = [None if v is None else enc(v, col)[5:] for v in vals]
    tag = TAG_FLOAT if kind == "float" else TAG_DEC
    return pa.array(out, type=pa.binary()), tag


def _timestamp_ns_array(array, col: Column):
    """Колонка мгновений в наносекундах от эпохи UTC, векторно."""
    pa = _pa()
    pc = __import__("pyarrow.compute", fromlist=["compute"])
    t = array.type
    if pa.types.is_timestamp(t):
        # приведение к ns учитывает единицу самой колонки; зона у Arrow -
        # ярлык над теми же UTC-тиками, поэтому пересчета не требует
        return pc.cast(pc.cast(array, pa.timestamp("ns")), pa.int64())
    if pa.types.is_date32(t) or pa.types.is_date64(t):
        days = pc.cast(pc.cast(array, pa.date32()), pa.int64())
        return pc.multiply(days, pa.scalar(86400 * 1_000_000_000, pa.int64()))
    if pa.types.is_integer(t):
        mult = TIME_UNIT_NS[col.unit or "us"]
        return pc.multiply(pc.cast(array, pa.int64()),
                           pa.scalar(mult, pa.int64()))
    raise CodecError(f"колонка {col.name!r}: тип Arrow {t} не читается как "
                     "момент времени")


def _time_ns_array(array):
    pa = _pa()
    pc = __import__("pyarrow.compute", fromlist=["compute"])
    t = array.type
    if pa.types.is_time32(t) or pa.types.is_time64(t):
        return pc.cast(pc.cast(array, pa.time64("ns")), pa.int64())
    return pc.cast(array, pa.int64())


def _logical_from_arrow(t) -> str:
    pa = _pa()
    if pa.types.is_boolean(t):
        return "bool"
    if pa.types.is_integer(t):
        return "int"
    if pa.types.is_floating(t):
        return "float"
    if pa.types.is_decimal(t):
        return "decimal"
    if pa.types.is_timestamp(t):
        return "timestamp"
    if pa.types.is_date(t):
        return "date"
    if pa.types.is_time(t):
        return "time"
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return "bytes"
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "string"
    raise CodecError(f"тип Arrow {t} не отображается в логический тип; "
                     "объявите схему клетки явно")


def _framed_buffer(content_array, tag: bytes):
    """Колонка закодированных полей: тег + длина + содержимое, векторно.

    Возвращает (offsets, data) как numpy-массивы: смещения int64 и байты
    uint8. NULL кодируется тегом N и нулевой длиной - независимо от того,
    какой тег у колонки: пустая строка и NULL обязаны различаться.
    """
    np = _np()
    pa = _pa()
    if not (pa.types.is_binary(content_array.type)
            or pa.types.is_large_binary(content_array.type)):
        content_array = content_array.cast(pa.binary())
    if isinstance(content_array, pa.ChunkedArray):
        content_array = content_array.combine_chunks()
    arr = content_array
    n = len(arr)
    bufs = arr.buffers()
    validity, off_buf, data_buf = bufs[0], bufs[1], bufs[2]
    off_dtype = np.int64 if pa.types.is_large_binary(arr.type) else np.int32
    offsets = np.frombuffer(off_buf, dtype=off_dtype,
                            count=n + 1, offset=arr.offset
                            * np.dtype(off_dtype).itemsize).astype(np.int64)
    data = np.frombuffer(data_buf, dtype=np.uint8) if data_buf is not None \
        else np.zeros(0, dtype=np.uint8)
    lens = (offsets[1:] - offsets[:-1]).astype(np.int64)

    tags = np.full(n, tag[0], dtype=np.uint8)
    if arr.null_count:
        mask = np.zeros(n, dtype=bool)
        valid = np.frombuffer(validity, dtype=np.uint8)
        bits = np.unpackbits(valid, bitorder="little")
        mask[:] = bits[arr.offset:arr.offset + n].astype(bool)
        lens = np.where(mask, lens, 0)
        tags = np.where(mask, tags, TAG_NULL[0]).astype(np.uint8)
    else:
        mask = np.ones(n, dtype=bool)

    new_lens = lens + 5
    new_off = np.zeros(n + 1, dtype=np.int64)
    np.cumsum(new_lens, out=new_off[1:])
    out = np.empty(int(new_off[-1]), dtype=np.uint8)
    starts = new_off[:-1]
    out[starts] = tags
    be = lens.astype(">u4").view(np.uint8).reshape(n, 4)
    out[(starts[:, None] + np.arange(1, 5)[None, :]).ravel()] = be.ravel()
    total = int(lens.sum())
    if total:
        # позиции содержимого: старт поля + 5 + смещение внутри значения
        src_starts = np.where(mask, offsets[:-1], 0)
        rep_dst = np.repeat(starts + 5, lens)
        rep_src = np.repeat(src_starts, lens)
        within = np.arange(total, dtype=np.int64) - np.repeat(
            np.concatenate(([0], np.cumsum(lens)[:-1])), lens)
        out[rep_dst + within] = data[rep_src + within]
    return new_off, out


def _batch_row_hashes(batch, schema: LogicalSchema):
    """sha256 каждой записи батча Arrow; список байтовых хешей."""
    np = _np()
    cols = list(batch.columns)
    if schema is not None and len(cols) != len(schema):
        raise CodecError(f"в батче {len(cols)} колонок, в объявленной схеме "
                         f"{len(schema)}")
    n = batch.num_rows
    pieces = []
    for i, arr in enumerate(cols):
        col = schema.columns[i] if schema is not None else Column(
            batch.schema.field(i).name, "auto")
        content, tag = _column_content_binary(arr, col)
        pieces.append(_framed_buffer(content, tag))

    # склейка полей в запись: строим общий буфер строки по смещениям колонок
    row_lens = np.zeros(n, dtype=np.int64)
    for off, _ in pieces:
        row_lens += off[1:] - off[:-1]
    row_off = np.zeros(n + 1, dtype=np.int64)
    np.cumsum(row_lens, out=row_off[1:])
    buf = np.empty(int(row_off[-1]), dtype=np.uint8)
    cursor = row_off[:-1].copy()
    for off, data in pieces:
        lens = off[1:] - off[:-1]
        total = int(lens.sum())
        if not total:
            continue
        base = np.concatenate(([0], np.cumsum(lens)[:-1]))
        within = np.arange(total, dtype=np.int64) - np.repeat(base, lens)
        buf[np.repeat(cursor, lens) + within] = data[
            np.repeat(off[:-1], lens) + within]
        cursor += lens

    # memoryview вместо bytes: срез представления не копирует буфер строки,
    # а на 10M записей копия каждой строки - отдельный объект и отдельная
    # аллокация. hashlib принимает представление как есть
    raw = memoryview(buf).cast("B")
    sha = hashlib.sha256
    starts = row_off[:-1].tolist()
    ends = row_off[1:].tolist()
    return [sha(raw[s:e]).digest() for s, e in zip(starts, ends)]


# Сколько записей кодировать за один проход векторной реализации.
# Причина числа - память, а не вкус: на один проход строится буфер полей и
# буфер записей, то есть две-три копии закодированных байт батча. Батч в
# 10M строк на 50 колонках давал 35 ГБ пика и 486 с (подготовительный прогон
# 2026-09-15, ch), нарезка на 100k строк снимает и то, и другое. Величина
# перекрывается переменной окружения на случай очень широких строк.
CHUNK_ROWS = int(os.environ.get("BENCH_CODEC_CHUNK_ROWS", "100000") or 100000)


def digest_arrow(source, schema=None, mode: str = CODEC_MODE_FULL,
                 accumulator: DigestAccumulator = None,
                 chunk_rows: int = None):
    """Векторная реализация digest для Arrow.

    source: pyarrow.Table, RecordBatch, RecordBatchReader или итератор батчей.
    Возвращает строку digest; если передан accumulator, наполняет его и
    возвращает его же значение (потоковая сверка без материализации выдачи).
    """
    pa = _pa()
    schema = LogicalSchema.parse(schema) if schema is not None else None
    acc = accumulator if accumulator is not None else DigestAccumulator(
        schema, mode=mode)
    if isinstance(source, pa.Table):
        batches = source.to_batches()
    elif isinstance(source, pa.RecordBatch):
        batches = [source]
    else:
        batches = source
    limit = int(chunk_rows or CHUNK_ROWS)
    for batch in batches:
        if isinstance(batch, pa.Table):
            sub = batch.to_batches()
        else:
            sub = [batch]
        for b in sub:
            if b.num_rows == 0:
                continue
            # нарезка на куски: батч приходит каким угодно (у Arrow-путей это
            # часто ВСЯ таблица одним куском), а память векторного прохода
            # растет с его размером линейно и в несколько копий
            for start in range(0, b.num_rows, limit):
                piece = b.slice(start, min(limit, b.num_rows - start))
                for h in _batch_row_hashes(piece, acc.schema or schema):
                    acc.add_hash(h)
    return acc.value()


# ------------------------------------------------- подпись схемы результата -

def schema_signature_arrow(obj) -> str:
    """Подпись схемы Arrow: имена, типы, nullability, единицы времени, зона.

    Одного названия "Arrow Table" недостаточно для утверждения об одинаковом
    объекте (D43, README пакета): численная пара публикуется только при
    совпадении этой подписи у обоих плеч.
    """
    pa = _pa()
    schema = obj if isinstance(obj, pa.Schema) else obj.schema
    parts = []
    for field in schema:
        t = field.type
        desc = str(t)
        if pa.types.is_timestamp(t):
            desc = f"timestamp[{t.unit},tz={t.tz or '-'}]"
        elif pa.types.is_time32(t) or pa.types.is_time64(t):
            desc = f"time[{t.unit}]"
        elif pa.types.is_decimal(t):
            desc = f"decimal({t.precision},{t.scale})"
        parts.append(f"{field.name}:{desc}:{'null' if field.nullable else 'notnull'}")
    return "arrow/v1|" + ";".join(parts)


def schema_signature_dataframe(df) -> str:
    """Подпись схемы DataFrame: порядок колонок и карта типов."""
    parts = []
    for name in df.columns:
        dtype = df.dtypes[name]
        parts.append(f"{name}:{dtype!s}")
    return "dataframe/v1|" + ";".join(parts)


def schema_signature_tuples(rows, names=None, sample: int = 1) -> str:
    """Подпись кортежной выдачи: перепись типов по колонкам.

    Типы снимаются с первых непустых значений каждой колонки: у DB-API
    объявленной схемы результата нет, есть только description и сами объекты.
    """
    rows = list(rows[:max(sample, 1)]) if isinstance(rows, list) else \
        [r for _, r in zip(range(max(sample, 1)), rows)]
    if not rows:
        return "tuples/v1|"
    width = len(rows[0]) if isinstance(rows[0], (list, tuple)) else 1
    types = []
    for i in range(width):
        seen = "null"
        for row in rows:
            v = row[i] if isinstance(row, (list, tuple)) else row
            if v is not None:
                seen = type(v).__name__
                break
        name = names[i] if names and i < len(names) else f"c{i}"
        types.append(f"{name}:{seen}")
    return "tuples/v1|" + ";".join(types)


def schema_signature(obj, names=None) -> str:
    """Подпись схемы результата по фактическому типу объекта."""
    if hasattr(obj, "schema") and hasattr(obj, "num_rows"):
        return schema_signature_arrow(obj)
    if hasattr(obj, "dtypes") and hasattr(obj, "columns"):
        return schema_signature_dataframe(obj)
    return schema_signature_tuples(obj, names=names)
