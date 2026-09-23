"""Разбор RowBinaryWithNamesAndTypes ClickHouse для сверки фактического
потока вне окна замера (D52, ревью 2026-09-19 п.3). Модуль без тяжелых
импортов: его зовут и путь ch_http, и тесты.
"""
# D52, ревью 2026-09-19 п.3: сверка через Native подтверждала данные запроса,
# но не корректность полученного RowBinary. Теперь поток разбирается сам,
# вне окна замера (режим digest), по типам из его собственного заголовка.
# Поддержаны типы таблиц cb_*: целые, String, FixedString(N), DateTime,
# Date, Float32/64; Nullable и составные типы - отказ вслух.
_RB_INT = {"Int8": ("<b", 1), "UInt8": ("<B", 1), "Int16": ("<h", 2),
           "UInt16": ("<H", 2), "Int32": ("<i", 4), "UInt32": ("<I", 4),
           "Int64": ("<q", 8), "UInt64": ("<Q", 8),
           "Float32": ("<f", 4), "Float64": ("<d", 8),
           "DateTime": ("<I", 4), "Date": ("<H", 2)}


def _rb_varuint(buf, pos):
    shift, val = 0, 0
    while True:
        b = buf[pos]
        pos += 1
        val |= (b & 0x7F) << shift
        if not b & 0x80:
            return val, pos
        shift += 7


def _rb_string(buf, pos):
    n, pos = _rb_varuint(buf, pos)
    return buf[pos:pos + n], pos + n


def parse_rowbinary_with_names_and_types(blob):
    """(имена, типы, строки) из потока RowBinaryWithNamesAndTypes.

    Целые и моменты отдаются целыми (DateTime - секунды, Date - дни): кодек
    канонизирует их по объявленной схеме так же, как значения из Native.
    Строки - bytes (кодек в колонке string берет байты как есть).
    """
    import struct                                          # noqa: PLC0415
    buf = memoryview(bytes(blob))
    pos = 0
    ncols, pos = _rb_varuint(buf, pos)
    names, types = [], []
    for _ in range(ncols):
        s, pos = _rb_string(buf, pos)
        names.append(bytes(s).decode("utf-8"))
    for _ in range(ncols):
        s, pos = _rb_string(buf, pos)
        types.append(bytes(s).decode("utf-8"))
    readers = []
    for tname in types:
        # DateTime('UTC') - тот же UInt32 секунд; DateTime64 и Nullable -
        # других типов у cb_* нет, отказ вслух
        if tname.startswith("DateTime(") and not tname.startswith("DateTime64"):
            tname = "DateTime"
        if tname in _RB_INT:
            fmt, size = _RB_INT[tname]
            st = struct.Struct(fmt)
            readers.append(("fixed", st, size))
        elif tname == "String":
            readers.append(("string", None, 0))
        elif tname.startswith("FixedString("):
            n = int(tname[len("FixedString("):-1])
            readers.append(("fixedstr", None, n))
        elif tname.startswith("LowCardinality("):
            raise SystemExit(f"RowBinary: тип {tname} в потоке - словарного "
                             "кодирования у cb_* нет, разбор не объявлен")
        else:
            raise SystemExit(f"RowBinary: тип {tname} не поддержан разбором сверки")
    rows = []
    total = len(buf)
    while pos < total:
        row = []
        for kind, st, size in readers:
            if kind == "fixed":
                row.append(st.unpack_from(buf, pos)[0])
                pos += size
            elif kind == "string":
                s, pos = _rb_string(buf, pos)
                row.append(bytes(s))
            else:
                row.append(bytes(buf[pos:pos + size]))
                pos += size
        rows.append(tuple(row))
    return names, types, rows


