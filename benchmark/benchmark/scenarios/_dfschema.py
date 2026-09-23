"""Объявленная схема DataFrame - один и тот же полезный финиш у разных путей (E1).

Независимое ревью 2026-09-13 (раздел 5, E1): пути до Arrow и до кортежей
отдают РАЗНЫЕ объекты и разные типы времени - uint32 у Arrow-путей
ClickHouse, datetime у кортежей, timestamp[us] у ADBC PostgreSQL. Сравнение
"до одинакового полезного результата" требует схемы, объявленной ДО опыта:
здесь она задана по имени (BENCH_DF_SCHEMA), а приведение к ней входит в
окно wall клетки; его цена пишется отдельно (extra2_s). Гейт E1 сверяет
хеши нормализованных датафреймов двух путей полосы.

Правила приведения:
  int64   - astype("int64") (int16/int32/uint у Arrow, object у кортежей);
  str     - object-колонка питоновских строк (Arrow string -> object);
  datetime64[us, UTC] - момент времени в UTC с точностью до микросекунды:
            целые числа читаются как секунды эпохи (uint32 DateTime ClickHouse
            в Arrow), наивные datetime - как UTC (стенд в UTC по построению,
            гейт зоны), осведомленные - приводятся к UTC.
Порядок колонок - как объявлено; отсутствие объявленной колонки - ошибка
клетки (класс df_schema), а не тихое пропускание.
"""
import json
import os
import time
from pathlib import Path

import pandas as pd

SCHEMAS = {
    # cb_w10_*: десятка hits_w10 (COLS10 из bench/29-g3-build-subsets.py)
    "e1": (
        ("WatchID", "int64"), ("JavaEnable", "int64"), ("Title", "str"),
        ("EventTime", "datetime64[us, UTC]"), ("CounterID", "int64"),
        ("ClientIP", "int64"), ("RegionID", "int64"), ("UserID", "int64"),
        ("URL", "str"), ("Referer", "str"),
    ),
}

class DfSchemaError(ValueError):
    """Датафрейм не привести к объявленной схеме (нет колонки, чужой тип)."""


# --- широкие схемы: задел под C2 -------------------------------------------
# Геометрия C1 ставит ширины только на 10M и только до Arrow и кортежей, но
# полное пересечение C2 требует тех же DataFrame-клеток на w50 и w105.
# Объявлять пятьдесят и сто пять колонок руками нельзя: список разъедется с
# манифестом раздачи при первой же правке. Поэтому широкие схемы ВЫВОДЯТСЯ из
# того же файла логических схем, по которому считаются эталоны таблиц
# (analysis/run-profiles/focused-talk/logical-schemas.json), и отображение
# логического типа в объявление DataFrame здесь одно на все ширины.
_LOGICAL_TO_DF = {
    "int": "int64",
    "string": "str",
    "bytes": "str",
    "timestamp": "datetime64[us, UTC]",
    "date": "datetime64[us, UTC]",
    "bool": "int64",
    "float": "float64",
}


def _logical_schemas_path():
    env = os.environ.get("STANDF_LOGICAL_SCHEMAS", "").strip()
    if env:
        return Path(env)
    here = Path(__file__).resolve().parent            # standf/scenarios
    return (here.parent.parent / "analysis" / "run-profiles" / "focused-talk"
            / "logical-schemas.json")


def load_width_schemas(path=None) -> dict:
    """Схемы e1_w50 и e1_w105 из объявленных логических схем ширин.

    Возвращает то, что добавлено. Отсутствие файла - не ошибка: широкие
    DataFrame-клетки в C1 не исполняются, и без файла модуль обязан работать
    как прежде.
    """
    p = Path(path) if path else _logical_schemas_path()
    try:
        doc = json.loads(p.read_text(encoding="utf-8"))
    except OSError:
        return {}
    added = {}
    for width, body in (doc.get("widths") or {}).items():
        if width == "w10":
            continue                       # десятка уже объявлена как e1
        cols = []
        for col in body["columns"]:
            decl = _LOGICAL_TO_DF.get(col["logical"])
            if decl is None:
                raise DfSchemaError(
                    f"ширина {width}, колонка {col['name']}: логический тип "
                    f"{col['logical']!r} не отображается в объявление "
                    "DataFrame - допишите _LOGICAL_TO_DF осознанно")
            cols.append((col["name"], decl))
        name = f"e1_{width}"
        SCHEMAS[name] = tuple(cols)
        added[name] = len(cols)
    return added


def _to_utc_us(series: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(series):
        tz = getattr(series.dt, "tz", None)
        series = series.dt.tz_localize("UTC") if tz is None else series.dt.tz_convert("UTC")
        return series.astype("datetime64[us, UTC]")
    if pd.api.types.is_integer_dtype(series):
        # секунды эпохи: DateTime ClickHouse в Arrow/ADBC приезжает как uint32
        return pd.to_datetime(series.astype("int64"), unit="s", utc=True).astype("datetime64[us, UTC]")
    # object: datetime без зоны (psycopg, clickhouse-driver) или строки
    return pd.to_datetime(series, utc=True).astype("datetime64[us, UTC]")


def normalize_df(df: pd.DataFrame, name: str):
    """Привести df к схеме name. Возвращает (новый DataFrame, секунды).

    Кадр собирается через pd.concat, а НЕ конструктором из словаря готовых
    Series. Разница не стилистическая: конструктор консолидирует колонки в
    блоки numpy, и на 105 колонках это 0.95 с против 0.08 с у concat при
    побитово одинаковых значениях - то есть девять десятых всей цены
    приведения. В корпусе C1+C2+C3 этот шаг стоил 50 секунд на клетке
    w105/10m, и цитировался он как цена приведения типов, хотя приведение
    самих типов там 0.04 с (F80).
    """
    if name not in SCHEMAS:
        raise DfSchemaError(f"схема {name!r} не объявлена: {sorted(SCHEMAS)}")
    schema = SCHEMAS[name]
    t = time.perf_counter()
    # имена колонок сверяются без учета регистра: PostgreSQL складывает
    # незакавыченные идентификаторы в нижний регистр (watchid, eventtime),
    # ClickHouse и Arrow отдают как объявлено; на выходе - объявленные имена
    by_lower = {str(c).lower(): c for c in df.columns}
    missing = [c for c, _ in schema if c.lower() not in by_lower]
    if missing:
        raise DfSchemaError(f"в датафрейме нет объявленных колонок {missing} "
                            f"(есть {list(df.columns)})")
    out = []
    for col, decl in schema:
        series = df[by_lower[col.lower()]]
        try:
            if decl == "int64":
                series = series.astype("int64")
            elif decl == "float64":
                series = series.astype("float64")
            elif decl == "str":
                series = series.astype(object) if series.dtype != object else series
            elif decl.startswith("datetime64"):
                series = _to_utc_us(series)
            else:
                raise DfSchemaError(f"схема {name}: неизвестное объявление {decl!r} у {col}")
        except (TypeError, ValueError, OverflowError) as exc:
            raise DfSchemaError(f"колонка {col}: не привести к {decl}: {exc}") from exc
        out.append(series.rename(col))
    result = pd.concat(out, axis=1)
    # имена ставятся ЯВНО: у исходных Series они могли отличаться регистром,
    # а concat взял бы имя Series, а не объявленное
    result.columns = [c for c, _ in schema]
    return result, time.perf_counter() - t


# --- приведение фрейма polars ----------------------------------------------
# Зачем отдельная ветка. Ось целевой структуры сравнивает pandas и polars на
# ОДНОМ и том же пути: плечи обязаны отличаться ровно целевой структурой и
# ничем больше. До этой ветки клетка polars отдавала фрейм как есть, а клетка
# pandas приводила его к объявленной схеме - и приведение на w105/10m стоит
# 50-56 секунд у ЛЮБОГО пути, вшестеро дороже самой конверсии Arrow -> pandas
# (9.3 с). Сравнив их так, мы бы "доказали" превосходство polars, просто
# выбросив у него шаг. Поэтому приведение есть у обеих целевых структур, и
# результат обеих обязан совпасть по ЗНАЧЕНИЯМ - это держат тесты.
#
# Отображение объявления в тип polars одно на все ширины, как и _LOGICAL_TO_DF.
_DECL_TO_POLARS = {
    "int64": "Int64",
    "str": "String",
    "float64": "Float64",
    "datetime64[us, UTC]": ("Datetime", "us", "UTC"),
}


def _polars_to_utc_us(pl, series):
    """Момент времени в UTC с точностью до микросекунды - те же правила, что
    у pandas (_to_utc_us), иначе плечи оси сравнивали бы разные значения.

    Целое - секунды эпохи (DateTime ClickHouse приезжает в Arrow как uint32);
    наивный момент - читается как UTC (стенд в UTC по построению, есть гейт
    зоны); осведомленный - переводится в UTC. Дата - полночь UTC.
    """
    want = pl.Datetime("us", "UTC")
    dtype = series.dtype
    if dtype == pl.Date:
        return series.cast(pl.Datetime("us")).dt.replace_time_zone("UTC")
    if isinstance(dtype, pl.Datetime):
        series = (series.dt.replace_time_zone("UTC") if dtype.time_zone is None
                  else series.dt.convert_time_zone("UTC"))
        return series.cast(want)
    if dtype.is_integer():
        # секунды эпохи -> микросекунды умножением: у polars шкала Datetime
        # начинается с миллисекунд, единицы "s" в ней нет вовсе
        return (series.cast(pl.Int64) * 1_000_000).cast(pl.Datetime("us")) \
            .dt.replace_time_zone("UTC")
    if dtype == pl.String:
        return series.str.to_datetime(time_unit="us", time_zone="UTC")
    raise DfSchemaError(f"тип {dtype} не привести к моменту времени UTC")


def normalize_polars(frame, name: str):
    """Привести фрейм polars к схеме name. Возвращает (новый фрейм, секунды).

    Зеркало normalize_df: те же имена колонок без учета регистра, тот же
    порядок, те же правила времени, та же ошибка класса df_schema при
    отсутствии объявленной колонки.
    """
    import polars as pl                                    # noqa: PLC0415

    if name not in SCHEMAS:
        raise DfSchemaError(f"схема {name!r} не объявлена: {sorted(SCHEMAS)}")
    schema = SCHEMAS[name]
    t = time.perf_counter()
    by_lower = {str(c).lower(): c for c in frame.columns}
    missing = [c for c, _ in schema if c.lower() not in by_lower]
    if missing:
        raise DfSchemaError(f"в датафрейме нет объявленных колонок {missing} "
                            f"(есть {list(frame.columns)})")
    out = []
    for col, decl in schema:
        series = frame[by_lower[col.lower()]]
        target = _DECL_TO_POLARS.get(decl)
        if target is None:
            raise DfSchemaError(
                f"схема {name}: неизвестное объявление {decl!r} у {col}")
        try:
            if decl.startswith("datetime64"):
                series = _polars_to_utc_us(pl, series)
            else:
                series = series.cast(getattr(pl, target))
        except Exception as exc:                  # noqa: BLE001 - класс клетки
            raise DfSchemaError(f"колонка {col}: не привести к {decl}: "
                                f"{exc}") from exc
        out.append(series.alias(col))
    result = pl.DataFrame(out)
    return result, time.perf_counter() - t


def declared_polars_dtypes(name: str) -> str:
    """Строка объявленных типов polars - для сверки с client_dtype клетки."""
    import polars as pl                                    # noqa: PLC0415

    names = []
    for _, decl in SCHEMAS[name]:
        target = _DECL_TO_POLARS[decl]
        names.append(str(pl.Datetime("us", "UTC")) if decl.startswith("datetime64")
                     else str(getattr(pl, target)))
    return ";".join(names)


def declared_dtypes(name: str) -> str:
    """Строка объявленных типов - для сверки с client_dtype клетки."""
    return ";".join(("object" if d == "str" else d) for _, d in SCHEMAS[name])


# Широкие схемы подгружаются при импорте: клетка объявляет их по имени
# (BENCH_DF_SCHEMA=e1_w50), и без загрузки имя было бы неизвестно.
load_width_schemas()
