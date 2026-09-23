"""Оси серии: наборы-константы, чтение окружения, валидации, подключения.

ЗАКОН словаря имен - scenarios/CONTRACT.md. Имена путей, переменных, режимов
и настроек берутся только оттуда: раннер и сценарий обязаны говорить на одном
словаре, иначе кейсы молча отбрасываются или едут на дефолтах.

Все осевые переменные читаются ПРИ ИМПОРТЕ этого модуля, и валидации падают
SystemExit тоже при импорте: клетка с кривой осью не должна дойти до замера.
Служебный флаг --list-paths обязан работать при любом окружении - это решается
в точке входа (09_gen_paths.py) ДО импорта осевых модулей, а не ослаблением
валидаций здесь.
"""

import os

# Отменены как мертвые (контракт): BENCH_SRV_COST, BENCH_DIGEST,
# BENCH_BLOCK_SIZE, GEN_PROFILE, GEN_ROWS, GEN_COLS.


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        raise SystemExit(f"{name}={raw!r}: ожидается целое число")


def _env_choice(name: str, default: str, allowed) -> str:
    # пустая строка = переменная не задана: раннер экспортирует все имена
    # разом, и часть из них на конкретном кейсе пустая
    value = os.environ.get(name, "").strip() or default
    if value not in allowed:
        raise SystemExit(f"{name}={value!r}, ожидается одно из {tuple(allowed)}")
    return value


# Режимы замера. Первые пять - питоновские; count и csv умеет только
# Java-клиент (F13/DATA-03, запрос исполнителя java, п. 10): режим csv зовет
# точка гейта идентичности gate2_file_sha (sha256 файла java против выгрузки
# file_export), и без имени в словаре сценарий падал на импорте ДО java, а
# гейт печатал мягкое "точка не отработала" - java-путь не сверялся ни с чем
# молча. Разрешение сужено путем: питоновский путь на BENCH_MODE=csv не
# провалился бы громко, а тихо снял бы материализацию под чужой меткой,
# поэтому проверка стоит в точке входа рядом с реестром путей
# (09_gen_paths._check_java_mode).
JAVA_MODES = ("count", "csv")
MODES = ("drain", "materialize", "digest", "srvcost", "dfgate") + JAVA_MODES
CODECS = ("none", "lz4", "zstd")
TARGETS = ("tuples", "columnar", "arrow", "df", "df_arrowdtype", "np",
           "polars", "raw")
# Реестр профилей холста gen расширяем: сами колонки живут в bench_sql
# (словарь PROFILE_COLS), новый профиль добавляется в ОБА места - сюда имя,
# туда построитель; сверка одинаковости списков делается при импорте bench_sql.
#   g20 / g30 / g75 / g105 - лестница ширины (продолжение схемы g50
#                 дешевыми целыми)
#   entropy     - g50, но строки = ПРЕФИКС md5 той же ширины (несжимаемая
#                 пара при тех же байтах; F12)
#   g10null     - g10 с детерминированной NULL-маской (BENCH_NULL_DENSITY)
#   g_arr       - Array(Int64) длины 10 + скаляры (PG: int8[])
#   g_map       - Map(String,Int64) (PG: jsonb; кросс-движковый sha256
#                 НЕ сходится - клетки только внутридвижковые)
#   g10flat     - те же десять чисел и s16 плоскими колонками: третья нога
#                 тройки контейнеров колонки/массив/словарь (F12)
#   shape_len   - одна строковая колонка длины BENCH_STR_LEN (ось длины строки)
#   g1int / g1i16 / g1f64 / g1decimal / g1dt / g1date / g1uuid / g1bool /
#   g1str1000   - одноколоночные профили прайса типов (d10b)
#   g1decbound  - граничный Decimal(18,4): значения не представимы во float64
#   g10f        - g10 в типах, которые умеет Arrow Flight SQL adapter
#                 PostgreSQL (date -> timestamp, timestamptz -> timestamp в
#                 UTC, numeric -> int8). СКВОЗНОЙ: в PROFILES_INTRA_ENGINE
#                 его нет, кросс-движковый sha256 обязан сходиться -
#                 обоснование в bench_sql._cols_g10f
PROFILES = ("g1", "g10", "g10f", "g20", "g30", "g50", "g75", "g105", "entropy",
            "g10null", "g_arr", "g_map", "g10flat", "shape", "shape_len",
            "g1int", "g1i16", "g1f64", "g1decimal", "g1dt", "g1date",
            "g1uuid", "g1bool", "g1str1000", "g1decbound")
# Профили, у которых кросс-движкового sha256 НЕТ по построению: пара типов
# у движков разная (g_map: Map против jsonb) либо представление
# движко-зависимо (g1f64: float). Их клетки сравниваются только ВНУТРИ
# движка, в гейт 2 они не регистрируются, а сам список печатается
# --list-profiles - чтобы предполет брал правило из кода, а не из
# комментария сетки (F13).
PROFILES_INTRA_ENGINE = ("g_map", "g1f64")


def profile_needs_cross_sha(name: str) -> bool:
    """Обязан ли профиль сойтись побайтово между движками (гейт 2)."""
    return name not in PROFILES_INTRA_ENGINE


# hits_legacy - побайтовая копия старой hits с БИТОЙ датой (перенос дампом,
# только PG): клетки моста -legacy-bridge, в оси и слайды с цифрами не идет
CANVASES = ("gen", "narrow", "hits", "hits_legacy")
CX_PROTOCOLS = ("binary", "csv", "cursor")
# mysqlclient - второй клиент пути ch_mysql_emu (C-разбор против чистого
# Python у pymysql); остальные значения - блок аналитика и файловый маршрут
VARIANTS = ("", "psycopg", "sqlalchemy", "export", "read1", "read2",
            "mysqlclient")
ENGINES = ("", "pg", "ch")

# Кодировка значений и кадрирование в pg-wire: два РАЗНЫХ пути, а не один с
# ветвлением внутри (контракт) - у COPY другое кадрирование и другой набор
# значений BENCH_FMT.
PG_FMTS = ("simple_text", "ext_text", "ext_binary")
PG_COPY_FMTS = ("copy_text", "copy_binary", "copy_csv")
CH_FMTS = ("Native", "RowBinary", "RowBinaryWithNamesAndTypes", "Arrow",
           "ArrowStream", "Parquet", "TabSeparated", "CSV",
           "JSONCompactEachRow", "JSONEachRow")
# Файловые форматы блока аналитика (контракт: BENCH_FMT у пути file_export -
# csv | parquet). Регистр здесь строчный и это не мелочь: у ClickHouse
# FORMAT Parquet - имя формата на проводе, а тут имя формата ФАЙЛА, и путать
# две сущности в одной колонке нельзя.
FILE_FMTS = ("csv", "parquet")
# Форматы без питоновского парсера: только drain, и это честная пустая клетка
CH_NO_PARSER = ("RowBinary", "RowBinaryWithNamesAndTypes", "Values")
# Форматы, у которых сжатие живет ВНУТРИ формата, а не в транспорте
ARROW_FMTS = ("Arrow", "ArrowStream", "Parquet")
# Таблица соответствия кодек-формат (в том числе lz4 -> lz4_frame у Arrow)
# НЕ дублируется здесь: единственный источник - ch_codec_plan из _srvcontrol,
# та же таблица, которую валидирует гейт кодеков раннера. Две независимые
# копии рано или поздно расходятся, и гейт начинает проверять не ту, по
# которой едет серия.

CANVAS = _env_choice("BENCH_CANVAS", "gen", CANVASES)
PROFILE = _env_choice("BENCH_PROFILE", "g10", PROFILES)
ROWS = _env_int("BENCH_ROWS", 1_000_000)
SHAPE = _env_int("BENCH_SHAPE", 10)
WIDTH = os.environ.get("BENCH_WIDTH", "w10")
TABLE = os.environ.get("BENCH_TABLE", "hits_w10_1m")
MODE = _env_choice("BENCH_MODE", "materialize", MODES)
FMT = os.environ.get("BENCH_FMT", "").strip()
CODEC = _env_choice("BENCH_CODEC", "none", CODECS)
TARGET = _env_choice("BENCH_TARGET_STRUCT", "tuples", TARGETS)
# Факт задания отличаем от дефолта - как у BENCH_BATCH и по той же причине.
# Дефолт tuples верен для путей провода: кортеж там и есть то, что отдает
# драйвер. У блока аналитика кортежей нет вовсе, и незаданная переменная
# уронила бы там КАЖДУЮ клетку - см. _analyst_target в paths_analyst.
TARGET_SET = bool(os.environ.get("BENCH_TARGET_STRUCT", "").strip())

# --- уровень транспортного кодека (ось внутри A6) --------------------------
# Уровень - ОСЬ, а не константа. У ClickHouse его задает ОДНА серверная ручка
# http_zlib_compression_level, и она уходит любому методу сжатия HTTP, хотя
# шкалы у методов разные:
#   lz4 (кадровый LZ4F) - 1..12, и шкала НЕ линейная: начиная с 3 библиотека
#     переключается в режим высокого сжатия LZ4HC (LZ4HC_CLEVEL_MIN = 3).
#     Клетки корпуса, снятые уровнем 3, мерили LZ4HC, а не обычный lz4 -
#     отсюда перевернутая картина, где zstd и жмет сильнее, и работает
#     быстрее (отчет серии, раздел 3.2 п. 2);
#   zstd - 1..22, и уровень 3 совпадает с собственным дефолтом библиотеки,
#     то есть zstd-клетки корпуса сняты честной точкой шкалы.
# Дефолт оси в срезе v1.5 - 1, а не 3 (F25, объявленный сдвиг F -> финал).
# Причина: уровень 3 у lz4 - это уже LZ4HC, то есть клетки серии F, снятые
# "обычным lz4", мерили режим высокого сжатия, и картина переворачивалась.
# Держать такой дефолт можно было только пока уровень не попадал в корпус;
# теперь фактический уровень пишется колонкой codec_level (CODEC_LEVEL_COL),
# и клетку любого прогона видно по сырью, а не по имени. Дефолт 1 делает
# слово lz4 в метке правдой: обычный lz4, без HC.
# Факт задания отличаем от дефолта (как у BENCH_BATCH и по той же причине):
# на нем держатся сверка с меткой клетки и запрет уровня там, где ручка
# ничего не меняет.
CODEC_LEVEL_DEFAULT = 1
CODEC_LEVEL_SET = bool(os.environ.get("BENCH_CH_CODEC_LEVEL", "").strip())
CODEC_LEVEL = _env_int("BENCH_CH_CODEC_LEVEL", CODEC_LEVEL_DEFAULT)
# Шкала своя у каждого кодека - общий диапазон был бы ложью в обе стороны:
# 22 у lz4 сервер не примет, 12 у zstd отрезало бы половину шкалы.
CODEC_LEVEL_RANGE = {"lz4": (1, 12), "zstd": (1, 22)}
if CODEC_LEVEL_SET and CODEC == "none":
    raise SystemExit(
        "BENCH_CH_CODEC_LEVEL задан при BENCH_CODEC=none - none-клетка не "
        "сжимается вовсе, задавать ей уровень нечему; такая клетка обещала "
        "бы корпусу точку оси, которой в ней нет")
_LEVEL_LO, _LEVEL_HI = CODEC_LEVEL_RANGE.get(CODEC, (1, 22))
if not _LEVEL_LO <= CODEC_LEVEL <= _LEVEL_HI:
    raise SystemExit(
        f"BENCH_CH_CODEC_LEVEL={CODEC_LEVEL}: у кодека {CODEC} шкала "
        f"{_LEVEL_LO}..{_LEVEL_HI}; вне ее сервер откажет уже внутри окна "
        "замера, а окно короткое")


def _label_codec_level(label: str):
    """Уровень, ОБЪЯВЛЕННЫЙ меткой клетки: сегмент вида lvl1 (контракт).

    Колонки под уровень в схеме CSV нет и не будет (схема заморожена), так
    что в корпус уровень попадает единственным способом - через метку. Значит
    метка и переменная обязаны сойтись: иначе клетка уровня 1 уедет в корпус
    под именем клетки уровня 3, и это ровно та подмена, из-за которой цену
    обычного lz4 пришлось объявить неизмеренной.
    """
    for part in label.split("-"):
        if part.startswith("lvl") and part[3:].isdigit():
            return int(part[3:])
    return None


_LABEL = os.environ.get("MEASURE_LABEL", "").strip()
_DECLARED_LEVEL = _label_codec_level(_LABEL)
# none-пара уровневой подоси несет тот же сегмент lvl в метке - иначе сводка
# и таблицы не найдут ей пару (они спариваются заменой -lz4-/-zstd- на
# -none- в ТОЙ ЖЕ метке). Уровень к ней не применяется по построению, поэтому
# сверять там нечего.
if _LABEL and CODEC != "none" and _DECLARED_LEVEL is not None \
        and _DECLARED_LEVEL != CODEC_LEVEL:
    raise SystemExit(
        f"метка клетки {_LABEL!r} обещает уровень {_DECLARED_LEVEL}, а "
        f"BENCH_CH_CODEC_LEVEL дает {CODEC_LEVEL} - клетка ушла бы в корпус "
        "под чужим именем")
if _LABEL and CODEC_LEVEL_SET and CODEC_LEVEL != CODEC_LEVEL_DEFAULT \
        and _DECLARED_LEVEL is None:
    raise SystemExit(
        f"BENCH_CH_CODEC_LEVEL={CODEC_LEVEL}, а метка клетки {_LABEL!r} про "
        f"уровень молчит - по имени такую клетку не отличить от снятой "
        f"дефолтным уровнем {CODEC_LEVEL_DEFAULT}, а по имени ее и читают "
        f"сетка, сводка и таблицы (колонка codec_level страхует корпус, но "
        f"не метку); сегмент метки - lvl{CODEC_LEVEL}")

# Имя оси по контракту среза v1.5. CODEC_LEVEL остается для уже написанных
# потребителей (bench_runtime, paths_ch), CH_CODEC_LEVEL - то же значение под
# контрактным именем; две переменные с разным значением тут завестись не
# могут по построению.
CH_CODEC_LEVEL = CODEC_LEVEL

# Слой, на котором живет сжатие (ось A6). transport - кодек провода
# (Accept-Encoding у HTTP, network_compression_method у нативного);
# format - кодек ВНУТРИ колоночного формата
# (output_format_arrow_compression_method / output_format_parquet_...).
# Разделять обязательно: у Arrow/Parquet сжатый транспорт и сжатый формат
# дают разные байты и разную работу на обеих сторонах, а метка клетки без
# слоя описывала бы две разные вещи одним словом.
# Какие форматы вообще имеют свой кодек, здесь НЕ перечисляется (см.
# комментарий про таблицу кодек-формат выше): применимость слоя проверяет
# ch_codec_plan из _srvcontrol - тот же код, по которому едет клетка.
CH_CODEC_LAYER = _env_choice("BENCH_CH_CODEC_LAYER", "transport",
                             ("transport", "format"))

# Значение колонки codec_level в строке сырья. Пусто в двух случаях, и оба
# означают "ручки уровня в этой клетке нет": сжатия нет вовсе (codec=none) и
# сжатие живет ВНУТРИ формата - уровень задает единственная транспортная
# ручка сервера (http_zlib_compression_level / network_zstd_compression_level)
# и до форматного слоя не доходит. Ноль вместо пустоты был бы враньем: у
# zstd ноль - точка шкалы.
CODEC_LEVEL_COL = "" if (CODEC == "none" or CH_CODEC_LAYER == "format") \
    else str(CODEC_LEVEL)

EXECS = _env_int("BENCH_EXECS", 1)
PARALLEL_FMT = _env_choice("BENCH_PARALLEL_FMT", "0", ("0", "1"))

# max_threads: на генерации прибит в 1 - иначе порядок строк недетерминирован
# и предполетный гейт по sha256 теряет смысл. На РЕАЛЬНЫХ холстах (hits,
# hits_legacy, narrow) НЕ задается: анкеры сопоставляются с разведочными
# сериями, где потоки были дефолтные.
# F25 / GRID-14: список холстов-исключений - ЕДИНСТВЕННЫЙ источник правды,
# он живет здесь. Раннер не вычисляет max_threads сам и не экспортирует
# BENCH_MAX_THREADS пустой строкой (заданная пустая переменная перебивает
# этот дефолт, и narrow ехал по серверному значению не по решению, а по
# случайности); кому нужен дефолт - не задает переменную вовсе, как у
# BENCH_BATCH и BENCH_TARGET_STRUCT.
MAX_THREADS_SERVER_CANVASES = ("hits", "hits_legacy", "narrow")
MAX_THREADS = os.environ.get(
    "BENCH_MAX_THREADS",
    "" if CANVAS in MAX_THREADS_SERVER_CANVASES else "1").strip()
# Значение остается строкой (пустая = не задавать), но мусор ловится здесь,
# при импорте: int(MAX_THREADS) зовут bench_sql (хвост SETTINGS эмуляций) и
# bench_runtime (_ch_settings), и голый ValueError-трейсбек оттуда не
# объяснил бы, какая переменная кривая
if MAX_THREADS:
    try:
        int(MAX_THREADS)
    except ValueError:
        raise SystemExit(
            f"BENCH_MAX_THREADS={MAX_THREADS!r}: ожидается целое число "
            "или пустая строка (не задавать max_threads)")
# Фактическое значение для строки сырья и паспорта (F25/GRID-14): по 38
# колонкам корпуса F клетку с потолком потоков нельзя было отличить от
# клетки без него, и кросс-сравнения narrow-CH с gen-CH держались на вере.
# Слово server, а не пустая строка: пустая читалась бы как "не знаем".
MAX_THREADS_FACT = MAX_THREADS if MAX_THREADS else "server"

# Размер порции - ОДНО имя на все клиенты (контракт). Факт задания отличаем
# от дефолта: max_block_size у ClickHouse трогаем только когда о нем попросили
# явно, иначе ось размера порции незаметно переехала бы на все кейсы серии.
BATCH_SET = bool(os.environ.get("BENCH_BATCH", "").strip())
BATCH = _env_int("BENCH_BATCH", 10_000)
ITERSIZE = _env_int("BENCH_ITERSIZE", BATCH)

# psycopg: None выключает автоподготовку совсем, 0 готовит с первого раза.
# Пустая строка и слово none - одно и то же (раннеру удобно писать none).
_PREP_RAW = os.environ.get("BENCH_PREPARE_THRESHOLD", "").strip().lower()
PREPARE_THRESHOLD = None if _PREP_RAW in ("", "none", "null") \
    else _env_int("BENCH_PREPARE_THRESHOLD", 5)

CX_PROTOCOL = _env_choice("BENCH_CX_PROTOCOL", "binary", CX_PROTOCOLS)

_ADBC_COPY_RAW = os.environ.get("BENCH_ADBC_COPY", "false").strip().lower()
if _ADBC_COPY_RAW in ("true", "1", "yes"):
    ADBC_COPY = "true"
elif _ADBC_COPY_RAW in ("false", "0", "no"):
    ADBC_COPY = "false"
else:
    raise SystemExit(f"BENCH_ADBC_COPY={_ADBC_COPY_RAW!r}: ожидается true|false")

# Как ADBC довозит numeric (F24 / PATH-04). Пусто - как в серии F: драйвер
# отдает numeric ТЕКСТОМ внутри Arrow (extension arrow.opaque, storage_type=
# string), и клетка d10-g1decimal-adbc под меткой mat-typed называла ценой
# Decimal цену строки. Непустое значение уходит опцией драйвера
# adbc.postgresql.numeric_as (проба, ее ставит paths_pg); клетка сетки -
# t3-decbound-adbc-numasdouble-1000k.
#
# Ось заводится ЗДЕСЬ, а не в пути: до этой правки она читалась прямо в
# paths_pg, и по строке сырья клетку с numeric-as-double нельзя было отличить
# от обычной ничем, кроме метки (регресс REG-07, запрос impl-grid-r2 п. 16).
# Дефолт обязан остаться пустым: на нем сняты клетки серии F и на нем стоит
# мост.
#
# Список значений короткий сознательно: имя ключа драйвера на пине 1.4.0 не
# проверено, проба на стенде обязательна ДО сетки, и значение мимо реестра
# означало бы, что раннер и сценарий разъехались. Нужна еще одна проба -
# значение дописывается сюда, а не подсовывается окружением.
# ВАЖНО про метку: уровень материализации у ЛЮБОГО исхода пробы -
# mat-untyped. Драйвер ключ не принял - numeric приезжает строкой; принял
# double - приезжает число с потерей знаков. Ни то, ни другое не Decimal.
ADBC_NUMERIC_AS_VALUES = ("", "double", "decimal", "string")
ADBC_NUMERIC_AS = _env_choice("BENCH_ADBC_NUMERIC_AS", "",
                              ADBC_NUMERIC_AS_VALUES)

# Ручки блока аналитика. Словарь имен обязан быть один на весь сценарий -
# иначе раннер и пути снова разъедутся на разных словарях.
VARIANT = _env_choice("BENCH_VARIANT", "", VARIANTS)
ENGINE = _env_choice("BENCH_ENGINE", "", ENGINES)
FILE = os.environ.get("BENCH_FILE", "").strip()

# Плотность NULL профиля g10null, в процентах строк на колонку. Значение
# участвует в ТЕКСТЕ запроса обеих баз одинаково (маска по модулю номера
# строки) - sha256-гейт остается применим на каждой плотности. Ноль - тоже
# зачетная точка: данные совпадают с g10, но типы на проводе уже Nullable,
# то есть клетка меряет цену самой обертки.
NULL_DENSITY = _env_int("BENCH_NULL_DENSITY", 0)
if not 0 <= NULL_DENSITY <= 99:
    raise SystemExit(
        f"BENCH_NULL_DENSITY={NULL_DENSITY}: ожидается 0..99 (маска по модулю "
        "100 номера строки; 100 дало бы колонку из одних NULL - такая клетка "
        "меряет не цену NULL, а пустой провод)")

# Длина строки профиля shape_len, в байтах. Постоянный объем держит СЕТКА
# (длина x строк = константа), сценарий знает только длину. Минимум 8:
# lpad/leftPad при длине короче самого числа усекали бы значение, и данные
# перестали бы быть уникальными по строкам.
STR_LEN = _env_int("BENCH_STR_LEN", 16)
if STR_LEN < 8:
    raise SystemExit(f"BENCH_STR_LEN={STR_LEN}: минимум 8 - короче lpad "
                     "усекает номер строки и значения теряют уникальность")

# Восстановление типов после mat-untyped (ось Д2, колонка extra2_s): текстовые
# форматы довозят строки, и клетка с этим флагом доплачивает pd.to_datetime /
# astype / Decimal до типизированного датафрейма. Флаг строгий - мусор в
# переменной означает раскоординацию раннера и сценария.
_RESTORE_RAW = os.environ.get("BENCH_RESTORE_TYPES", "").strip()
if _RESTORE_RAW not in ("", "0", "1"):
    raise SystemExit(
        f"BENCH_RESTORE_TYPES={_RESTORE_RAW!r}: ожидается пусто, 0 или 1")
RESTORE_TYPES = _RESTORE_RAW == "1"

# Объявленная схема DataFrame (E1, ревью 2026-09-13): имя схемы из
# _dfschema.SCHEMAS; пусто - датафрейм отдается как собрал путь. Список имен
# продублирован здесь сознательно: оси читаются без pandas.
DF_SCHEMA = os.environ.get("BENCH_DF_SCHEMA", "").strip()
if DF_SCHEMA:
    # Имена схем больше не перечисляются здесь списком: широкие схемы
    # (e1_w50, e1_w105) выводятся из объявленных логических схем ширин, и
    # второй список имен в этом файле разъехался бы с ними при первой правке
    # манифеста. Источник истины один - _dfschema.SCHEMAS.
    import _dfschema as _dfs
    if DF_SCHEMA not in _dfs.SCHEMAS:
        raise SystemExit(
            f"BENCH_DF_SCHEMA={DF_SCHEMA!r}: не объявлена; известны "
            f"{sorted(_dfs.SCHEMAS)}")

# Пересъемка retained-памяти (ось Д6): колонки retained_mb / rss_settled_mb
# по СОБСТВЕННОЙ версии структуры, при живой структуре. Замер идет внутри
# окна wall (в кортежном контракте другого места нет), поэтому wall клеток
# Д6 не цитируется - клетки существуют ради колонок памяти.
_RETAINED_RAW = os.environ.get("BENCH_RETAINED", "").strip()
if _RETAINED_RAW not in ("", "0", "1"):
    raise SystemExit(
        f"BENCH_RETAINED={_RETAINED_RAW!r}: ожидается пусто, 0 или 1")
RETAINED = _RETAINED_RAW == "1"

# Форма запроса (ось Д1): пусто - обычная выгрузка; point - серия точечных
# запросов по ключу (одно соединение, BENCH_EXECS исполнений); keyset -
# выгрузка чанками WHERE (ключ) > последнего ORDER BY ключ LIMIT N против
# сплошного SELECT. Обе формы живут только на холсте narrow - там есть
# таблица и ключ (валидация в bench_sql, рядом с построением текстов).
#
# ordered (F13, запрос исполнителя lib, п. 2) - не замерная форма, а форма
# ГЕЙТА: детерминированный ORDER BY ключа плюс LIMIT BENCH_ROWS, текст
# одинаков на обоих движках. Гейт 2 на реальной таблице (gate_2_hits_digest в
# lib.sh) сверяет sha256 среза hits_w10_1m между PostgreSQL и ClickHouse:
# без явного порядка он сверял бы раскладку частей, а не данные. Замерные
# клетки эту форму не берут - LIMIT без ORDER BY у них сознательный.
QUERY_FORM = _env_choice("BENCH_QUERY_FORM", "",
                         ("", "point", "keyset", "ordered"))
KEYSET_CHUNK = _env_int("BENCH_KEYSET_CHUNK", 1024)

# Усеченный digest для сверхбольших объемов: пусто или 0 - полный sha256 всей
# выдачи; N > 0 - в канон входят первые N строк, каждая N-я из остальных и
# итоговый счетчик строк (механика - digest_rows в bench_runtime). Полная
# канонизация стоит порядка микросекунды на ячейку: на сотнях миллионов строк
# это минуты CPU на путь, и гейт идентичности перестает помещаться в окно.
DIGEST_SAMPLE = _env_int("BENCH_DIGEST_SAMPLE", 0)
if DIGEST_SAMPLE < 0:
    raise SystemExit(
        f"BENCH_DIGEST_SAMPLE={DIGEST_SAMPLE}: ожидается 0 (полный digest) "
        "или положительное N - отрицательный шаг выборки не значит ничего")

# --- оси среза v1.5 -------------------------------------------------------
# Все они читаются здесь и только здесь: ось, размазанная по путям, рано или
# поздно получает второй дефолт и второе значение (так уже случилось с
# max_threads - см. F25/GRID-14).

# Как Arrow-таблица превращается в кортежи (F11 / PATH-02). pylist - штатный
# table.to_pylist(): он строит по СЛОВАРЮ на строку, и словарный слой уносил
# ~300 МиБ и около 16% времени, приписанных формату Arrow. colwise - сборка
# кортежей из колонок (zip по столбцам), без словарей. Дефолт остается
# pylist: на нем сняты клетки серии F, и мост сравнивает подобное с подобным;
# честная пара - отдельные клетки *-mat-colwise-*.
ARROW_TO_ROWS = _env_choice("BENCH_ARROW_TO_ROWS", "pylist",
                            ("pylist", "colwise"))

# Контракт сессии PostgreSQL - одинаковый у ВСЕХ клиентов (F8 / DATA-01).
# ЕДИНСТВЕННАЯ реализация живет в _srvcontrol (PG_SESSION_SETTINGS +
# pg_startup_options / pg_startup_options_uri): оттуда ее уже берут lib.sh,
# paths_pg (uri для ADBC и connectorx) и paths_jvm (строка для java), там же
# делается SET/SHOW в pg_prepare_session. До этой правки контракт сессии был
# реализован ДВАЖДЫ - здесь и там; два списка одних и тех же настроек - ровно
# тот дефект, из-за которого появилась находка F8, поэтому своя копия снята
# (запрос исполнителя runtime, п. 2).
#
# Имена осей остаются в реестре осей - но значениями, прочитанными ТАМ:
# jit (цена компиляции выражений зависит от плана, а не от протокола; 61
# клетка серии F ехала при jit=on у ADBC и connectorx против jit=off у
# psycopg и JDBC) и потолок параллелизма (без него число воркеров пляшет от
# ширины профиля, и серверная доля несопоставима между точками).
from _srvcontrol import PG_SESSION_SETTINGS as _PG_SESSION_SETTINGS

_PG_SESSION = dict(_PG_SESSION_SETTINGS)
PG_JIT = _PG_SESSION["jit"]
PG_PARALLEL = int(_PG_SESSION["max_parallel_workers_per_gather"])

# Усыпление клиента на каждый кусок в drain (гипотеза C8): моделирует
# медленного потребителя и показывает, кто платит за паузу - клиент,
# сервер или сеть. Микросекунды; 0 - штатная клетка без пауз.
CLIENT_DELAY_US = _env_int("BENCH_CLIENT_DELAY_US", 0)
if CLIENT_DELAY_US < 0:
    raise SystemExit(
        f"BENCH_CLIENT_DELAY_US={CLIENT_DELAY_US}: ожидается 0 или больше")

# Форма слива у psycopg (F4): cursor - серверный курсор с itersize (строки
# идут потоком), buffered - fetchmany по уже вычитанному ответу libpq. Это
# РАЗНЫЕ классы слива (rows против buffered), и раньше они стояли под одной
# меткой drain, из-за чего разность mat - drain меряла кадрирование, а не
# удержание.
DRAIN_FORM = _env_choice("BENCH_DRAIN_FORM", "cursor", ("cursor", "buffered"))

# Сетевой профиль полосы (F7): применяет РАННЕР на замерной VM через tc
# qdisc на egress к адресу сервера, сценарий только знает имя профиля и
# несет его в паспорт клетки. none - канал как есть.
NET_PROFILES = ("none", "rtt5", "rtt20", "bw1g", "bw100m", "far")
NET_PROFILE = _env_choice("BENCH_NET_PROFILE", "none", NET_PROFILES)

# Размер приемного буфера сокета клиента, байты (гипотеза C7, секция h-c7 и
# клетки k7-pgsock-*). 0 - буфер как есть, ядро решает само; ненулевое
# значение путь ставит SO_RCVBUF на своем соединении ДО первого чтения.
# Зачем ось: pg-wire в корпусе F шел кусками по 6.5 КиБ на пакет против
# 36 КиБ у ClickHouse по HTTP, и системный процессор на мегабайт отличался
# в 7.6 раза. Пара default против 1m разводит две причины - нарезку самого
# протокола (libpq отдает CopyData на строку) и мелкое чтение сокета: если
# разрыв уходит при крупном буфере, это практический совет, если остается -
# платит форма протокола, и это прямо в центральный вывод доклада.
# Ядро может выдать МЕНЬШЕ запрошенного (потолок net.core.rmem_max), поэтому
# путь обязан прочитать фактическое значение обратно и напечатать его в лог
# клетки - иначе метка k7-pgsock-1m соврет о том, что мерялось.
SOCK_RCVBUF = _env_int("BENCH_SOCK_RCVBUF", 0)
if SOCK_RCVBUF < 0:
    raise SystemExit(
        f"BENCH_SOCK_RCVBUF={SOCK_RCVBUF}: ожидается 0 (буфер как есть) "
        "или размер в байтах")

# Файловый маршрут блока аналитика (F6 / MEAS-11). Обе оси - флаги, и обе
# заведены отдельными КЛЕТКАМИ, а не молчаливой правкой старых: клетки серии
# F читали из страничного кеша и возвращали управление до сброса на диск, они
# остаются мостом под старыми метками.
#   FILE_COLD    - сбросить страничный кеш файла перед чтением (posix_fadvise
#                  DONTNEED), то есть честное холодное чтение;
#   FILE_DURABLE - выгрузка со сбросом на диск (fsync): клетка отвечает на
#                  вопрос "файл лежит", а не "команда вернула управление".
def _env_flag(name: str) -> bool:
    raw = os.environ.get(name, "").strip().lower()
    if raw in ("", "0", "off", "false"):
        return False
    if raw in ("1", "on", "true"):
        return True
    raise SystemExit(
        f"{name}={raw!r}: ожидается пусто, 0 или 1 - мусор в флаге означает "
        "раскоординацию раннера и сценария, а не выключенную ось")


FILE_COLD = _env_flag("BENCH_FILE_COLD")
FILE_DURABLE = _env_flag("BENCH_FILE_DURABLE")

# F04 (C3): digest ТЕМ ЖЕ текстом, что и замер. Обычный режим digest добавляет
# на реальных холстах детерминированный ORDER BY - он нужен гейту 2, который
# сверяет данные ДВУХ движков. Но у него есть цена: он проверяет не то, что
# меряется. Замерные клетки hits идут LIMIT без ORDER BY, и вопрос "приезжает
# ли из раунда в раунд один и тот же миллион строк" ORDER BY как раз и
# скрывает. С этим флагом текст запроса в режиме digest совпадает с замерным
# дословно (та же проекция, тот же LIMIT, без ORDER BY): раннер гоняет два
# таких прогона подряд одним путем и требует равенства sha256, а клетку с
# расхождением объявляет недействительной (hits_subset_unstable).
DIGEST_AS_MEASURED = _env_flag("BENCH_DIGEST_AS_MEASURED")

# Java-клиент (F25 / PATH-10): обе ручки читает BenchJdbc через окружение,
# но допустимые значения и дефолты живут здесь - словарь имен один на все
# языки. binaryTransfer=true - дефолт pgJDBC; warm connect - прогревочное
# соединение до окна замера (иначе connect_s первого раунда несет загрузку
# классов JVM).
_JDBC_BIN_RAW = os.environ.get("BENCH_JDBC_BINARY_TRANSFER",
                               "true").strip().lower()
if _JDBC_BIN_RAW not in ("true", "false"):
    raise SystemExit(
        f"BENCH_JDBC_BINARY_TRANSFER={_JDBC_BIN_RAW!r}: ожидается true|false")
JDBC_BINARY_TRANSFER = _JDBC_BIN_RAW
JDBC_WARM_CONNECT = _env_choice("BENCH_JDBC_WARM_CONNECT", "0", ("0", "1"))

# Порты: пустая строка = переменная не задана (раннер экспортирует имена разом,
# и VER-клетка с пустым CH_OLD_TCP_PORT дала бы слово CH_TCP_PORT=); мусор -
# внятный SystemExit, а не голый ValueError при импорте
PG = {
    "host": os.environ.get("PG_HOST", "localhost"),
    "port": _env_int("PG_PORT", 5432),
    "user": os.environ.get("PG_USER", "bench"),
    "db": os.environ.get("PG_DB", "bench"),
}
CH = {
    "host": os.environ.get("CH_HOST", "localhost"),
    "http_port": _env_int("CH_HTTP_PORT", 8123),
    "tcp_port": _env_int("CH_TCP_PORT", 9000),
    "user": os.environ.get("CH_USER", "bench"),
    "password": os.environ.get("CH_PASSWORD", ""),
    "db": os.environ.get("CH_DB", "bench"),
}


# ------------------------------------------------- класс слива и кадр
# F4: mat_level=drain склеивал ПЯТЬ разных механик - сырые байты, питоновские
# строки, Arrow-батчи, буферизованный ответ libpq и серверную пробу. Клетка
# a6-tcp не мерила предмет своей оси, а блоки V-30M и Д4 смешивали внутри
# одной кратности x20. Класс слива объявляется здесь и печатается в колонку
# drain_class сырья; ТОТ ЖЕ разбор случая повторяет lib.sh для --list-cases
# и для проверки однородности блока (девятого поля в спецификации клетки не
# заводим - формат восьми полей неизменен).
#
# Классы: raw (байты без разбора), rows (драйвер строит питоновские строки),
# arrow (готовые Arrow-батчи), buffered (ответ вычитан целиком, дальше
# fetchmany по буферу), srvcost (клетка серверной пробы, клиентского слива
# нет вовсе).
DRAIN_CLASSES = ("raw", "rows", "arrow", "buffered", "srvcost")

# Пути, у которых класс слива не зависит от осей.
_DRAIN_CLASS_BY_PATH = {
    "pg_copy": "raw",           # COPY TO STDOUT: поток байт, разбора нет
    "pg_server_cursor": "rows",
    "pg_execs": "buffered",     # fetchmany по уже вычитанному ответу
    "pg_adbc": "arrow",
    "pg_flightsql": "arrow",    # тот же клиент, что у ch_flightsql
    "cx_pg": "buffered",        # connectorx строит структуру целиком
    "pg_pandas": "buffered",    # read_sql собирает фрейм, потока нет
    "ch_native": "rows",
    "ch_http": "raw",
    "ch_pg_emu": "rows",
    "ch_mysql_emu": "rows",
    "ch_flightsql": "arrow",
    "ch_adbc_http": "arrow",
    "file_export": "raw",       # чтение файла байтовыми блоками
    "duckdb_file": "arrow",
}


def _jdbc_fetch_size_set() -> bool:
    """Задан ли fetchSize у Java-клиента (F4, запрос java п. 1).

    BENCH_BATCH=0 - это НЕ "кадр в ноль строк", а именно "fetchSize не
    задан" (так записана ступень a8-jdbc_pg-batch0 в сетке): пустая строка
    и ноль означают одно и то же.
    """
    return BATCH_SET and BATCH != 0


def drain_class(path: str, mode: str = None, drain_form: str = None) -> str:
    """Класс слива клетки. Пусто у режимов, где слива нет по построению.

    Функция чистая: все, от чего зависит ответ, приходит аргументами или
    берется из осей этого модуля. Второй экземпляр той же таблицы живет в
    lib.sh (bash не может импортировать python), и сверка двух экземпляров -
    задача гейта словаря, а не веры.
    """
    mode = mode or MODE
    if mode == "srvcost":
        return "srvcost"
    if mode != "drain":
        return ""
    if path == "pg_psycopg":
        # cursor - серверный курсор с itersize (строки идут потоком),
        # buffered - fetchmany по вычитанному ответу: разные механики,
        # и разность mat - drain у них означает разное (F4 / PATH-05)
        return "buffered" if (drain_form or DRAIN_FORM) == "buffered" \
            else "rows"
    if path == "jdbc_pg" and not _jdbc_fetch_size_set():
        # pgJDBC без fetchSize НЕ стримит: он выкачивает весь ResultSet в
        # heap - ровно тот буферный слив, что у psycopg при
        # BENCH_DRAIN_FORM=buffered. Клиент печатает факт (drain:buffered),
        # и объявлять эту ступень построчной значило бы склеить ось порции
        # a8-jdbc_pg-batch0 с построчными клетками - дефект F4 (запрос
        # исполнителя java, п. 1 и п. 14; та же ветка в _measure и в lib.sh)
        return "buffered"
    if path.startswith("jdbc_"):
        return "rows"
    klass = _DRAIN_CLASS_BY_PATH.get(path)
    if klass is None:
        raise SystemExit(
            f"класс слива пути {path!r} не объявлен: см. таблицу "
            "_DRAIN_CLASS_BY_PATH в bench_axes и ее двойник в lib.sh - "
            "клетка без класса ушла бы в корпус в общую кучу drain")
    return klass


# Размер кадра чтения (колонка frame). Одно имя оси на всех клиентов -
# BENCH_BATCH, но зовется кадр у каждого по-своему, и в корпусе F по строке
# нельзя было понять, каким кадром снята клетка. Путь вправе перекрыть
# значение своим (словарь extras сильнее умолчания).
# Кадр чтения HTTP-потока. Имя и значение те же, что у HTTP_BLOCK_BYTES в
# bench_runtime: там кусок читается, здесь называется в колонке frame.
# Держать в синхроне обязан тот, кто меняет размер куска (bench_runtime
# берет оси через getattr от этого модуля - значение можно свести в одно).
HTTP_BLOCK_BYTES = 1 << 20


def frame_col(path: str) -> str:
    """Кадр чтения строкой вида itersize=2000 / http=1048576 / пусто."""
    if path in ("pg_server_cursor",):
        return f"itersize={ITERSIZE}"
    if path == "pg_psycopg" and MODE == "drain" and DRAIN_FORM == "cursor":
        return f"itersize={ITERSIZE}"
    if path in ("pg_execs", "cx_pg", "pg_pandas"):
        return f"fetch={BATCH}" if BATCH_SET else ""
    if path == "ch_http":
        return f"http={HTTP_BLOCK_BYTES}"
    if path in ("ch_native", "ch_pg_emu", "ch_mysql_emu"):
        return f"max_block_size={BATCH}" if BATCH_SET else ""
    if path.startswith("jdbc_"):
        # ноль - это "fetchSize не задан", а не кадр в ноль строк (см.
        # _jdbc_fetch_size_set): печатать fetchSize=0 значило бы объявить
        # кадром отсутствие кадра
        return f"fetchSize={BATCH}" if _jdbc_fetch_size_set() else ""
    return ""


def axis_extras(path: str) -> dict:
    """Осевые факты клетки для строки сырья (словарь extras обвязки).

    Единственный источник умолчаний трех колонок среза (CONTRACT, п. 7.1):
    зовет функцию точка входа 09_gen_paths ДО окна замера и дописывает
    значения в extras пути, где НЕпустое значение пути сильнее (T12).

    Четвертый ключ - max_threads - колонкой не становится: в HEADER его нет,
    и обвязка выбрасывает его из extras по имени (AXIS_ONLY_KEYS), а сам факт
    точка входа печатает в журнал полосы строкой stderr "# axis max_threads=".
    """
    return {
        "codec_level": CODEC_LEVEL_COL,
        "drain_class": drain_class(path),
        "frame": frame_col(path),
        "max_threads": MAX_THREADS_FACT,
    }


# ------------------------------------------------- гейт 2: пути и пины
# F13 / DATA-03: гейт идентичности сверял ДВА пути из пятнадцати, и шесть
# путей (46 клеток) уходили в корпус без единой сверки выдачи. Расхождение
# этого класса в серии F поймали глазами по колонке client_dtype - повторить
# удачу нельзя.
#
# Реестр читает предполет (lib.sh) через 09_gen_paths.py --list-gate2-paths.
# Поля: тег (уникальное имя точки гейта), путь, движок, ожидание, окружение
# клетки, причина.
#
# Ожидания:
#   cross      - digest обязан совпасть с ДРУГИМ движком (родная пара);
#   pin        - digest сверяется с пином серии F (gate2-pins-F.json):
#                расхождение с родным путем законно и объявлено ниже,
#                расхождение с пином - остановка;
#   rows_bytes - digest недоступен по построению (pg_copy отвергает режим),
#                сверяются число строк и объем потока против пина;
#   file_sha   - клиент не умеет digest (Java), сверка идет вне окна замера:
#                выгрузка в файл и sha256 против file_export.
GATE2_EXPECTS = ("cross", "pin", "rows_bytes", "file_sha")

GATE2_PATHS = (
    ("pg_native", "pg_psycopg", "pg", "cross", "BENCH_FMT=simple_text",
     "родная пара PostgreSQL - эталон кросс-движковой сверки"),
    ("ch_native", "ch_native", "ch", "cross", "",
     "родная пара ClickHouse - эталон кросс-движковой сверки"),
    ("pg_cursor", "pg_server_cursor", "pg", "cross", "",
     "серверный курсор обязан отдать те же объекты, что клиентский"),
    ("pg_execs", "pg_execs", "pg", "cross", "BENCH_FMT=simple_text",
     "буферный fetchmany обязан отдать те же объекты, что курсор"),
    ("pg_binary", "pg_psycopg", "pg", "cross", "BENCH_FMT=ext_binary",
     "бинарная кодировка значений против текстовой на том же клиенте"),
    ("cx_csv", "cx_pg", "pg", "pin", "BENCH_CX_PROTOCOL=csv",
     "connectorx довозит numeric во float64 - расхождение объявлено"),
    ("cx_cursor", "cx_pg", "pg", "pin", "BENCH_CX_PROTOCOL=cursor",
     "тот же float64 на numeric, другой протокол вычитки"),
    ("emu_pg", "ch_pg_emu", "ch", "pin", "BENCH_FMT=simple_text",
     "эмуляция pg-wire отдает DateTime строкой - на этом стоит слайд"),
    ("emu_mysql", "ch_mysql_emu", "ch", "pin", "",
     "эмуляция mysql-wire: своя деградация типов"),
    ("http_json", "ch_http", "ch", "pin", "BENCH_FMT=JSONEachRow",
     "текстовый формат: числа и даты приезжают строками"),
    ("http_csv", "ch_http", "ch", "pin", "BENCH_FMT=CSV",
     "текстовый формат без типов"),
    ("http_tsv", "ch_http", "ch", "pin", "BENCH_FMT=TabSeparated",
     "текстовый формат без типов"),
    ("pg_copy", "pg_copy", "pg", "rows_bytes", "BENCH_FMT=copy_binary",
     "COPY отвергает digest по построению - сверка строк и объема"),
    ("jdbc_pg", "jdbc_pg", "pg", "file_sha", "",
     "Java-клиент digest не считает - sha256 файла против file_export"),
)

# Расхождения, которые ОЖИДАЮТСЯ и объявлены заранее: без реестра предполет
# стал бы вечно красным, а с молчанием - слепым. Ключ - тег точки гейта.
EXPECTED_DIGEST_DIVERGENCE = {
    tag: why for tag, _path, _engine, expect, _env, why in GATE2_PATHS
    if expect != "cross"
}
