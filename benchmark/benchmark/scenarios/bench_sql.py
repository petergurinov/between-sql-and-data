"""Построение SQL: холсты, профили генерации, тексты запросов обоих движков.

Главный ход серии: на холсте gen данных на диске нет вообще. Обе базы собирают
результат по формуле - PostgreSQL через generate_series, ClickHouse через
numbers - значит из уравнения физически выведены хранение, план чтения, кеш
страниц и LIMIT без ORDER BY. Меняются только параметры, влияющие на протокол
взаимодействия.

Значения подобраны так, чтобы совпадать ПОБАЙТОВО между движками: никаких
float (представление движко-зависимо), никаких timestamp с микросекундами,
строки только фиксированной длины (lpad / leftPad), время - с явной зоной UTC
с ОБЕИХ сторон. Проверяется предполетным гейтом BENCH_MODE=digest.

Исключения из правила побайтовой одинаковости объявлены ЯВНО и списком:
профили g_map (Map против jsonb) и g1f64 (float) - внутридвижковые, их
клетки сравниваются только внутри одного движка, в гейт 2 они не идут.
Список ведет bench_axes.PROFILES_INTRA_ENGINE, печатает его флаг
--list-profiles точки входа - гейт не должен выводить это правило из
комментариев.
"""

import os
import sys

from bench_axes import (CANVAS, DIGEST_AS_MEASURED, KEYSET_CHUNK, MAX_THREADS,
                        MODE, NULL_DENSITY, PARALLEL_FMT, PROFILE, PROFILES,
                        QUERY_FORM, ROWS, SHAPE, STR_LEN, TABLE, WIDTH, CH)
# F15 / SRVEXEC-01: маркер служебных запросов - один на весь стенд, сборщик
# srvstats выкидывает по нему строки, а не гадает по тексту
from _srvcontrol import SETUP_MARKER

# Пары выражений PG / CH одной строкой: побайтовая одинаковость значений
# должна быть видна глазом, а не выводиться из двух далеких кусков файла.
# В PG генератор дает g, в CH - n (см. sql_pg / sql_ch).
_S16_PG = "lpad((g % 100000000)::text, 16, '0')"
_S16_CH = "leftPad(toString(n % 100000000), 16, '0')"

# Время: зона проставлена ЯВНО с обеих сторон (контракт). Timestamp без зоны
# у PG совпадал бы с DateTime ClickHouse только пока сессия сервера случайно
# стоит в UTC; AT TIME ZONE 'UTC' не зависит от настройки сессии вообще.
_TS_PG = ("((TIMESTAMP '2026-01-01 00:00:00' "
          "+ ((g % 86400) * INTERVAL '1 second')) AT TIME ZONE 'UTC')"
          "::timestamptz(0)")
_TS_CH = "toDateTime('2026-01-01 00:00:00', 'UTC') + toInt32(n % 86400)"

G10 = (
    ("id", "g", "toInt64(n)"),
    ("i2", "g * 3", "toInt64(n * 3)"),
    ("i16", "(g % 30000)::int2", "toInt16(n % 30000)"),
    ("i7", "(g % 7)::int2", "toInt16(n % 7)"),
    ("s8", "lpad((g % 10000)::text, 8, '0')",
     "leftPad(toString(n % 10000), 8, '0')"),
    ("s16", _S16_PG, _S16_CH),
    ("d", "DATE '2026-01-01' + (g % 365)::int",
     "toDate('2026-01-01') + toInt32(n % 365)"),
    ("ts", _TS_PG, _TS_CH),
    # знаменатель 10000 при целом числителе - значение точно представимо
    # четырьмя знаками, значит округление и усечение дают одно и то же
    ("dec", "(((g % 1000000)::numeric) / 10000)::numeric(18,4)",
     "toDecimal64(n % 1000000, 4) / 10000"),
    ("i32", "(g % 100000)::int4", "toInt32(n % 100000)"),
)

COLS10 = ("WatchID, JavaEnable, Title, EventTime, CounterID, "
          "ClientIP, RegionID, UserID, URL, Referer")
# Проекция w10tz (F14/HC5): те же десять колонок, что у w10, но EventTime
# отдается в UTC ЯВНО с обеих сторон. Зачем: у remote-контура зона сервера
# Europe/Moscow, и клиент разбирает каждую дату другим кодом - аномалия +31%
# на ch_native приписывалась контуру, а не зоне. Квадрат {контур} x
# {w10, w10tz} называет цену зоны числом.
#   CH: toTimeZone к DateTime - смена зоны РЕНДЕРА, момент времени тот же.
#   PG: EventTime в hits имеет тип timestamp БЕЗ зоны (bench/pg_create.sql:7),
#       поэтому пара AT TIME ZONE 'UTC' (истолковать как UTC -> показать в
#       UTC) дает тот же timestamp без зоны при ЛЮБОЙ настройке сессии.
#       Одиночный AT TIME ZONE брать нельзя: он вернул бы timestamptz, и
#       клиент отрендерил бы его в зоне сессии - клетка мерила бы не зону
#       сервера, а наш собственный сдвиг типа.
_COLS10_TZ_PG = COLS10.replace(
    "EventTime",
    "(EventTime AT TIME ZONE 'UTC' AT TIME ZONE 'UTC') AS EventTime")
_COLS10_TZ_CH = COLS10.replace(
    "EventTime", "toTimeZone(EventTime, 'UTC') AS EventTime")
# PROJ - словарь допустимых значений BENCH_WIDTH (валидация ниже). Тексты
# проекций разные у движков только на w10tz, поэтому у каждого движка свой
# словарь, а общий остается законом имен.
PROJ = {"w10": COLS10, "w105": "*", "url1": "URL", "w10tz": COLS10}
PROJ_PG = dict(PROJ, w10tz=_COLS10_TZ_PG)
PROJ_CH = dict(PROJ, w10tz=_COLS10_TZ_CH)
# G3: у физических таблиц cb_<ширина>_<строки> есть и 1, и 10k - без них
# _expected_rows отдавал 0, и COPY писал rows=0 при status=ok (12 клеток G3)
NARROW_ROWS = {"1": 1, "10k": 10_000, "100k": 100_000, "1m": 1_000_000, "10m": 10_000_000}

if CANVAS in ("hits", "hits_legacy") and WIDTH not in PROJ:
    raise SystemExit(f"BENCH_WIDTH={WIDTH!r}, ожидается одно из {tuple(PROJ)}")

# Имя таблицы холстов hits/hits_legacy: у legacy другая таблица (побайтовая
# копия старой hits с битой датой, живет только в PG), проекция и LIMIT - те
# же, что у hits: клетки моста сравнивают байты при прочих равных.
HITS_TABLE = {"hits": "hits", "hits_legacy": "hits_legacy"}

# Ключ детерминированного порядка на реальных холстах - тот самый набор
# числовых колонок, которым нарезались таблицы narrow (29-narrow-tables.sh,
# переменная ORD). Он же ключ keyset-выгрузки ниже: два разных набора здесь
# означали бы, что гейт сверяет один порядок, а замер идет по другому.
_ORDER_COLS = ("WatchID", "EventTime", "CounterID", "ClientIP",
               "RegionID", "UserID", "JavaEnable")


def _digest_order() -> str:
    """Хвост ORDER BY для гейта идентичности на холстах hits и narrow (F13).

    Замерные клетки порядок НЕ навязывают: LIMIT без ORDER BY - сознательный
    ход серии, он выводит из уравнения план чтения. Но гейт 2 сверяет sha256
    ДВУХ движков, и без явного порядка он сверял бы не данные, а раскладку
    частей - на CH голова миллиона при LIMIT без ORDER BY не воспроизводится
    даже между стендами. Поэтому порядок появляется ровно в режиме digest и
    ровно тем ключом, которым таблицы отбирались при создании.

    Текст одинаков для обоих движков - иначе гейт сравнивал бы два разных
    запроса.
    """
    # форма ordered (F13) - тот же детерминированный порядок вне режима
    # digest: гейт 2 на реальной таблице снимает срез ею, а не режимом
    if (MODE != "digest" and QUERY_FORM != "ordered") or CANVAS == "gen":
        return ""
    # C3 (F04): режим "digest как в замере" - текст обязан совпадать с
    # замерным дословно, значит никакого ORDER BY. Гейт стабильности клеток
    # hits проверяет ровно то, что меряется: два прогона подряд одним путем
    # и одним текстом обязаны дать один sha256.
    # Форма ordered - исключение: там порядок задает ОСЬ клетки, он есть и в
    # замерном тексте, и снять его значило бы разойтись с замером, а заодно
    # тихо превратить детерминированный срез гейта 2 в недетерминированный
    if MODE == "digest" and DIGEST_AS_MEASURED and QUERY_FORM != "ordered":
        return ""
    if CANVAS in HITS_TABLE and WIDTH == "w105":
        raise SystemExit(
            "BENCH_MODE=digest на проекции w105: 105 колонок сортировать "
            "незачем и дорого - гейт идентичности реальных холстов "
            "снимается на w10 (или w10tz)")
    if (CANVAS in HITS_TABLE and WIDTH == "url1") or \
            (CANVAS == "narrow" and "url1" in TABLE):
        # у url1-таблиц ключевых колонок нет вовсе, единственная колонка и
        # есть ключ: одинаковые значения дают одинаковые строки, и порядок
        # внутри группы равных на хеш не влияет
        return "\nORDER BY URL"
    return "\nORDER BY " + ", ".join(_ORDER_COLS)


def _digest_limit() -> str:
    """LIMIT для гейта на холсте narrow: у замерных клеток его нет (таблица
    читается целиком), а гейт снимается на срезе BENCH_ROWS. Форма ordered
    берет тот же срез вне режима digest (F13)."""
    if CANVAS != "narrow":
        return ""
    if MODE == "digest" and DIGEST_AS_MEASURED and QUERY_FORM != "ordered":
        # C3: у замерной клетки narrow LIMIT нет вовсе - таблица читается
        # целиком, и добавленный срез сверял бы не тот запрос. У формы
        # ordered срез задан осью и есть в замерном тексте - его не трогаем
        return ""
    if MODE == "digest" or QUERY_FORM == "ordered":
        return f"\nLIMIT {ROWS}"
    return ""


# ------------------------------------------------- профили холста gen
# Реестр расширяем словарем: новый профиль = построитель здесь + имя в
# PROFILES (bench_axes). Построитель возвращает кортеж троек
# (имя колонки, выражение PG, выражение CH).

def _cols_g1():
    return (("s1", _S16_PG, _S16_CH),)


def _cols_g10():
    return G10


def _cols_g10f():
    """g10 в типах, которые умеет отдать Arrow Flight SQL adapter PostgreSQL.

    Зачем отдельный профиль, а не ветка внутри g10. Адаптер
    (arrow_flight_sql 0.2.0-dev) знает ровно девять типов результата:
    int2/int4/int8, float4/float8, text/varchar, bytea и timestamp БЕЗ зоны.
    В g10 три колонки из десяти в этот набор не попадают - d (date), ts
    (timestamptz) и dec (numeric), - а неподдержанный тип у адаптера не
    просто отказ: клиент получает NotImplemented, и ТУТ ЖЕ падает фоновый
    воркер "arrow-flight-sql: server", после чего Flight-порт мертв до
    перезапуска PostgreSQL (проверено локально 2026-09-08 на bool, date,
    numeric, bpchar, timestamptz, uuid и int8[]). То есть клетка g10 на
    Flight - не пустая клетка, а выстрел в стенд, и профиль обязан быть
    другим.

    Что меняется ровно в трех колонках, остальные семь - буква в букву g10:
      d   date        -> timestamp без зоны (полночь того же дня);
      ts  timestamptz -> тот же момент, timestamp без зоны в UTC;
      dec numeric     -> int8 (значение другое: цену Decimal этот профиль не
                        меряет по построению - на Flight ее замерить нечем).

    Профиль СКВОЗНОЙ (в PROFILES_INTRA_ENGINE его нет), и вот почему.
    Канонизация digest (_srvcontrol._canon) читает наивный datetime как UTC,
    а осведомленный приводит к UTC - значит timestamp без зоны у PostgreSQL и
    DateTime('UTC') у ClickHouse дают ОДИН И ТОТ ЖЕ текст. Обе новые
    временные колонки построены на одних и тех же числах, что в g10, третья -
    обычное целое. Кросс-движковый sha256 обязан сойтись, и гейт 2 это
    проверяет (регистрация - в grid-extra под STANDF_PGFLIGHT=1).
    """
    out = []
    for name, pg, ch in G10:
        if name == "d":
            # у CH toDateTime(Date, 'UTC') - полночь UTC того же дня; у PG
            # приведение date -> timestamp дает ту же полночь без зоны
            out.append((name, "(DATE '2026-01-01' + (g % 365)::int)::timestamp",
                        "toDateTime(toDate('2026-01-01') + toInt32(n % 365), "
                        "'UTC')"))
        elif name == "ts":
            # AT TIME ZONE над timestamptz снимает зону, оставляя момент в
            # UTC: значение то же, что у g10, тип - тот, который умеет адаптер
            out.append((name, f"({_TS_PG} AT TIME ZONE 'UTC')", _TS_CH))
        elif name == "dec":
            out.append((name, "(g % 1000000)::int8", "toInt64(n % 1000000)"))
        else:
            out.append((name, pg, ch))
    return tuple(out)


def _cheap_ints(upto: int):
    # дешевые целые лестницы ширины: одна схема на g50/g75/g105, чтобы
    # ступени лестницы отличались ТОЛЬКО числом колонок
    return tuple(
        (f"c{k}", f"((g + {k}) % 1000)::int4", f"toInt32((n + {k}) % 1000)")
        for k in range(11, upto + 1))


def _cols_g20():
    # ступень лестницы ширины ниже g50 (F25/A13): та же схема дешевых целых,
    # чтобы соседние ступени отличались ТОЛЬКО числом колонок
    return G10 + _cheap_ints(20)


def _cols_g30():
    # вторая нижняя ступень лестницы ширины (F25/A13)
    return G10 + _cheap_ints(30)


def _cols_g50():
    # десять смешанных плюс сорок дешевых целых: ширина растет, а цена
    # значения на сервере почти нулевая
    return G10 + _cheap_ints(50)


def _cols_g75():
    # ступень лестницы ширины между g50 и g105 (ось A13)
    return G10 + _cheap_ints(75)


def _cols_g105():
    # ширина реальной аналитической таблицы - изоляция падения Flight (A13)
    return G10 + _cheap_ints(105)


def _cols_entropy():
    # Ширина и типы как у g50, но строковые значения - ПРЕФИКС md5 от номера
    # строки: детерминированно, побайтово одинаково между движками и почти
    # несжимаемо. Высокоэнтропийная пара к g50 для оси сжатия. Вторая
    # строковая колонка солится префиксом - иначе обе несли бы ОДИН И ТОТ ЖЕ
    # хеш, и межколоночная дедупликация кодека прожала бы "несжимаемые"
    # данные вдвое.
    # F12 (GRID-11): берется именно ПРЕФИКС длины 8 и 16, а не весь md5 из
    # 32 символов. Полный хеш добавлял 24 + 16 = 40 байт на строку (38.15 МиБ
    # на 1М), и контрольная пара g50/entropy расходилась ОДНОВРЕМЕННО по
    # содержимому и по объему - разницу времен нельзя было приписать
    # сжимаемости. С префиксом совпадают число строк, число колонок, типы и
    # байты, а отличие остается ровно одно - сжимаемость.
    # Оговорка для сетки: hex-алфавит из 16 символов несжимаем для lz4, но
    # zstd отыгрывает на нем около двух раз; zstd-пару на entropy ставить
    # только со случайной строкой полного алфавита той же ширины.
    cols = []
    for name, pg, ch in G10:
        if name == "s8":
            cols.append((name, "substr(md5(g::text), 1, 8)",
                         "substring(lower(hex(MD5(toString(n)))), 1, 8)"))
        elif name == "s16":
            cols.append((name, "substr(md5('s16:' || g::text), 1, 16)",
                         "substring(lower(hex(MD5(concat('s16:', "
                         "toString(n))))), 1, 16)"))
        else:
            cols.append((name, pg, ch))
    return tuple(cols) + _cheap_ints(50)


def _cols_g10null():
    # g10 с детерминированной NULL-маской: колонка k пуста там, где
    # (номер строки + k) % 100 < плотность. Маска считается в ТЕКСТЕ запроса
    # ОДИНАКОВО на обеих базах - sha256-гейт применим на каждой плотности.
    # Сдвиг на индекс колонки рассыпает NULL по строкам: без него маска
    # выбивала бы строки целиком и мерила бы не цену NULL в значениях,
    # а укороченный набор строк.
    # Плотность 0 - тоже зачетная точка: данные совпадают с g10, но тип на
    # проводе уже Nullable (у CH это отдельный байт-маска на значение) -
    # клетка меряет цену самой обертки.
    cols = []
    for k, (name, pg, ch) in enumerate(G10):
        cols.append((
            name,
            f"CASE WHEN (g + {k}) % 100 < {NULL_DENSITY} "
            f"THEN NULL ELSE {pg} END",
            f"if((n + {k}) % 100 < {NULL_DENSITY}, NULL, {ch})",
        ))
    return tuple(cols)


def _cols_g_arr():
    # Вложенный тип: массив Int64 длины 10 плюс скаляры по бокам - видно,
    # что провод делает с массивом, на фоне обычных колонок того же запроса.
    # PG отдает int8[], CH - Array(Int64); питоновские клиенты обоих движков
    # строят списки, так что кросс-движковый sha256 имеет шанс сойтись -
    # решает предполетный гейт, а не вера.
    arr_pg = "ARRAY[" + ", ".join(f"g + {i}" for i in range(10)) + "]::int8[]"
    arr_ch = "arrayMap(i -> toInt64(n) + toInt64(i), range(10))"
    return (
        ("id", "g", "toInt64(n)"),
        ("arr", arr_pg, arr_ch),
        ("s16", _S16_PG, _S16_CH),
    )


def _cols_g_map():
    # Map(String, Int64) у ClickHouse против jsonb у PostgreSQL: одинаковой
    # типовой пары здесь НЕТ по построению, и кросс-движковый sha256 на этом
    # профиле НЕ гарантируется - клетки g_map сравниваются только
    # ВНУТРИ движка (пометка в сетке), гейт идентичности на них не вешать.
    # F12 (DATA-04): нагрузка выровнена с g_arr и g10flat - те же десять
    # значений g..g+9 и та же колонка s16. До правки словарь нес ТРИ числа
    # против одиннадцати у массива, и "вдвое меньше байт у jsonb" было
    # разницей полезной нагрузки, а не контейнера; при равных значениях знак
    # утверждения переворачивается - словарь докупает провод за имена ключей.
    # Три профиля (g10flat / g_arr / g_map) на ОДНИХ И ТЕХ ЖЕ числах и дают
    # честный ответ "сколько стоит контейнер": колонки, массив, словарь.
    map_pg = "jsonb_build_object(" + ", ".join(
        f"'a{i}', g + {i}" for i in range(10)) + ")"
    map_ch = "map(" + ", ".join(
        f"'a{i}', toInt64(n) + {i}" for i in range(10)) + ")"
    return (
        ("id", "g", "toInt64(n)"),
        ("m", map_pg, map_ch),
        ("s16", _S16_PG, _S16_CH),
    )


def _cols_g10flat():
    # Третья нога тройки контейнеров (F12/DATA-04): те же десять значений
    # g..g+9 и та же s16, но ПЛОСКИМИ колонками. Каркас строки против
    # каркаса массива против каркаса словаря с именами ключей - на одних
    # числах, поэтому разница байт и есть цена упаковки.
    return (
        (("id", "g", "toInt64(n)"),)
        + tuple((f"a{i}", f"g + {i}", f"toInt64(n) + {i}") for i in range(10))
        + (("s16", _S16_PG, _S16_CH),)
    )


def _cols_shape_len():
    # Ось длины строки (Д3): одна строковая колонка ровно BENCH_STR_LEN байт,
    # постоянный объем держит сетка (длина x строк = константа). Механика
    # значения та же, что у shape: номер строки, добитый нулями до длины, -
    # значения детерминированы, повторяемы и побайтово одинаковы у обеих баз.
    return ((
        "s",
        f"lpad((g % 100000000)::text, {STR_LEN}, '0')",
        f"leftPad(toString(n % 100000000), {STR_LEN}, '0')",
    ),)


# Одноколоночные профили прайса типов (Д10 / d10b).
# ЦЕНА ТИПА СЧИТАЕТСЯ РАЗНОСТЬЮ ОДНОРОДНЫХ КЛЕТОК: (wall(тип) - wall(g1int))
# на ТОМ ЖЕ пути, том же кадрировании и том же режиме, деленная на число
# строк. Прежняя формула (wall_mat - wall_drain)/строк отменена (F24 /
# PATH-05): у psycopg drain идет серверным курсором, а materialize - одним
# execute, то есть вычитались два РАЗНЫХ кадрирования и цена выходила
# отрицательной (-121 нс на int64); у ch_native и ADBC drain уже строит
# объекты и Arrow-массивы, за тип платят обе клетки, и разность около нуля
# означает не бесплатный тип. Долю общего пола дает отдельная клетка
# d10b-<тип>-floor-{pg,ch} (drain по сырым байтам, без питоновского цикла).
# Та же формулировка обязана стоять в spec-stand-f.md, grid-extra.sh и
# standf-harness-map - формула жила в четырех местах сразу.
def _cols_g1int():
    return (("v", "g", "toInt64(n)"),)


def _cols_g1i16():
    # int16: половина ширины int32 и четверть int64 - нижняя точка прайса
    # целых (T1). Значения те же, что у колонки i16 профиля g10.
    return (("v", "(g % 30000)::int2", "toInt16(n % 30000)"),)


def _cols_g1f64():
    # ВНУТРИДВИЖКОВЫЙ профиль (кросс-движковый sha256 не требуется, как у
    # g_map): текстовое представление float у движков и клиентов свое, и
    # запрет спеки на float в общем холсте остается в силе. Знаменатель -
    # степень двойки, поэтому значение точно представимо в double и в
    # пределах одного движка воспроизводимо байт в байт. Нужен ради строки
    # "float64" в прайсе типов: в корпусе F такой клетки нет вовсе.
    return (("v", "((g % 1000000)::float8 / 8)",
             "toFloat64(n % 1000000) / 8"),)


def _cols_g1date():
    # date без времени: у PG 4 байта на проводе, у CH 2 - строка прайса,
    # которой в корпусе F не было. Значения те же, что у колонки d в g10.
    return (("v", "DATE '2026-01-01' + (g % 365)::int",
             "toDate('2026-01-01') + toInt32(n % 365)"),)


def _cols_g1uuid():
    # UUID из md5 номера строки: значение детерминировано и ОДИНАКОВО в
    # обеих базах. PG: md5() дает 32 шестнадцатеричных символа, приведение
    # к uuid раскладывает их канонически. CH: UUIDNumToString читает те же
    # 16 байт MD5 в порядке big-endian (вариант по умолчанию) и дает ту же
    # каноническую строку - один вызов MD5 на строку, а не пять, как дала бы
    # сборка через substring.
    return (("v", "md5(g::text)::uuid",
             "toUUID(UUIDNumToString(MD5(toString(n))))"),)


def _cols_g1bool():
    # bool: самый дешевый тип провода (1 байт у обоих движков) - нижняя
    # отсечка прайса. У CH тип Bool, а не UInt8: иначе клиент вернул бы
    # целое, и клетка мерила бы цену числа под именем булева.
    return (("v", "((g % 2) = 0)", "toBool(n % 2 = 0)"),)


def _cols_g1str1000():
    # Строка ровно 1000 символов - верхняя точка прайса по длине значения.
    # Отдельный профиль, а не shape_len с BENCH_STR_LEN=1000: клетка прайса
    # типов не должна зависеть от оси, которую держит сетка.
    return (("v", "lpad((g % 100000000)::text, 1000, '0')",
             "leftPad(toString(n % 100000000), 1000, '0')"),)


def _cols_g1dt():
    return (("v", _TS_PG, _TS_CH),)


def _cols_g1decimal():
    return (("v", "(((g % 1000000)::numeric) / 10000)::numeric(18,4)",
             "toDecimal64(n % 1000000, 4) / 10000"),)


def _cols_g1decbound():
    # Граничный Decimal (контрольная пара): значения на 17 значащих цифрах
    # НЕ представимы во float64 - путь, довозящий Decimal float-ом, теряет
    # ЗНАЧЕНИЕ при сохранном типе колонки. Сумма всегда в пределах
    # numeric(18,4)/Decimal64: 9e12 + до 100 при масштабе 4.
    return (("v",
             "(9000000000000::numeric"
             " + ((g % 1000000)::numeric / 10000))::numeric(18,4)",
             "toDecimal64(9000000000000, 4)"
             " + toDecimal64(n % 1000000, 4) / 10000"),)


def _cols_shape():
    # shape - ось формы нарезки: значение всегда строка ровно из 16 символов,
    # и в pg-wire текстовая и бинарная кодировка строки совпадают побайтово -
    # значит кодировка выведена из уравнения, вся разница в байтах каркас
    return tuple(
        (f"c{k}",
         f"lpad(((g + {k}) % 10000000)::text, 16, '0')",
         f"leftPad(toString((n + {k}) % 10000000), 16, '0')")
        for k in range(1, SHAPE + 1))


PROFILE_COLS = {
    "g1": _cols_g1,
    "g10": _cols_g10,
    "g10f": _cols_g10f,
    "g20": _cols_g20,
    "g30": _cols_g30,
    "g50": _cols_g50,
    "g75": _cols_g75,
    "g105": _cols_g105,
    "entropy": _cols_entropy,
    "g10null": _cols_g10null,
    "g_arr": _cols_g_arr,
    "g_map": _cols_g_map,
    "g10flat": _cols_g10flat,
    "shape": _cols_shape,
    "shape_len": _cols_shape_len,
    "g1int": _cols_g1int,
    "g1i16": _cols_g1i16,
    "g1f64": _cols_g1f64,
    "g1decimal": _cols_g1decimal,
    "g1dt": _cols_g1dt,
    "g1date": _cols_g1date,
    "g1uuid": _cols_g1uuid,
    "g1bool": _cols_g1bool,
    "g1str1000": _cols_g1str1000,
    "g1decbound": _cols_g1decbound,
}

# Сверка двух реестров: имя без построителя (или наоборот) должно падать при
# импорте, а не молча давать пустой SQL на середине серии
if set(PROFILE_COLS) != set(PROFILES):
    raise SystemExit(
        f"реестры профилей разошлись: PROFILES={sorted(PROFILES)}, "
        f"PROFILE_COLS={sorted(PROFILE_COLS)} - новый профиль добавляется "
        "в оба места")


def _profile_cols():
    """Колонки холста gen: (имя, выражение PG, выражение CH)."""
    return PROFILE_COLS[PROFILE]()


def sql_pg() -> str:
    if CANVAS == "narrow":
        return f"SELECT * FROM {TABLE}{_digest_order()}{_digest_limit()}"
    if CANVAS in HITS_TABLE:
        return (f"SELECT {PROJ_PG[WIDTH]} FROM {HITS_TABLE[CANVAS]}"
                f"{_digest_order()} LIMIT {ROWS}")
    cols = ",\n       ".join(f"{e} AS {name}" for name, e, _ in _profile_cols())
    # Ловушка: generate_series прямо во FROM - это FunctionScan, а он читает
    # функцию в tuplestore ЦЕЛИКОМ до первой строки. Обертка в подзапрос
    # ставит ProjectSet, и строки идут потоком (проверяется гейтом плана).
    return (f"SELECT {cols}\n"
            f"FROM (SELECT generate_series(1::bigint, {ROWS}::bigint) AS g) s")


def sql_ch() -> str:
    if CANVAS == "narrow":
        return f"SELECT * FROM {TABLE}{_digest_order()}{_digest_limit()}"
    # hits_legacy живет только в PG (перенос дампом) - CH-клеток этого холста
    # в сетке нет; текст строится симметрично, чтобы импорт не падал на
    # законных PG-клетках
    if CANVAS in HITS_TABLE:
        return (f"SELECT {PROJ_CH[WIDTH]} FROM {HITS_TABLE[CANVAS]}"
                f"{_digest_order()} LIMIT {ROWS}")
    cols = ",\n       ".join(f"{e} AS {name}" for name, _, e in _profile_cols())
    return (f"SELECT {cols}\n"
            f"FROM (SELECT toUInt64(number) + 1 AS n FROM numbers({ROWS}))")


SQL_PG = sql_pg()
SQL_CH = sql_ch()

# Эмуляции не получают настроек сессии: постгресовый SET по pg-wire эмуляция
# не понимает, у mysql-wire его тоже нет, - и без этого их клетки ехали бы на
# серверном дефолте max_threads рядом с родными путями, прибитыми в 1. Поэтому
# обязательную настройку вшиваем в САМ текст запроса: SELECT ... SETTINGS
# ClickHouse понимает на любом проводе, на набор строк это не влияет. Зона
# времени сюда не входит - она прибита в выражениях колонок
# (toDateTime(..., 'UTC')).
SQL_CH_EMU = SQL_CH + (
    f"\nSETTINGS max_threads={int(MAX_THREADS)}" if MAX_THREADS else "")


def _sql_ch_jdbc() -> str:
    """Текст запроса ClickHouse для Java-клиента (PATH-10).

    У clickhouse-jdbc серверные настройки в URL зависят от ветки драйвера, и
    молча снятая на неизвестной ручке цифра хуже пустой клетки. Способ,
    одинаковый на всех ветках, один - хвост SETTINGS в САМОМ тексте запроса,
    тот же прием, что у эмуляций. Расширять SQL_CH_EMU нельзя: на нем сняты
    клетки эмуляций и на них стоит мост.

    Набор - подмножество _ch_settings (bench_runtime), которое имеет смысл
    для java: max_threads (только когда потолок задан явно - иначе клетка
    уехала бы на серверном дефолте рядом с прибитым в 1 остальным),
    output_format_parallel_formatting (иначе смена одного слова FORMAT молча
    меняет число серверных потоков) и session_timezone (вторая половина пары
    к TimeZone=UTC у PostgreSQL). Форматных ручек Arrow/Parquet/JSON тут нет:
    java читает выдачу построчно и этих форматов не берет.
    """
    tail = [f"output_format_parallel_formatting={int(PARALLEL_FMT)}",
            "session_timezone='UTC'"]
    if MAX_THREADS:
        tail.insert(0, f"max_threads={int(MAX_THREADS)}")
    return SQL_CH + "\nSETTINGS " + ", ".join(tail)


SQL_CH_JDBC = _sql_ch_jdbc()

if MODE == "digest" and CANVAS == "gen" and MAX_THREADS != "1":
    raise SystemExit(
        "гейт идентичности требует BENCH_MAX_THREADS=1: с несколькими "
        "потоками порядок строк из numbers() недетерминирован, и sha256 "
        "разойдется не из-за данных, а из-за порядка")

if os.environ.get("BENCH_SHOW_SQL"):  # предполетная сверка холста глазами
    print(f"-- PG:\n{SQL_PG}\n-- CH:\n{SQL_CH}", file=sys.stderr)


def _expected_rows() -> int:
    """Сколько строк обязано приехать. На холстах gen, hits и hits_legacy
    это аргумент генератора или LIMIT, на narrow размер зашит в имя таблицы.
    Нужно там, где drain идет по сырым байтам и строки в потоке не
    различимы."""
    if CANVAS in ("gen", "hits", "hits_legacy"):
        return ROWS
    return NARROW_ROWS.get(TABLE.rsplit("_", 1)[-1].lower(), 0)


def _flight_sql(query: str) -> str:
    """Flight-сессия не привязана к базе - квалифицируем таблицу.
    На холсте gen квалифицировать нечего: таблицы нет."""
    if CANVAS == "gen":
        return query
    name = HITS_TABLE.get(CANVAS, TABLE)
    return query.replace(f"FROM {name}", f"FROM {CH['db']}.{name}")


# ------------------------------------------ формы запроса П4/П6 (ось Д1)
# Обе формы живут ТОЛЬКО на холсте narrow: точечный запрос и keyset-выгрузка
# осмыслены на таблице с ключом, у генерации ключа нет. На url1-таблицах нет
# ключевых колонок - валидация ниже, при импорте, а не на середине клетки.
#
# Ключ точечного запроса - WatchID: он числовой, детерминированно упорядочен
# и по нему у PostgreSQL СОЗДАЕТСЯ ИНДЕКС в блоке 0 подготовки данных
# (без индекса клетка П4 меряет sequential scan, а не точечный доступ).
# ClickHouse-таблицы narrow созданы с ORDER BY tuple() - там точечный запрос
# честно платит полный скан, и это находка клетки, а не ошибка стенда.
#
# Ключ keyset-выгрузки - составной: WatchID сам по себе не уникален, а keyset
# по неуникальному ключу со строгим '>' молча теряет строки-дубли. Составной
# кортеж - тот же набор числовых колонок, которым таблицы narrow
# детерминированно отбирались при создании; PostgreSQL под него получает
# составной индекс в том же блоке 0.

POINT_KEY = "WatchID"
# Тот же набор, что и ключ порядка гейта (_ORDER_COLS): один источник правды -
# иначе гейт сверял бы один порядок, а keyset-выгрузка шла по другому
KEYSET_COLS = _ORDER_COLS
_COLS10_LIST = tuple(c.strip() for c in COLS10.split(","))
# позиции ключевых колонок в SELECT * (порядок колонок narrow-таблиц =
# COLS10): по ним путь достает кортеж-ключ из последней строки чанка
KEYSET_POS = tuple(_COLS10_LIST.index(c) for c in KEYSET_COLS)

if QUERY_FORM == "ordered":
    # форма гейта (F13): порядок и срез, одинаковые на обоих движках. На
    # генерации она бессмысленна - там порядок и так детерминирован номером
    # строки, а ключевых колонок нет; url1-таблицы законны (порядок по URL)
    if CANVAS == "gen":
        raise SystemExit(
            "BENCH_QUERY_FORM=ordered на холсте gen: детерминированный "
            "порядок генерации задан номером строки, сортировать нечего "
            "- гейт 2 генерации снимается профилями")
elif QUERY_FORM:
    if CANVAS != "narrow":
        raise SystemExit(
            f"BENCH_QUERY_FORM={QUERY_FORM}: формы point/keyset живут только "
            "на холсте narrow - у генерации нет ни таблицы, ни ключа")
    if "url1" in TABLE:
        raise SystemExit(
            f"BENCH_QUERY_FORM={QUERY_FORM} на {TABLE}: у url1-таблиц нет "
            "ключевых колонок (WatchID и остальных) - форма неприменима")


def point_sql(engine: str, key=None) -> str:
    """Текст точечного запроса. Для psycopg key=None дает плейсхолдер %s -
    один текст на все исполнения, автоподготовка драйвера работает как в
    жизни. Остальные клиенты (adbc, clickhouse-connect, flight) получают
    ключ ЛИТЕРАЛОМ: параметры у них передаются несопоставимо по-разному,
    а int-литерал безопасен и одинаков; текст при этом меняется на каждом
    исполнении - server-side prepared там не возникает (пометка в CONTRACT).
    """
    where = "%s" if key is None else str(int(key))
    return f"SELECT * FROM {TABLE} WHERE {POINT_KEY} = {where}"


def point_keys_sql(engine: str, n: int) -> str:
    """Детерминированная выборка n ключей: каждая (rows/n)-я строка по
    порядку ключа. Случайность порядка обстрела дает seeded shuffle на
    стороне пути - выборка воспроизводима, ключи гарантированно существуют.
    Снимается ДО окна замера (в connect-фазе пути)."""
    step = max(_expected_rows() // n, 1)
    # F15 / SRVEXEC-01: выборка ключей делается в connect-фазе и в ночи F
    # становилась самой тяжелой строкой окна у всех клеток d1-p4-*, подменяя
    # собой сотню точечных запросов. Маркер стоит ВНУТРИ оператора, а не
    # ведущим: ведущий комментарий pg_stat_statements может не сохранить, и
    # фильтр слоя (_PG_SELF_FILTER, s.query ILIKE '%bench:setup%') промахнется
    if engine == "pg":
        return (f"SELECT {SETUP_MARKER} {POINT_KEY} FROM "
                f"(SELECT {POINT_KEY}, row_number() OVER "
                f"(ORDER BY {POINT_KEY}) AS rn FROM {TABLE}) s "
                f"WHERE (rn - 1) % {step} = 0 ORDER BY {POINT_KEY} LIMIT {n}")
    return (f"SELECT {SETUP_MARKER} {POINT_KEY} FROM "
            f"(SELECT {POINT_KEY}, rowNumberInAllBlocks() AS rn FROM "
            f"(SELECT {POINT_KEY} FROM {TABLE} ORDER BY {POINT_KEY})) "
            f"WHERE rn % {step} = 0 ORDER BY {POINT_KEY} LIMIT {n}")


def keyset_sql(first: bool) -> str:
    """Чанк keyset-выгрузки для psycopg: первый - без WHERE, следующие -
    строгое сравнение кортежей от последней строки предыдущего чанка
    (значения идут параметрами %s). Пара сравнения - сплошной SELECT *
    той же таблицы, обычной клеткой."""
    cols = ", ".join(KEYSET_COLS)
    if first:
        return f"SELECT * FROM {TABLE} ORDER BY {cols} LIMIT {KEYSET_CHUNK}"
    marks = ", ".join(["%s"] * len(KEYSET_COLS))
    return (f"SELECT * FROM {TABLE} WHERE ({cols}) > ({marks}) "
            f"ORDER BY {cols} LIMIT {KEYSET_CHUNK}")
