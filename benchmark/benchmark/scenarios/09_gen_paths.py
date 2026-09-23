#!/usr/bin/env python3
"""Точка входа сценария: реестр путей, служебные флаги, запуск обвязки.

ЗАКОН словаря имен - scenarios/CONTRACT.md. Имена путей, переменных, режимов
и настроек берутся только оттуда: когда раннер и сценарий написаны на разных
словарях, кейсы молча отбрасываются или едут на дефолтах, и корпус получается
связным на вид и неверным по содержанию. Поэтому список путей печатается
флагом --list-paths, а раннер обязан сверять по нему свой (гейт словаря).

Сценарий нарезан на модули (карта - в CONTRACT.md):
  bench_axes     оси, env-валидации, подключения
  bench_sql      построение SQL обоих движков
  bench_runtime  drain / execs, целевые структуры, гейты, srvcost, digest
  paths_pg       пути PostgreSQL (psycopg, COPY, ADBC, connectorx)
  paths_ch       пути ClickHouse (Native, HTTP, эмуляции, Flight)
  paths_analyst  блок аналитика (pandas, файловый маршрут, duckdb)
  paths_jvm      Java-пути (опциональный модуль; без него реестр - питоновский)
  _srvcontrol    серверные метрики (EXPLAIN SERIALIZE / FORMAT Null / query_log)

СЛУЖЕБНЫЕ ФЛАГИ (все печатают в stdout и выходят с кодом 0):
  --list-paths  имена путей по одному в строке - гейт словаря раннера.
                Обязан работать при ЛЮБОМ окружении: перед импортом осевых
                модулей кривые осевые переменные снимаются (см. _scrub);
  --list-profiles
                профили холста gen: "имя<TAB>cross|intra". cross - профиль
                обязан сойтись побайтово между движками (гейт 2 его
                регистрирует), intra - пара типов у движков разная по
                построению (g_map, g1f64) и клетки сравниваются только
                внутри движка. Предполет берет правило отсюда, а не из
                комментария сетки (F13);
  --list-gate2-paths
                точки гейта идентичности: "тег<TAB>путь<TAB>движок<TAB>
                ожидание<TAB>окружение<TAB>причина", по одной в строке.
                Ожидание - cross | pin | rows_bytes | file_sha (семантика -
                в CONTRACT, раздел 8). Строки с ожиданием не cross и есть
                реестр ОЖИДАЕМЫХ расхождений (bench_axes.
                EXPECTED_DIGEST_DIVERGENCE): без него предполет был бы
                вечно красным, а без гейта - слепым (F13 / DATA-03);
  (режимы count и csv - java-блочные: точка входа отклоняет их на
  питоновском пути, см. _check_java_mode)

  --print-sql   РЕАЛЬНЫЙ текст запроса для пути из --path (без --path - текст
                PostgreSQL) - гейт плана раннера снимает план по нему,
                запасной эталонной строки быть не должно. Для эмуляций и
                для java-пути jdbc_ch печатается именно исполняемый текст
                (с хвостом SETTINGS).

Все три списочных флага не зависят от осей и обязаны работать при любом
окружении - оси снимаются перед импортом модулей.
"""

import os
import sys


# Списочные служебные флаги: печатают статические реестры, от осей не
# зависят - значит обязаны работать при любом окружении (см. _scrub).
_LIST_FLAGS = ("--list-paths", "--list-profiles", "--list-gate2-paths")


def _scrub_axis_env() -> None:
    """Снять осевые переменные перед списочными флагами.

    Валидации осей исполняются при импорте модулей и падают SystemExit на
    кривых значениях - это правильно для замера, но служебный флаг обязан
    печатать список путей при любом окружении: гейт словаря раннера иначе
    не отличит "сценарий сломан" от "переменная кейса кривая". Список путей
    от осей не зависит, поэтому оси здесь безопасно сводятся к дефолтам.
    """
    for name in list(os.environ):
        if name.startswith("BENCH_"):
            del os.environ[name]
    # порты подключений превращаются в int при импорте - мусор в них уронил
    # бы --list-paths так же, как кривая ось
    for name in ("PG_PORT", "CH_HTTP_PORT", "CH_TCP_PORT"):
        os.environ.pop(name, None)


if __name__ == "__main__" and any(f in sys.argv[1:] for f in _LIST_FLAGS):
    _scrub_axis_env()

from _measure import run_scenario

from bench_axes import (ENGINE, EXPECTED_DIGEST_DIVERGENCE, GATE2_PATHS,
                        JAVA_MODES, MODE, PROFILES, axis_extras,
                        profile_needs_cross_sha)
from bench_sql import (SQL_CH, SQL_CH_EMU, SQL_CH_JDBC, SQL_PG,
                       _flight_sql)
from paths_pg import PATHS_PG
from paths_ch import PATHS_CH
from paths_analyst import PATHS_ANALYST, _duckdb_sql

PATHS = {}
PATHS.update(PATHS_PG)
PATHS.update(PATHS_CH)
PATHS.update(PATHS_ANALYST)

# Java-пути живут в одном реестре с питоновскими: гейт словаря покрывает их
# наравне со всеми, отдельного словаря имен у Java-клеток нет. Модуль
# опционален: без него сценарий полноценен на питоновской части, но об
# усеченном реестре говорится вслух - молча пропавшие Java-клетки выглядели
# бы как дыра серии.
try:
    from paths_jvm import PATHS_JVM
    PATHS.update(PATHS_JVM)
except ImportError:
    print("# paths_jvm не импортируется - Java-пути в реестре отсутствуют",
          file=sys.stderr)

# Какой движок обслуживает путь - нужно только для --print-sql. Эмуляции
# отвечают по чужому проводу, но SQL разбирают свой, ClickHouse, и исполняют
# текст с хвостом SETTINGS (ch_emu). У file_export движок задается окружением,
# а duckdb_file вообще не ходит на сервер - у него свой SQL, поверх файла.
# Java-пути ходят теми же текстами, что питоновские (движок по имени).
_PATH_SQL = {
    "pg_psycopg": "pg", "pg_copy": "pg", "pg_adbc": "pg",
    # pg_flightsql едет по Flight, но SQL разбирает PostgreSQL, и текст у него
    # тот же самый, что у остальных PG-путей. Квалифицировать таблицу базой,
    # как у ch_flightsql, не нужно: база выбрана заголовком соединения
    # x-flight-sql-database, а сессия адаптера - обычный бэкенд PostgreSQL
    "pg_flightsql": "pg",
    "pg_server_cursor": "pg", "pg_execs": "pg", "cx_pg": "pg",
    "ch_native": "ch", "ch_http": "ch", "ch_pg_emu": "ch_emu",
    "ch_mysql_emu": "ch_emu", "ch_flightsql": "flight",
    "ch_adbc_http": "flight",
    "pg_pandas": "pg", "file_export": (ENGINE or "pg"), "duckdb_file": "duckdb",
}
# Java-пути: движок выводится из имени (jdbc_pg -> pg, jdbc_ch -> ch_jdbc).
# PATH-10: у java серверные настройки ClickHouse едут хвостом SETTINGS в
# тексте запроса (ручек URL, одинаковых на всех ветках драйвера, нет), и
# клиент ЛОВИТ рассогласование - при заданном BENCH_MAX_THREADS без
# max_threads в тексте клетка падает. Значит --print-sql обязан печатать
# именно SQL_CH_JDBC: гейт плана и захваты сверяют исполняемый текст, а не
# его укороченного двойника (запрос исполнителя runtime, п. 4).
for _name in PATHS:
    if _name not in _PATH_SQL:
        _PATH_SQL[_name] = "ch_jdbc" if "_ch" in _name else "pg"


def _argv_path(argv) -> str:
    """Значение --path из командной строки (--path NAME или --path=NAME)."""
    for i, arg in enumerate(argv):
        if arg == "--path" and i + 1 < len(argv):
            return argv[i + 1]
        if arg.startswith("--path="):
            return arg.split("=", 1)[1]
    return ""


def _service_flags(argv) -> None:
    """Служебные флаги: печать в stdout и выход с кодом 0.

    Все они обслуживают гейты раннера, поэтому разбираются ДО argparse
    обвязки (она про них не знает и упала бы на неизвестном аргументе) и
    работают без подключения к базам. Гейт словаря сверяет свой список путей
    с --list-paths, гейт идентичности берет профили и точки сверки из
    --list-profiles и --list-gate2-paths, гейт плана снимает план PostgreSQL
    на тексте из --print-sql - запасной эталонной строки в раннере быть не
    должно, иначе гейт проверяет сервер, а не сценарий.
    """
    if "--list-paths" in argv:
        for name in sorted(PATHS):
            print(name)
        raise SystemExit(0)
    if "--list-profiles" in argv:
        # порядок реестра, а не алфавит: лестницы ширины и прайс типов
        # читаются глазом группами, как они и объявлены
        for name in PROFILES:
            print(f"{name}\tcross" if profile_needs_cross_sha(name)
                  else f"{name}\tintra")
        raise SystemExit(0)
    if "--list-gate2-paths" in argv:
        for tag, path, engine, expect, env, why in GATE2_PATHS:
            print("\t".join((tag, path, engine, expect, env, why)))
        # сверка двух реестров на месте: пропущенная строка ожидаемого
        # расхождения означала бы вечно красный (или слепой) предполет
        declared = {tag for tag, _p, _e, expect, _v, _w in GATE2_PATHS
                    if expect != "cross"}
        if declared != set(EXPECTED_DIGEST_DIVERGENCE):
            raise SystemExit(
                "реестры гейта 2 разошлись: GATE2_PATHS против "
                "EXPECTED_DIGEST_DIVERGENCE - см. bench_axes")
        raise SystemExit(0)
    if "--print-sql" in argv:
        name = _argv_path(argv)
        if name and name not in _PATH_SQL:
            raise SystemExit(f"--print-sql: путь {name!r} неизвестен, "
                             f"см. --list-paths")
        # без --path печатаем текст PostgreSQL: гейт плана - про план PG
        engine = _PATH_SQL.get(name, "pg")
        if engine == "duckdb":
            print(_duckdb_sql())        # у этого пути сервера нет - есть файл
        elif engine == "flight":
            print(_flight_sql(SQL_CH))
        elif engine == "ch_jdbc":
            print(SQL_CH_JDBC)          # хвост SETTINGS - в самом тексте
        elif engine == "ch_emu":
            # эмуляции исполняют текст с вшитым SETTINGS - печатается именно
            # он: гейт и захваты обязаны видеть исполняемый текст, а не его
            # укороченного двойника
            print(SQL_CH_EMU)
        else:
            print(SQL_CH if engine == "ch" else SQL_PG)
        raise SystemExit(0)


def _merge_axis_extras(result, base: dict):
    """Дописать осевые факты в результат пути; значение пути СИЛЬНЕЕ (T12).

    Формы результата - обе из контракта обвязки: кортеж (rows, ttfb[, extra
    [, extras]]) и список словарей (ось самоускорения, замер на исполнение).
    Пустое значение пути считается НЕзаданным - ровно так же на него смотрит
    обвязка (_measure.emit заполняет пустые codec_level / drain_class /
    frame своей запасной таблицей), поэтому поведение клетки не меняется:
    меняется источник умолчания - теперь это одна функция bench_axes.
    """
    if isinstance(result, list):
        for rec in result:
            for key, val in base.items():
                if val and not rec.get(key):
                    rec[key] = val
        return result
    if not isinstance(result, tuple):
        return result
    rows, ttfb = result[0], result[1]
    extra = result[2] if len(result) > 2 else None
    extras = dict(result[3]) if len(result) > 3 else {}
    for key, val in base.items():
        if val and not extras.get(key):
            extras[key] = val
    return (rows, ttfb, extra, extras)


def _with_axis_extras(paths: dict, name: str) -> dict:
    """Осевые факты клетки считает ОДНА функция - bench_axes.axis_extras
    (CONTRACT, п. 7.1). До этой правки функция была мертвой: codec_level /
    drain_class / frame приходили либо от самого пути, либо из ЗАПАСНЫХ
    таблиц обвязки, а контракт обещал единственный источник (T12).

    Словарь считается ЗДЕСЬ, до входа в окно замера: внутри окна лишний
    вызов - это микросекунды, но у клеток с wall в единицы миллисекунд они
    уже видны, а окно обязано мерить путь, а не обвязку.

    max_threads в схеме строки нет и не будет (схема закрыта 54 колонками):
    обвязка молча выбрасывает этот ключ из extras (AXIS_ONLY_KEYS), а факт
    уходит в паспорт полосы отдельной строкой stderr - журнал полосы пишется
    целиком, и по нему клетку с потолком потоков видно (F25 / GRID-14).
    """
    if not name or name not in paths:
        return paths                    # оркестраторный режим: путь выберет
    base = axis_extras(name)             # подпроцесс, у него своя обертка
    threads = base.get("max_threads", "")
    if threads:
        # подстрока 'axis max_threads=' - контракт с паспортом полосы;
        # решетка в начале - обычная разметка диагностики сценария
        print(f"# axis max_threads={threads}", file=sys.stderr)
    fn = paths[name]

    def wrapped():
        return _merge_axis_extras(fn(), base)

    wrapped.__name__ = getattr(fn, "__name__", name)
    wrapped.__doc__ = getattr(fn, "__doc__", None)
    out = dict(paths)
    out[name] = wrapped
    return out


def _check_java_mode(argv) -> None:
    """Режимы count и csv законны только на java-путях (запрос java, п. 10).

    Их умеет один клиент - Java (count считает строки на своей стороне, csv
    пишет файл в BENCH_CSV_OUT для точки file_sha гейта идентичности).
    Питоновский путь на таком режиме не упал бы: ветки "не drain и не
    digest" в bench_runtime снимают материализацию - то есть клетка ушла бы
    в корпус чужим замером под меткой гейта. Проверка стоит здесь, рядом с
    реестром путей: имя пути известно только точке входа.
    """
    if MODE not in JAVA_MODES:
        return
    name = _argv_path(argv) or "(не задан)"
    if not name.startswith("jdbc_"):
        raise SystemExit(
            f"BENCH_MODE={MODE} - режим java-блока, а путь {name} к нему не "
            "относится: count и csv умеет только Java-клиент "
            "(см. CONTRACT, раздел 2)")


if __name__ == "__main__":
    _service_flags(sys.argv[1:])
    _check_java_mode(sys.argv[1:])
    run_scenario("09_gen_paths",
                 _with_axis_extras(PATHS, _argv_path(sys.argv[1:])))
