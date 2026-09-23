"""Контракт сессии PostgreSQL: одни фабрики соединений на замер и на гейт.

F02 (внешнее ревью 2026-09-07). До среза stand-g-v1.0 гейт сессии в раннере
поднимал свои соединения СТАРТОВЫМИ КЛЮЧАМИ libpq (options=...), а замерные
пути psycopg и SQLAlchemy делали SET. На remote-контуре с пулером это не
одно и то же: SET доезжает, options - нет. Гейт при этом проверял тот
механизм, которым никто не мерил, а любое расхождение на remote глушил
безусловным exit(0). Отсюда неверная оговорка "через пулер контракт не
доезжает у всех клиентов" и клетка a10-anchor-adbc-copy-w10-1000k-m с
разбросом байтов 17.55%.

Лечение: единственный модуль, который умеет открывать PG-соединение по
контракту, и единственная функция, которая читает контракт ОБРАТНО С СЕРВЕРА.
Его зовут и замерные пути (paths_pg, paths_analyst), и гейт сессии раннера
(standf/runner/session_gate.py) - второй реализации подключения больше нет.

Виды клиентов (kind):
    psycopg     - connect + SET через pg_prepare_session (как мерил путь);
    sqlalchemy  - engine/connection + SET через exec_driver_sql (ветка pandas);
    adbc        - adbc connect, затем SET каждого ключа отдельным execute ВНЕ
                  окна замера: ADBC умеет исполнять произвольный SQL, а
                  стартовые ключи libpq через пулер не доезжают;
    cx          - connectorx SET не умеет вовсе (соединение внутри rust), у
                  него остаются options в URI: pg_connect отдает не
                  соединение, а строку URI, и клетка подписывается
                  session=options;
    flightsql   - Arrow Flight SQL adapter (путь pg_flightsql). Соединение
                  открывает сам путь (gRPC, не libpq), контракт доставляет
                  SELECT set_config(...) - утилитный SET через Flight роняет
                  исполнителя адаптера. Здесь у этого вида только обратное
                  чтение: тот же current_setting, вычитанный целиком.

Обратное чтение (pg_session_readback) идет одним и тем же запросом
current_setting у всех видов - иначе клиенты сравнивались бы по разным
формам ответа. Расхождение с ожиданием у замерных путей - остановка клетки
классом session:<ключ> (_measure.PathExit), а не тихий замер.
"""

import os
import sys
from pathlib import Path

import psycopg

import _measure
import _srvcontrol
from _srvcontrol import pg_prepare_session, pg_startup_options_uri, setup_sql

# Четыре ключа контракта - ровно те, что в _srvcontrol.PG_SESSION_SETTINGS.
# Список имен держим здесь отдельно только для порядка колонок в артефакте
# гейта; значения всегда берутся из единственного источника.
SESSION_KEYS = ("synchronize_seqscans", "jit",
                "max_parallel_workers_per_gather", "TimeZone")

# Виды клиентов, которые обязан опросить гейт сессии (jdbc - отдельной
# java-пробой, ее наличие решает раннер).
PG_CLIENT_KINDS = ("psycopg", "sqlalchemy", "adbc", "cx")


def normalize(name: str, value) -> str:
    """Одна форма значения настройки для сравнения.

    on/off - строчными; числа - строкой без хвостов; зона - как есть (имена
    зон регистрозависимы: UTC и utc сервер отдает по-разному).
    """
    text = str(value).strip().strip("'\"")
    if name == "TimeZone":
        return text
    low = text.lower()
    if low in ("on", "off", "true", "false", "yes", "no", "1", "0"):
        # булевы настройки сервер отдает как on/off, а SET принимает и 1/0
        if name in ("jit", "synchronize_seqscans"):
            return "on" if low in ("on", "true", "yes", "1") else "off"
    try:
        return str(int(text))
    except ValueError:
        return low


def expected_pg_session() -> dict:
    """Ожидаемые значения четырех ключей с учетом текущих осей и холста.

    Источник один - _srvcontrol.PG_SESSION_SETTINGS (в нем уже разобраны оси
    BENCH_PG_JIT и BENCH_PG_PARALLEL). Модуль читается по имени, а не через
    from-импорт: тесты и гейт подменяют кортеж целиком, и снимок на импорте
    сделал бы подмену невидимой. Холст на набор не влияет (PGSESS-01 п.2),
    поэтому аргумента у функции нет вовсе - контракт одинаков на всех холстах.
    """
    return {name: normalize(name, value)
            for name, value in _srvcontrol.PG_SESSION_SETTINGS}


def pg_password() -> str:
    """Пароль PostgreSQL: переменная окружения либо ~/.pgpass по хосту."""
    password = os.environ.get("PGPASSWORD", "")
    if password:
        return password
    host = _pg()["host"]
    pgpass = Path.home() / ".pgpass"
    if pgpass.exists():
        for line in pgpass.read_text().splitlines():
            parts = line.split(":")
            if len(parts) == 5 and parts[0] == host:
                return parts[4]
    return ""


def _pg() -> dict:
    """Реквизиты PostgreSQL из осевого модуля.

    Импорт ленивый: гейт сессии раннера поднимает этот модуль в окружении, где
    осевые переменные замера могут быть не выставлены вовсе, а валидации
    bench_axes падают ПРИ ИМПОРТЕ.
    """
    import bench_axes
    return bench_axes.PG


def _canvas(canvas: str = None) -> str:
    if canvas is not None:
        return canvas
    return os.environ.get("BENCH_CANVAS", "")


def pg_uri(kind: str, canvas: str = None) -> str:
    """URI подключения с контрактом сессии стартовыми ключами libpq.

    Разделитель токенов у connectorx свой (табуляция): пробел на его стороне
    перекодируется в '+', а rust-postgres назад его не разбирает - смоук
    финала 2026-09-06.
    """
    pg = _pg()
    if kind == "cx":
        options = pg_startup_options_uri(_canvas(canvas), sep=chr(9))
    else:
        options = pg_startup_options_uri(_canvas(canvas))
    scheme = "postgresql+psycopg" if kind == "sqlalchemy" else "postgresql"
    return (f"{scheme}://{pg['user']}:{pg_password()}@"
            f"{pg['host']}:{pg['port']}/{pg['db']}?sslmode=disable"
            f"&options={options}")


def _set_statements():
    """SET-и контракта в порядке PG_SESSION_SETTINGS (имена - константы)."""
    return [f"SET {name} = {value}"
            for name, value in _srvcontrol.PG_SESSION_SETTINGS]


def pg_connect(kind: str, canvas: str = None, on_connect=None,
               verify: bool = False):
    """Открыть соединение ТОЙ ЖЕ фабрикой, которой пользуется замерный путь.

    on_connect(conn) - хук пути между connect и первым запросом (ось приемного
    буфера сокета у psycopg): контракт сессии на него не влияет, а порядок
    важен - буфер ставится до первого чтения ответа.

    verify=True (так зовут ЗАМЕРНЫЕ пути) - фактические значения читаются с
    сервера и сверяются с ожидаемыми ЗДЕСЬ ЖЕ, в фазе подключения: клетка с
    недоехавшим контрактом падает классом session:<ключ>. Дефолт False - для
    гейта сессии (C6): он обязан УВИДЕТЬ расхождение всех клиентов и записать
    их в артефакт, а не упасть на первом же.

    kind == "cx" возвращает не соединение, а строку URI: connectorx держит
    соединение внутри библиотеки и SET не умеет.
    """
    if kind == "cx":
        return pg_uri("cx", canvas)
    if kind == "psycopg":
        pg = _pg()
        conn = psycopg.connect(
            host=pg["host"], port=pg["port"], user=pg["user"],
            password=pg_password(), dbname=pg["db"], sslmode="disable",
        )
        if on_connect is not None:
            on_connect(conn)
        # pg_prepare_session ставит все четыре ключа и ТУТ ЖЕ читает их
        # обратно - своей читки после него не делаем: лишний обмен уехал бы
        # внутрь окна замера (у одиночного контракта коннект входит в wall)
        applied = pg_prepare_session(conn)
        if not conn.autocommit:
            conn.commit()
        if verify:
            _verify(kind, {k: normalize(k, v) for k, v in applied.items()})
        return conn
    if kind == "sqlalchemy":
        try:
            import sqlalchemy as sa
        except ImportError:
            raise SystemExit(
                "вид клиента sqlalchemy: пакет sqlalchemy не установлен - "
                "это пустая клетка окружения, а не сбой замера "
                "(pip install sqlalchemy)")
        # URI без options: ветка pandas делает SET, и именно этот механизм
        # обязан проверять гейт. Вариант с options - отдельная проба гейта
        # (имя psycopg-options / sqlalchemy-options), а не замерный путь
        pg = _pg()
        engine = sa.create_engine(
            f"postgresql+psycopg://{pg['user']}:{pg_password()}@"
            f"{pg['host']}:{pg['port']}/{pg['db']}?sslmode=disable")
        conn = engine.connect()
        try:
            # exec_driver_sql, а не pg_prepare_session: тот коммитит на уровне
            # psycopg и вышиб бы из-под SQLAlchemy ее собственную транзакцию
            for stmt in _set_statements():
                conn.exec_driver_sql(stmt)
            if verify:
                _verify(kind, pg_session_readback(kind, conn))
        except BaseException:
            conn.close()
            engine.dispose()
            raise
        return conn
    if kind == "adbc":
        import adbc_driver_postgresql.dbapi as pgdbapi
        conn = pgdbapi.connect(pg_uri("adbc", canvas))
        # F02: стартовые ключи libpq через remote-пулер не доезжают, а SQL
        # ADBC исполняет - значит контракт ставится теми же SET, что у
        # psycopg. Все это ВНЕ окна замера: фаза подключения
        try:
            with conn.cursor() as cur:
                for stmt in _set_statements():
                    cur.execute(setup_sql(stmt))
            try:
                conn.commit()
            except Exception:   # noqa: BLE001 - при autocommit коммита нет
                pass
            if verify:
                _verify(kind, pg_session_readback(kind, conn))
        except BaseException:
            conn.close()
            raise
        return conn
    raise ValueError(f"неизвестный вид клиента PostgreSQL: {kind!r}")


def readback_sql() -> str:
    """ОДИН запрос на все четыре ключа: у одиночного контракта фаза
    подключения входит в wall, и четыре отдельных обмена были бы четырьмя
    лишними round trip внутри окна. Имена ключей - константы модуля,
    литерал безопасен; маркер setup держит служебную читку вне серверной
    статистики клетки (F15)."""
    cols = ", ".join(f"current_setting('{key}') AS {_alias(key)}"
                     for key in SESSION_KEYS)
    return setup_sql(f"SELECT {cols}")


def _alias(key: str) -> str:
    return "s_" + key.lower()


def pg_session_readback(kind: str, conn) -> dict:
    """Прочитать четыре ключа С СЕРВЕРА тем же клиентом, что и мерил.

    Возвращает {ключ: нормализованное значение}. Ключа, которого сервер не
    отдал, в словаре нет - вызывающая сторона отличает "не применилось" от
    "не спросили".
    """
    row = _readback_row(kind, conn)
    out = {}
    for key, value in zip(SESSION_KEYS, row or ()):
        if value is not None:
            out[key] = normalize(key, value)
    return out


def _readback_row(kind: str, conn):
    sql = readback_sql()
    if kind == "cx":
        import connectorx as cx
        # у connectorx соединения на руках нет - читаем той же библиотекой,
        # которой едет замер: иначе проверялся бы чужой сеанс
        table = cx.read_sql(conn, sql, return_type="arrow")
        return [col.to_pylist()[0] if col.to_pylist() else None
                for col in table.columns]
    if kind == "sqlalchemy":
        row = conn.exec_driver_sql(sql).fetchone()
        return list(row) if row else []
    if kind == "flightsql":
        # Flight-курсор читается ЦЕЛИКОМ: на fetchone драйвер бросает
        # недочитанный поток, адаптер отменяет его и пишет в лог сервера
        # "stream ended with error: context canceled" - шум ровно на
        # служебной читке контракта. Одна строка, одна колонка на ключ
        with conn.cursor() as cur:
            cur.execute(sql)
            table = cur.fetch_arrow_table()
        return [col.to_pylist()[0] if col.to_pylist() else None
                for col in table.columns]
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
    return list(row) if row else []


def _verify(kind: str, applied: dict) -> dict:
    """Сверка фактических значений с ожидаемыми; расхождение - падение клетки.

    Клетка, у которой контракт не доехал, обязана падать громко: до этого
    среза она молча мерила другой сервер (jit=on, parallel=2, плавающий
    миллион строк на hits) и уезжала в корпус под нормальной меткой.
    """
    expected = expected_pg_session()
    for key, want in expected.items():
        got = applied.get(key)
        if got != want:
            raise _measure.PathExit(
                f"session:{key}",
                f"контракт сессии PostgreSQL не доехал до сервера: {key} "
                f"ожидалось {want!r}, сервер отдает {got!r} (клиент {kind}). "
                "Клетка мерила бы другой сервер под нашей меткой")
    return applied


def assert_pg_session(kind: str, conn) -> dict:
    """Прочитать сессию с сервера и сверить с ожиданием (вне окна замера).

    Возвращает словарь фактических значений (идет в паспорт и в артефакт
    гейта). Зовется путями, которые открыли соединение мимо pg_connect.
    """
    return _verify(kind, pg_session_readback(kind, conn))


def session_note(kind: str) -> str:
    """Пометка о способе доставки контракта для колонки client_dtype."""
    return "session=options" if kind == "cx" else "session=set"


def _selftest() -> None:
    """Самотест без базы: только формы значений и текст URI."""
    exp = expected_pg_session()
    assert set(exp) == set(SESSION_KEYS), exp
    assert normalize("jit", "ON") == "on"
    assert normalize("max_parallel_workers_per_gather", "0") == "0"
    assert normalize("TimeZone", "UTC") == "UTC"
    print(f"# _session selftest ok: {exp}", file=sys.stderr)


if __name__ == "__main__":
    _selftest()
