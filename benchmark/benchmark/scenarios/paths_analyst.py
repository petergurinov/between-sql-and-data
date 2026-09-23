"""Блок аналитика: pandas.read_sql, файловый маршрут и duckdb над файлом.

Рамка дословно: "я аналитик данных или датасаентист и хочу выкачать табличку
к себе, чтобы ее покрутить". Ось блока - НЕ "какая библиотека быстрее", а
МЕСТО СБОРКИ ДАТАФРЕЙМА, и все четыре класса обязаны быть на графике:
  1. через питоновские кортежи (pandas в ветке SQLiteDatabase, polars поверх
     DB-API);
  2. через Arrow в клиенте (ADBC, Flight, HTTP Arrow-форматы);
  3. сразу в целевую структуру силами Rust или C++ (connectorx, duckdb,
     колоночная выдача);
  4. мимо драйвера, через файл (file_export и duckdb_file поверх выгрузки).
Сравнение скоростей внутри блока законно только после гейта BENCH_MODE=dfgate.
"""

import gc
import hashlib
import json
import os
import resource
import shutil
import subprocess
import sys
import tempfile
import time
import warnings
from pathlib import Path

import pandas as pd
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

try:  # duckdb держит ровно один путь блока - без него должны работать остальные
    import duckdb
except ImportError:
    duckdb = None

try:  # sqlalchemy нужен ровно одному варианту pg_pandas
    import sqlalchemy as sa
except ImportError:
    sa = None

# F02: единая фабрика соединений PostgreSQL и обратное чтение контракта
# сессии - тот же модуль зовет гейт сессии раннера
import _session
# F08: сброс watermark пика и его снятие путем (клетка read2)
import _measure

import bench_axes
from bench_axes import (BATCH, CANVAS, CODEC, ENGINE, FILE, FILE_FMTS, FMT,
                        MODE, PROFILE, RESTORE_TYPES, ROWS, SHAPE, TABLE,
                        TARGET, TARGET_SET, VARIANT, WIDTH)
from bench_sql import SQL_CH, SQL_PG, _expected_rows
from bench_runtime import (HTTP_BLOCK_BYTES, _arrow_to_tuples, _raw_result,
                           _assert_pg_codec, _byte_blocks, _ch_connect_http,
                           _ch_settings, _close, _dfgate, _drain,
                           _dtypes_first, _extras, _frame_dtypes,
                           _maybe_restore, _maybe_retained, _pg_connect,
                           _require_polars, _single, _srvcost,
                           digest_rows, pl, restore_types)


def _axis_flag(name: str, env: str) -> bool:
    """Флаговая ось среза v1.5 с запасным входом через окружение.

    Разбор и валидация живут в bench_axes (единый словарь имен), но пути
    обязаны работать и на дереве, где ось еще не зарегистрирована - тогда
    берется то же значение из окружения и тот же дефолт контракта (0).
    """
    value = getattr(bench_axes, name, None)
    if value is None:
        value = os.environ.get(env, "0").strip().lower() in ("1", "on", "true")
    return bool(value)


# F6 (MEAS-11): холодное чтение файла - ОСЬ, а не новое поведение по
# умолчанию. Клетки серии F читали из страничного кеша (an-file-pg-csv-read1
# показывал cpu ровно равным wall, то есть нулевой iowait, а duckdb читал
# 60.6 МиБ за 0.208 с при дисковой пробе машины 112 МиБ/с) - они остаются
# мостом под старыми метками, честные снимаются новыми клетками -cold.
FILE_COLD = _axis_flag("FILE_COLD", "BENCH_FILE_COLD")
# F6 (MEAS-11 п.2): выгрузка со сбросом на диск - отдельная клетка
# (export_durable), а не молчаливая правка старой. Клетка без fsync честна
# для сценария "команда вернула управление", клетка с fsync - для "файл лежит
# на диске"; на слайде называется вслух, какая из двух цифр берется.
FILE_DURABLE = _axis_flag("FILE_DURABLE", "BENCH_FILE_DURABLE")


def _analyst_target(default: str = "df") -> str:
    """Целевая структура для путей блока аналитика.

    Незаданная переменная сводится к df, а не роняет клетку: дефолт сценария -
    tuples, и он верен для путей провода, но у аналитика датафрейм и есть
    цель. Явно заданный tuples по-прежнему падает - это уже не забывчивость
    раннера, а ошибка в описании кейса, и молчать о ней нельзя.
    """
    return TARGET if TARGET_SET else default


def _pandas_call(build):
    """Вызов pandas с перехватом предупреждений.

    На голом соединении psycopg pandas печатает UserWarning "pandas only
    supports SQLAlchemy connectable..." и уходит в ветку SQLiteDatabase, где
    строит фрейм из питоновских кортежей своим обходным путем. Это готовый
    артефакт для слайда, поэтому предупреждение НЕ подавляется.

    Но и оставлять его самому себе нельзя по двум причинам. Реестр warnings
    печатает такое сообщение один раз на процесс - в серии из нескольких
    исполнений оно молча исчезло бы со второго. И стандартный вывод занят
    CSV - чужая строка там сломала бы разбор. Отсюда simplefilter("always"),
    перехват списком и печать в stderr одной строкой с решеткой.
    """
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        frame = build()
    for item in caught:
        text = " ".join(str(item.message).split())   # многострочные - в одну
        print(f"# pandas warning: {item.category.__name__}: {text}",
              file=sys.stderr)
    return frame


def _pandas_result(read, t0):
    """Разбор режима для pandas.read_sql. read(kwargs) -> DataFrame."""
    kwargs = {}
    structure = _analyst_target()
    if MODE == "materialize":
        if structure == "df_arrowdtype":
            # третья точка оси целевой структуры: тот же провод и тот же
            # разбор, но фрейм ложится в буферы Arrow, а не в numpy
            kwargs["dtype_backend"] = "pyarrow"
        elif structure != "df":
            raise SystemExit(
                f"BENCH_TARGET_STRUCT={structure}: pandas.read_sql строит ровно "
                "одну структуру - pandas-датафрейм. Законны df и "
                "df_arrowdtype; за колоночной выдачей и Arrow идите на пути, "
                "которые их отдают физически")

    t1 = time.perf_counter()
    df = _pandas_call(lambda: read(kwargs))
    build_s = time.perf_counter() - t1

    if MODE == "drain":
        # честно: read_sql собирает фрейм целиком, потока тут нет ни на
        # секунду - первая строка приезжает вместе с последней
        return {"rows": len(df), "ttfb_s": time.perf_counter() - t0,
                "client_dtype": "drain:buffered"}
    if MODE == "digest":
        return {"rows": len(df),
                "digest": digest_rows(df.itertuples(index=False, name=None)),
                "client_dtype": _frame_dtypes(df)}
    if MODE == "dfgate":
        return _dfgate(df, build_s)
    return _maybe_retained(
        {"rows": len(df), "extra_s": build_s,
         "client_dtype": _frame_dtypes(df)}, df, "df")


def path_pg_pandas():
    """pandas.read_sql - самая частая строка кода у аналитика.

    Два варианта (BENCH_VARIANT) - это не два драйвера, а две ВЕТКИ САМОГО
    pandas на одном и том же psycopg:
      psycopg     - голое DB-API соединение. pandas такого не знает, печатает
                    предупреждение и уходит в ветку SQLiteDatabase: вычитывает
                    курсор кортежами и собирает фрейм из них. Класс 1 - сборка
                    через питоновские кортежи;
      sqlalchemy  - штатная ветка SQLDatabase поверх того же psycopg.
    Провод в обоих случаях один, разница целиком в клиенте - ровно то, что
    доклад и утверждает.

    BENCH_FMT тут не ось: кодировку значений выбирает pandas, а не мы.
    """
    if MODE == "srvcost":
        # путь ходит по pg-wire, серверная цена у него та же, что у psycopg
        return _srvcost("pg")

    variant = VARIANT or "psycopg"
    if variant not in ("psycopg", "sqlalchemy"):
        raise SystemExit(
            f"BENCH_VARIANT={VARIANT!r}: у пути pg_pandas два варианта - "
            "psycopg (голое DB-API соединение, ветка SQLiteDatabase) и "
            "sqlalchemy (штатная ветка SQLDatabase)")
    if FMT not in ("", "simple_text"):
        raise SystemExit(
            f"BENCH_FMT={FMT!r} у пути pg_pandas: pandas открывает курсор сам "
            "и кодировку значений не выбирает - клетка уехала бы в графики "
            "под чужой меткой формата")
    _assert_pg_codec()

    t0 = time.perf_counter()
    if variant == "psycopg":
        conn = _pg_connect()
        try:
            rec = _pandas_result(lambda kw: pd.read_sql(SQL_PG, conn, **kw), t0)
        finally:
            _close(conn)
        rec.setdefault("proto_mode", "simple")
        return _single(rec)

    if sa is None:
        raise SystemExit(
            "BENCH_VARIANT=sqlalchemy, а пакет sqlalchemy не установлен - "
            "это пустая клетка окружения, а не сбой замера "
            "(pip install sqlalchemy)")
    # F02: соединение и контракт сессии - общей фабрикой _session (SET через
    # exec_driver_sql, а не через _pg_session: тот коммитит на уровне psycopg
    # и вышиб бы из-под SQLAlchemy ее собственную транзакцию). Там же, в фазе
    # подключения, значения читаются обратно с сервера и сверяются с
    # ожидаемыми - расхождение роняет клетку классом session:<ключ>.
    # Гейт сессии раннера открывает соединение ЭТОЙ ЖЕ фабрикой, поэтому
    # проверяется ровно тот механизм, которым идет замер.
    conn = _session.pg_connect("sqlalchemy", canvas=CANVAS, verify=True)
    engine = conn.engine
    try:
        with conn:
            # Строку pandas отдает в exec_driver_sql, а paramstyle psycopg -
            # pyformat, и плейсхолдеры драйвер разбирает даже при пустом
            # наборе параметров (SQLAlchemy передает не None, а {}). Одиночный
            # % оператора остатка он принимает за начало %(name)s и роняет
            # клетку - "incomplete placeholder". Удвоение драйвер сворачивает
            # обратно при сборке текста, поэтому на сервер уходит тот же
            # запрос, что у остальных путей: план и результат не меняются.
            # Ветка psycopg получает голое соединение, разбора плейсхолдеров
            # там нет вовсе - экранировать ее нечем и незачем.
            sql = SQL_PG.replace("%", "%%")
            rec = _pandas_result(lambda kw: pd.read_sql(sql, conn, **kw), t0)
    finally:
        engine.dispose()
    rec.setdefault("proto_mode", "simple")
    return _single(rec)


def _file_fmt(path_name: str) -> str:
    """BENCH_FMT как имя формата ФАЙЛА - строчные csv или parquet.

    Регистр здесь несущий: Parquet с большой буквы - это имя формата
    ClickHouse на проводе, а нам нужен формат файла на диске. Две разные
    сущности в одной колонке CSV - это ровно тот класс ошибки, ради которого
    написан контракт.
    """
    fmt = FMT.strip().lower()
    if fmt not in FILE_FMTS:
        raise SystemExit(
            f"BENCH_FMT={FMT!r} у пути {path_name}: формат файла задается "
            f"строчными именами {FILE_FMTS} (контракт)")
    return fmt


def _sql_text() -> str:
    """Текст запроса выгрузки - он же отпечаток содержимого файла."""
    return SQL_CH if (ENGINE or "pg") == "ch" else SQL_PG


def _sql_fingerprint() -> str:
    """sha256 текста запроса: он кодирует холст, профиль, объем, ширину,
    плотность NULL, длину строки и таблицу разом."""
    return hashlib.sha256(_sql_text().encode("utf-8")).hexdigest()


def _sql_fingerprints() -> set:
    """Отпечатки ОБОИХ движков: выгрузка и чтение - разные процессы, и у
    читателя (duckdb_file, read1/read2) BENCH_ENGINE может быть не задан или
    другой, тогда как содержимое файла по построению одно (гейт 2 сверяет
    выдачу движков побайтово). Смоук финала 2026-09-06: duckdb_file отвергал
    свежий parquet, выгруженный движком ch, потому что считал отпечаток
    по SQL PostgreSQL."""
    return {hashlib.sha256(t.encode("utf-8")).hexdigest() for t in (SQL_PG, SQL_CH)}


def _export_path(fmt: str) -> Path:
    """Куда кладется выгрузка.

    Имя обязано быть ДЕТЕРМИНИРОВАННЫМ: варианты export, read1 и read2 - три
    разных процесса, и duckdb_file - четвертый; состояние между ними не
    передается ничем, кроме этого имени. BENCH_FILE перекрывает автоимя
    (контракт) - тогда все четверо смотрят в один файл явно.

    PATH-14: имя строится не перечислением осей (список ровно так и
    разъехался - холсты hits и hits_legacy давали ОДНО имя при разных
    данных, а BENCH_NULL_DENSITY и BENCH_STR_LEN в него не входили вовсе), а
    читаемым префиксом плюс коротким отпечатком ТЕКСТА запроса. Отпечаток
    закрывает и текущие коллизии, и любую будущую ось без правки имени.
    """
    if FILE:
        return Path(FILE)
    if CANVAS == "gen":
        shape = f"-c{SHAPE}" if PROFILE == "shape" else ""
        stem = f"gen-{PROFILE}{shape}-{ROWS}"
    elif CANVAS == "narrow":
        stem = f"narrow-{TABLE}"
    else:
        stem = f"{CANVAS}-{WIDTH}-{ROWS}"
    fp = _sql_fingerprint()[:12]
    return (Path(tempfile.gettempdir())
            / f"bench-{ENGINE or 'pg'}-{stem}-{fp}.{fmt}")


def _meta_path(target: Path) -> Path:
    """Паспорт выгрузки рядом с ней: .meta вместо голого target.exists()."""
    return target.with_suffix(target.suffix + ".meta")


def _write_meta(target: Path, fmt: str, engine: str, size: int) -> None:
    """Записать паспорт файла: число строк, размер, отпечаток запроса и хеш
    первых килобайт. Чтение сверяет его и падает на расхождении - иначе
    несвежий файл прошлого прогона молча уехал бы в клетку под правильной
    меткой (PATH-14)."""
    with target.open("rb") as fh:
        head = hashlib.sha256(fh.read(1 << 16)).hexdigest()
    meta = {
        "rows": _expected_rows(), "size": size, "fmt": fmt, "engine": engine,
        "sql_sha256": _sql_fingerprint(), "head_sha256": head,
    }
    _meta_path(target).write_text(json.dumps(meta, ensure_ascii=False),
                                 encoding="utf-8")


def _require_export(target: Path, fmt: str) -> None:
    """Гейт свежести выгрузки перед окном чтения (вне замера).

    Паспорта может не быть - файл выгружен прошлым срезом харнесса или задан
    руками через BENCH_FILE; это пометка в лог, а не остановка. А вот
    несовпадение отпечатка запроса или размера - остановка: клетка прочитала
    бы ЧУЖОЙ файл и отчиталась бы правильной меткой.
    """
    if not target.exists():
        raise SystemExit(
            f"{target} не существует: чтение идет по уже выгруженному файлу - "
            "сначала прогоните ту же клетку с BENCH_VARIANT=export")
    meta_file = _meta_path(target)
    if not meta_file.exists():
        print(f"# файл {target.name} без паспорта .meta - свежесть не "
              "проверена (выгрузка старым срезом или BENCH_FILE вручную)",
              file=sys.stderr)
        return
    try:
        meta = json.loads(meta_file.read_text(encoding="utf-8"))
    except ValueError as exc:
        raise SystemExit(f"паспорт {meta_file} не читается: {exc}")
    want = _sql_fingerprints()
    if meta.get("sql_sha256") not in ("", None) and meta.get("sql_sha256") not in want:
        raise SystemExit(
            f"файл {target} выгружен ДРУГИМ запросом (паспорт "
            f"{meta.get('sql_sha256', '')[:12]}, клетка {_sql_fingerprint()[:12]}) - "
            "клетка прочитала бы чужие данные под своей меткой")
    size = target.stat().st_size
    if meta.get("size") not in (None, size):
        raise SystemExit(
            f"файл {target}: размер {size} против паспорта {meta['size']} - "
            "выгрузка оборвана или файл переписан")


def _file_passport(target: Path) -> None:
    """Паспорт файлового маршрута в лог клетки (MEAS-11 п.3).

    Тип монтирования каталога выгрузки решает, что вообще меряют файловые
    клетки: tmpfs означает "мы меряем память", а nrd - настоящий диск. В
    манифесте серии F этого не было ни строкой.
    """
    where = str(target.parent)
    if shutil.which("findmnt"):
        out = subprocess.run(
            ["findmnt", "-no", "SOURCE,FSTYPE,OPTIONS", where],
            capture_output=True, text=True)
        if out.returncode == 0 and out.stdout.strip():
            print(f"# файловый маршрут: findmnt {where} -> "
                  f"{' '.join(out.stdout.split())}", file=sys.stderr)
            return
    out = subprocess.run(["stat", "-f", "-c", "%T", where],
                         capture_output=True, text=True)
    fstype = out.stdout.strip() if out.returncode == 0 else "?"
    print(f"# файловый маршрут: {where} тип ФС {fstype or '?'}",
          file=sys.stderr)


def _drop_page_cache(target: Path) -> None:
    """Вытеснить файл из страничного кеша ПЕРЕД окном чтения (ось FILE_COLD).

    Порядок обязателен: сначала fsync, потом POSIX_FADV_DONTNEED. DONTNEED
    сбрасывает только ЧИСТЫЕ страницы, поэтому без fsync грязные остались бы
    в кеше и правка не сработала бы молча - ровно тот класс тишины, который
    и сделал файловые клетки серии F замером страничного кеша.
    """
    if not FILE_COLD:
        return
    fd = os.open(str(target), os.O_RDONLY)
    try:
        try:
            os.fsync(fd)
        except OSError as exc:  # на некоторых ФС fsync по O_RDONLY отказывает
            print(f"# холодное чтение: fsync не прошел ({exc}) - грязные "
                  "страницы могли остаться в кеше", file=sys.stderr)
        fadvise = getattr(os, "posix_fadvise", None)
        if fadvise is None:
            raise SystemExit(
                "BENCH_FILE_COLD=1, а posix_fadvise на этой системе нет "
                "(не Linux): вытеснить файл из кеша нечем, клетка мерила бы "
                "горячее чтение под меткой холодного")
        fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
    finally:
        os.close(fd)
    print(f"# холодное чтение: {target.name} вытеснен из страничного кеша",
          file=sys.stderr)


# F6 + F16: досмотр файлового пути (паспорт маршрута, гейт свежести
# выгрузки, вытеснение из кеша) обязан идти ДО окна замера. В одиночном
# контракте _measure окно открывает сама обвязка - все, что путь делает
# первым делом, уже попало в wall. В срезе v1.5 это стало заметно: findmnt
# запускается подпроцессом, а гейт свежести читает и хеширует паспорт, и
# десятки миллисекунд молча приписывались бы чтению файла и duckdb - тем
# самым клеткам, которые сверяются мостом с серией F.
#
# Поэтому досмотр делается один раз на процесс и по возможности ДО того, как
# _measure откроет окно: имя пути известно из аргументов сценария
# (--path file_export | duckdb_file). Внутри окна остается обращение к кешу.
# Падение досмотра, случившееся до окна, не теряется, а откладывается и
# поднимается уже внутри клетки - тогда обвязка успевает записать строку
# status=error, и гейт полноты серии не досчитается клетки.
_PREPARED = {"kind": None, "error": None}


def _file_prepare(kind: str, defer: bool = False) -> None:
    """Досмотровые работы файлового пути вне окна замера (F6, F16).

    kind - имя пути (file_export или duckdb_file). Повторный вызов с тем же
    именем ничего не делает (или поднимает отложенную ошибку): вызов внутри
    пути остается для запусков, где досмотр до окна не случился - ручной
    прогон, чужая точка входа, другой порядок аргументов.
    """
    if _PREPARED["kind"] == kind:
        if _PREPARED["error"] is not None:
            raise _PREPARED["error"]
        return
    _PREPARED["kind"] = kind
    try:
        if kind == "duckdb_file":
            target, fmt = _duckdb_source()
            _require_export(target, fmt)
            _file_passport(target)
            # холодное чтение - тоже досмотр: fsync и вытеснение страниц
            # стоят времени, и в окне они были бы приписаны разбору файла
            _drop_page_cache(target)
            return
        fmt = _file_fmt("file_export")
        target = _export_path(fmt)
        _file_passport(target)
        if VARIANT in ("read1", "read2"):
            _require_export(target, fmt)
            if VARIANT == "read1":
                # read2 по определению читает уже прогретый файл - вытеснять
                # его значило бы отменить смысл варианта
                _drop_page_cache(target)
    except SystemExit as exc:
        _PREPARED["error"] = exc
        if not defer:
            raise


def _prepare_from_argv() -> None:
    """Позвать досмотр на импорте модуля, если сценарий запущен на файловом
    пути: импорт идет до того, как _measure открывает окно клетки."""
    argv = sys.argv
    if "--path" not in argv:
        return
    idx = argv.index("--path")
    kind = argv[idx + 1] if idx + 1 < len(argv) else ""
    if kind in ("file_export", "duckdb_file"):
        _file_prepare(kind, defer=True)


def _now_ms() -> int:
    """Граница окна замера в тех же единицах, что ts/ts_end схемы _measure -
    миллисекунды UTC epoch. Путь заполняет ИМЕННО эти колонки: заведенные
    было маркеры ts_measure/ts_measure_end в схему не приняли (схема закрыта
    на 43 колонках, requests-measure п.18), а ts/ts_end обвязка ставит только
    тогда, когда путь их не задал (_measure.py:606) - значит зачетное окно
    называется существующими колонками."""
    return int(time.time() * 1000)


def _export_pg(target: Path, fmt: str) -> int:
    """Выгрузка из PostgreSQL: COPY TO STDOUT прямо в файл.

    Кортежей в питоне не возникает вообще - драйвер отдает байты, мы их
    пишем. Это и есть класс 4 "мимо драйвера".

    parquet тут пустая клетка по построению: такого формата вывода у сервера
    PostgreSQL нет. Аналитик получает parquet конверсией уже в клиенте, а это
    другой замер, и мешать его в ту же колонку нельзя.
    """
    if fmt != "csv":
        raise SystemExit(
            f"BENCH_FMT={fmt} при BENCH_ENGINE=pg: сервер PostgreSQL умеет "
            "выгружать только текст и csv, формата parquet у него нет вовсе. "
            "Это честная пустая клетка, она проговаривается на слайде")
    conn = _pg_connect()
    try:
        # HEADER true обязателен: без него у файла нет имен колонок, и
        # прочитанный фрейм разошелся бы с остальными путями блока по ширине
        stmt = f"COPY ({SQL_PG}) TO STDOUT (FORMAT csv, HEADER true)"
        with target.open("wb") as fh, conn.cursor().copy(stmt) as cp:
            for chunk in cp:
                fh.write(chunk)
            _fsync_file(fh)
    finally:
        _close(conn)
    return target.stat().st_size


def _export_ch(target: Path, fmt: str) -> int:
    """Выгрузка из ClickHouse: потоковый ответ пишется в файл как есть.

    raw_stream, а не raw_query: второй втянул бы весь ответ в память замерного
    процесса, и пик RSS был бы про замерялку, а не про выгрузку. CSVWithNames
    вместо CSV - по той же причине, что HEADER у PostgreSQL: имена колонок.
    """
    client = _ch_connect_http()
    ch_fmt = "CSVWithNames" if fmt == "csv" else "Parquet"
    try:
        stream = client.raw_stream(SQL_CH, fmt=ch_fmt, settings=_ch_settings())
        try:
            with target.open("wb") as fh:
                for chunk in _byte_blocks(stream):
                    fh.write(chunk)
                _fsync_file(fh)
        finally:
            _close(stream)
    finally:
        _close(client)
    return target.stat().st_size


def _fsync_file(fh) -> None:
    """Дождаться записи на диск, если клетка это обещает (ось FILE_DURABLE).

    MEAS-11 п.2: реальный аналитик, делающий psql \\copy, сброса на диск не
    ждет, поэтому клетка без fsync честна для сценария "команда вернула
    управление" и остается мостом с серией F. Клетка с fsync честна для
    сценария "файл лежит на диске" и стоит примерно на секунду дороже на
    90 МиБ - обе снимаются, на слайде называется, какая из двух цифр взята.
    """
    if not FILE_DURABLE:
        return
    fh.flush()
    os.fsync(fh.fileno())


def _export_file(target: Path, engine: str, fmt: str) -> int:
    """Выгрузка через временное имя с атомарной подменой.

    Оборванная выгрузка не должна оставить после себя укороченный файл: его
    молча прочитали бы варианты read1, read2 и путь duckdb_file - без единой
    ошибки, но с неверным числом строк под правильной меткой. Ровно такие
    тихие подмены контракт и запрещает.
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    part = target.with_suffix(target.suffix + ".part")
    try:
        size = _export_pg(part, fmt) if engine == "pg" \
            else _export_ch(part, fmt)
    except BaseException:
        part.unlink(missing_ok=True)
        raise
    os.replace(part, target)
    _write_meta(target, fmt, engine, size)
    return size


def _read_arrow_file(target: Path, fmt: str):
    return pacsv.read_csv(target) if fmt == "csv" else pq.read_table(target)


def _read_pandas_file(target: Path, fmt: str, arrow_dtype: bool = False):
    """Чтение выгрузки в pandas.

    PATH-08: csv типов не хранит, и на оси Д2 (BENCH_RESTORE_TYPES=1) фрейм
    обязан прийти СТРОКАМИ - иначе pandas выводит типы сам, restore_types
    такие колонки уже не видит (там условие series.dtype != object), и клетка
    "доводка типов" доплачивает только за две колонки из десяти. Безусловный
    dtype=str брать нельзя: он молча переписал бы смысл клеток ядра
    an-file-pg-csv-read1/read2, где типы выводит сам pandas. Для parquet
    ничего не меняется - там типы лежат в файле.
    """
    kwargs = {"dtype_backend": "pyarrow"} if arrow_dtype else {}
    if fmt == "csv":
        if RESTORE_TYPES and not arrow_dtype:
            kwargs["dtype"] = str
        return pd.read_csv(target, **kwargs)
    return pd.read_parquet(target, **kwargs)


def _read_polars_file(target: Path, fmt: str):
    _require_polars()
    return pl.read_csv(target) if fmt == "csv" else pl.read_parquet(target)


def _file_result(target: Path, fmt: str, t0):
    """Чтение выгруженного файла в целевую структуру.

    Тут кончается провод и начинается диск: сервер, драйвер и протокол из
    уравнения выведены целиком, осталась только цена разбора формата и цена
    целевой структуры. Ради этого сравнения блок и заведен.
    """
    if MODE == "drain":
        # уровень доставки для файла - просто прочитать байты и отпустить
        with target.open("rb") as fh:
            return _drain(_byte_blocks(fh), t0, bytes_of=len, klass="raw",
                          path="file_export")
    if MODE == "digest":
        table = _read_arrow_file(target, fmt)
        rows = _arrow_to_tuples(table)
        return {"rows": len(rows), "digest": digest_rows(rows),
                "client_dtype": _dtypes_first(rows)}
    if MODE == "dfgate":
        t1 = time.perf_counter()
        df = _read_pandas_file(target, fmt)
        build_s = time.perf_counter() - t1
        if RESTORE_TYPES:
            # PATH-08 п.3: у путей провода восстановление в гейте есть
            # (bench_runtime._rows_result), у файлового пути его не было
            # вовсе - гейт сверял НЕвосстановленный фрейм, и завести в него
            # csv-клетку было бессмысленно
            df, restore_s = restore_types(df)
            rec = _dfgate(df, build_s)
            rec["extra2_s"] = restore_s
            return rec
        return _dfgate(df, build_s)

    t1 = time.perf_counter()
    structure = _analyst_target()
    if structure == "raw":
        blob = target.read_bytes()
        # F07/F44: байты файла живут до stop через общую воронку raw
        return _raw_result(blob, t1)
    if structure == "arrow":
        table = _read_arrow_file(target, fmt)
        return _maybe_retained(
            {"rows": table.num_rows, "extra_s": time.perf_counter() - t1,
             "client_dtype": ";".join(str(f.type) for f in table.schema)},
            table, "arrow")
    if structure in ("df", "df_arrowdtype"):
        df = _read_pandas_file(target, fmt,
                               arrow_dtype=(structure == "df_arrowdtype"))
        # ось Д2 (BENCH_RESTORE_TYPES=1): csv типов не хранит, и доводка
        # прочитанного фрейма до типов - отдельная фаза extra2_s
        return _maybe_retained(_maybe_restore(
            {"rows": len(df), "extra_s": time.perf_counter() - t1,
             "client_dtype": _frame_dtypes(df)}, df), df, "df")
    if structure == "polars":
        frame = _read_polars_file(target, fmt)
        return _maybe_retained(
            {"rows": len(frame), "extra_s": time.perf_counter() - t1,
             "client_dtype": _frame_dtypes(frame)}, frame, "polars")
    raise SystemExit(
        f"BENCH_TARGET_STRUCT={structure} у пути file_export: файл читают в "
        "датафрейм или в Arrow - законны df, df_arrowdtype, arrow, polars и "
        "raw. Кортежи, колоночная выдача и np - это уже конверсия готового "
        "фрейма, а не место его сборки")


def path_file_export():
    """Выгрузка в файл и чтение с диска - класс 4, мимо драйвера.

    Три варианта (BENCH_VARIANT), и третий тут не для симметрии:
      export - выгрузка. Байты идут по проводу и сразу на диск, ни одного
               питоновского объекта по дороге;
      read1  - первое чтение файла с диска;
      read2  - ВТОРОЕ чтение, отдельной строкой. Именно оно и есть правильная
               метрика сценария "выкачал, чтобы покрутить": крутят не один
               раз, и цена выгрузки размазывается по всем последующим чтениям.

    Поперек вариантов идут две оси среза v1.5 (F6): BENCH_FILE_DURABLE=1
    заставляет выгрузку дождаться диска (клетка export_durable), а
    BENCH_FILE_COLD=1 вытесняет файл из страничного кеша перед первым
    чтением (клетки -cold). Обе по умолчанию выключены: клетки серии F
    читали из кеша и не ждали записи, и под старыми метками они остаются
    мостом.

    Оба чтения отдаются СПИСКОМ из одной записи. В списочном контракте
    _measure время меряет сам путь - значит в него не попадает ни поиск
    файла, ни прогревочное чтение варианта read2. Мерить их обвязкой значило
    бы записать в "второе чтение" сумму двух.

    Сжатие тут запрещено сознательно: BENCH_CODEC у ClickHouse рулит и
    транспортом, и внутренностями формата, и при непустом кодеке мы записали
    бы в файл с расширением parquet gzip-поток HTTP. Сжатие файла - отдельная
    ось, в этой серии ее нет.

    ВАЖНО про гейт датафрейма: csv не хранит типов, и прочитанный из него
    фрейм законно отличается от фрейма курсорного пути (время станет строкой,
    Decimal - float). Это находка блока, а не сбой замера, поэтому гейт
    идентичности датафрейма по file_export снимается на parquet.
    """
    if MODE == "srvcost":
        raise SystemExit(
            "BENCH_MODE=srvcost у пути file_export: серверная цена снимается "
            "на путях провода (pg_psycopg, ch_http) - тут она была бы дублем "
            "той же цифры под другой меткой")
    if CODEC != "none":
        raise SystemExit(
            f"BENCH_CODEC={CODEC} у пути file_export: сжатие файла - отдельная "
            "ось, в этой серии ее нет. При непустом кодеке в файл уехал бы "
            "сжатый поток транспорта, а не формат, которым он назван")

    engine = ENGINE or "pg"
    if engine not in ("pg", "ch"):
        raise SystemExit(
            f"BENCH_ENGINE={ENGINE!r} у пути file_export: ожидается pg или ch")
    fmt = _file_fmt("file_export")
    target = _export_path(fmt)
    # паспорт файлового маршрута и гейт свежести - вне окна замера (F16):
    # на импорте модуля, тут остается обращение к кешу досмотра
    _file_prepare("file_export")

    if MODE == "dfgate":
        # гейт не зависит от варианта: сравнивается ФРЕЙМ, а он появляется
        # только после чтения. Файла может не быть - выгружаем его ДО замера
        if not target.exists():
            _export_file(target, engine, fmt)
        return _single(_file_result(target, fmt, time.perf_counter()))

    if VARIANT == "export":
        # одиночный контракт: wall меряет обвязка, и в него по методике
        # проекта входит установка соединения; байты на проводе она же и
        # считает - для этой клетки они и есть главная цифра
        t1 = time.perf_counter()
        size = _export_file(target, engine, fmt)
        if FILE_DURABLE:
            print("# выгрузка: со сбросом на диск (fsync) - клетка "
                  "export_durable", file=sys.stderr)
        return _single({
            "rows": _expected_rows(),
            "extra_s": time.perf_counter() - t1,
            # структуры в клиенте тут нет вовсе - есть файл, и его размер это
            # вторая половина пары "байт на проводе против байт на диске"
            "client_dtype": f"file:{fmt}:{size}B",
        })

    if VARIANT not in ("read1", "read2"):
        raise SystemExit(
            f"BENCH_VARIANT={VARIANT!r}: у пути file_export три варианта - "
            "export, read1 и read2")
    # гейт свежести и вытеснение из кеша сделаны досмотром (_file_prepare)
    if VARIANT == "read2":
        # F08: прогревочное чтение отпускаем СРАЗУ и сбрасываем watermark
        # пика. До среза stand-g-v1.0 фрейм прогрева жил до конца вызова, а
        # peak_rss_mb клетки read2 брался обвязкой с начала процесса - то
        # есть колонка показывала максимум из ДВУХ чтений (817 МБ против
        # 656 МБ зачетного) и читалась как "второе чтение дороже первого"
        warm = _file_result(target, fmt, time.perf_counter())
        # структура прогрева ушла в holder обвязки (_maybe_retained) - его и
        # отпускаем: без этого прогретый фрейм жил бы до конца вызова и
        # занимал память во время зачетного чтения
        _measure.release_live()
        del warm
        gc.collect()
        _measure.reset_peak_rss()

    # F16 (MEAS-08, запрос srv п.6): ts/ts_end обвязки охватывают ВЕСЬ вызов
    # пути, а у read2 в него входит прогревочное чтение - джойн телеметрии по
    # такому окну приписывает клетке два чтения (доли CPU и байты завышены
    # примерно вдвое). Зачетное окно знает только путь, поэтому он и
    # заполняет ts/ts_end сам: обвязка ставит их лишь тогда, когда путь
    # промолчал. Отдельные колонки-маркеры ts_measure/ts_measure_end в схему
    # не приняты (схема закрыта на 43 колонках) - и они не нужны.
    ts_measure = _now_ms()
    ru0 = resource.getrusage(resource.RUSAGE_SELF)
    start = time.perf_counter()
    rec = _file_result(target, fmt, start)
    rec["wall_s"] = time.perf_counter() - start
    # F08: пик ЗАЧЕТНОГО чтения кладет сам путь - иначе обвязка возьмет
    # watermark процесса, в котором остался прогрев. Колонка базовая (пятая в
    # схеме), через _extras ее класть нельзя: EXTRA_COLS начинаются позже и
    # значение было бы отброшено
    rec["peak_rss_mb"] = _measure.peak_rss_mb()
    # CPU - той же рамкой, что и окно: процессная дельта обвязки у списочного
    # результата считается на ВЕСЬ вызов (_measure.py:580), то есть на оба
    # чтения read2. Своя дельта делает cpu_scope честным exec
    ru1 = resource.getrusage(resource.RUSAGE_SELF)
    # T11: токен кадра - из словаря контракта (CONTRACT 7.1). Файл читается
    # тем же куском, что и HTTP-поток (одна константа HTTP_BLOCK_BYTES), и
    # свое имя block= у этого кадра было ЧЕТВЕРТЫМ словом для одной величины
    return [_extras(rec, frame=(f"http={HTTP_BLOCK_BYTES}"
                                if MODE == "drain" else ""),
                    ts=ts_measure, ts_end=_now_ms(),
                    cpu_user_s=ru1.ru_utime - ru0.ru_utime,
                    cpu_sys_s=ru1.ru_stime - ru0.ru_stime,
                    cpu_scope="exec")]


def _duckdb_source():
    """Файл для duckdb_file и его формат.

    BENCH_FILE - главный источник (контракт); без него берем ровно тот путь,
    куда пишет file_export, а он складывается из холста, профиля, объема,
    BENCH_ENGINE и BENCH_FMT - значит все эти переменные должны быть те же,
    что были на выгрузке. Когда это неудобно, задавайте BENCH_FILE явно:
    одно имя надежнее пяти совпадений.
    """
    if FILE:
        target = Path(FILE)
        fmt = target.suffix.lower().lstrip(".")
        if fmt not in FILE_FMTS:
            raise SystemExit(
                f"BENCH_FILE={FILE!r}: формат опознается по расширению, "
                f"ожидается одно из {FILE_FMTS}")
        return target, fmt
    fmt = _file_fmt("duckdb_file")
    return _export_path(fmt), fmt


def _duckdb_sql() -> str:
    target, fmt = _duckdb_source()
    reader = "read_csv_auto" if fmt == "csv" else "read_parquet"
    literal = str(target).replace("'", "''")
    return f"SELECT * FROM {reader}('{literal}')"


def path_duckdb_file():
    """duckdb поверх уже выгруженного файла - класс 3 в чистом виде.

    Ни сервера, ни провода, ни драйвера: файл читает и разбирает C++, и в
    целевую структуру он кладет данные сам, не проходя через объекты питона.
    Это верхняя граница того, что вообще достижимо на этом железе для
    сценария "выкачал, чтобы покрутить".

    Проговаривается вслух: число потоков duckdb берет по числу ядер, то есть
    читает файл параллельно, а pandas - в один поток. Это не подкрутка в его
    пользу, а часть ответа на вопрос "где собирается датафрейм": у duckdb
    сборка живет там, где ее можно распараллелить.

    ATTACH к PostgreSQL сюда сознательно не входит (решение по дизайну): он
    открывает до 64 соединений и параллелит чтение - это уже не провод.
    """
    if MODE == "srvcost":
        raise SystemExit(
            "BENCH_MODE=srvcost у пути duckdb_file: сервера в этом пути нет "
            "вовсе - мерить нечего")
    if duckdb is None:
        raise SystemExit(
            "путь duckdb_file требует пакет duckdb, а он не установлен - "
            "это пустая клетка окружения, а не сбой замера "
            "(pip install duckdb)")

    # гейт свежести, паспорт маршрута и вытеснение из кеша - досмотром до
    # окна замера (F16): в одиночном контракте окно открывает обвязка, и
    # подпроцесс findmnt с чтением паспорта уехали бы в wall клетки
    _file_prepare("duckdb_file")
    target, _fmt = _duckdb_source()
    query = _duckdb_sql()

    t0 = time.perf_counter()
    con = duckdb.connect()
    try:
        if MODE == "drain":
            # у duckdb есть настоящий поток батчей Arrow - берем его, а не
            # fetchall: иначе drain мерил бы материализацию
            reader = con.execute(query).fetch_record_batch(BATCH)
            try:
                return _single(_drain(reader, t0,
                                      rows_of=lambda b: b.num_rows,
                                      klass="arrow", path="duckdb_file"))
            finally:
                _close(reader)

        result = con.execute(query)
        if MODE == "digest":
            rows = result.fetchall()
            return _single({"rows": len(rows), "digest": digest_rows(rows),
                            "client_dtype": _dtypes_first(rows)})
        if MODE == "dfgate":
            t1 = time.perf_counter()
            df = result.df()
            return _single(_dfgate(df, time.perf_counter() - t1))

        t1 = time.perf_counter()
        structure = _analyst_target()
        if structure == "tuples":
            # честная точка класса 1: даже C++ платит за каждый питоновский
            # объект - на этой клетке видно, сколько именно
            rows = result.fetchall()
            return _single(_maybe_retained(
                {"rows": len(rows),
                 "extra_s": time.perf_counter() - t1,
                 "client_dtype": _dtypes_first(rows)}, rows, "tuples"))
        if structure == "arrow":
            table = result.arrow()
            return _single(_maybe_retained(
                {"rows": table.num_rows, "extra_s": time.perf_counter() - t1,
                 "client_dtype": ";".join(str(f.type) for f in table.schema)},
                table, "arrow"))
        if structure in ("df", "df_arrowdtype"):
            df = result.df() if structure == "df" \
                else result.arrow().to_pandas(types_mapper=pd.ArrowDtype)
            return _single(_maybe_retained(
                {"rows": len(df),
                 "extra_s": time.perf_counter() - t1,
                 "client_dtype": _frame_dtypes(df)}, df, "df"))
        if structure == "polars":
            _require_polars()
            frame = result.pl()
            return _single(_maybe_retained(
                {"rows": len(frame),
                 "extra_s": time.perf_counter() - t1,
                 "client_dtype": _frame_dtypes(frame)}, frame, "polars"))
        if structure == "np":
            arrays = result.fetchnumpy()
            rows = len(next(iter(arrays.values()))) if arrays else 0
            return _single(_maybe_retained(
                {"rows": rows, "extra_s": time.perf_counter() - t1,
                 "client_dtype": ";".join(f"{k}:{v.dtype}"
                                          for k, v in arrays.items())},
                arrays, "np"))
        raise SystemExit(
            f"BENCH_TARGET_STRUCT={structure} у пути duckdb_file: законны "
            "tuples, arrow, df, df_arrowdtype, polars и np. Сырого потока "
            "duckdb не отдает - он на то и движок, что разбирает файл сам")
    finally:
        con.close()


# Досмотр файлового пути зовется на импорте модуля - то есть до того, как
# _measure откроет окно клетки (F16). Вызов стоит в конце файла: функции
# досмотра нужны уже определенными.
_prepare_from_argv()


PATHS_ANALYST = {
    "pg_pandas": path_pg_pandas,
    "file_export": path_file_export,
    "duckdb_file": path_duckdb_file,
}
