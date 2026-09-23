"""Общий хелпер замеров для сценариев стенда.

Формат вывода - одна CSV-строка на исполнение пути (клетка x раунд;
при мультиисполнении - строка на каждый exec).

Семантика базовых метрик:
- wall_s      - время ПУТИ ЦЕЛИКОМ: установка соединения + запрос + полная
                выгрузка + сборка в целевую структуру клиента. Импорт драйвера
                в окно НЕ входит (импорты подняты на верх модулей сценариев),
                соединение - ВХОДИТ: это честная часть стоимости пути.
                Граница остановки часов одна на все языки (F07): целевая
                структура ЖИВА в момент stop, ее освобождение вынесено за
                окно в cleanup_s, служебные пробы пути - в post_check_s.
- connect_s   - время до готовности соединения (TCP+TLS+auth), до отправки
                запроса; раскладывает cold на коннект против JIT/кешей
                драйвера. Меряет сам путь, передает в extras.
- ttfb_s      - время от старта пути (t0 = до connect/execute) до первой
                строки/батча; пусто, если путь не стримит. Включает connect и
                постановку запроса - сравнивать только внутри одного сценария.
- peak_rss_mb - пиковый RSS процесса за окно: VmHWM из /proc/self/status со
                сбросом clear_refs перед окном (ru_maxrss - только фолбэк без
                /proc: он несбрасываемый, в серии исполнений все строки
                получили бы пик первого). В пик входит бейслайн импортов
                стека, который резидентен на момент сброса - бейслайн
                снимается отдельной строкой-калибровкой (BENCH_BASELINE=1).
- rss_settled_mb - RSS в покое С УДЕРЖИВАЕМОЙ структурой (НЕ вес данных):
                после gc.collect() + malloc_trim(0) - иначе glibc/pymalloc не
                отдают ОС фрагментированную кучу и tuples-пути систематически
                завышены против arrow. Заполняют ТОЛЬКО клетки Д6
                (BENCH_RETAINED=1) изнутри пути, при живой структуре; обвязка
                после возврата пути колонку не трогает - структура к этому
                моменту отпущена, и замер был бы про другое (см. CONTRACT).
                Вне /proc (macOS) - пусто.
- retained_mb - вес структуры по ее собственной версии (memory_usage(deep) /
                nbytes / estimated_size); считает путь, передает в extras.
                F08: rss_settled - retained - это ВЕСЬ остальной процесс
                (бейслайн импортов и драйвера, буферы соединения, живые
                промежуточные структуры, фрагментация); ценой аллокатора эта
                разность НЕ является и так называться не должна.
- cleanup_s   - секунды на освобождение удерживаемых структур ПОСЛЕ остановки
                часов (F07): путь отдает готовую структуру в holder обвязки
                (keep_alive), обвязка останавливает wall/CPU, снимает пик и
                только потом отпускает ссылку. Пусто у путей без holder
                (слив, srvcost, эмиссия заглушек).
- post_check_s - секунды служебной проверки ПОСЛЕ окна замера (F01): путь
                регистрирует пробу через register_post_check, обвязка зовет
                ее после остановки часов и чтения счетчиков. Пусто, если
                проверки нет.
- cpu_user_s / cpu_sys_s - CPU клиента: ДЕЛЬТА getrusage(RUSAGE_SELF) вокруг
                окна исполнения (одно чтение кумулятивно с рождения процесса -
                смешало бы импорты и коннект). cpu_scope говорит, чему равно
                окно: exec (окно одного исполнения) или process (дельта всего
                вызова пути - фолбэк для серий, где путь cpu не отдал).
- bytes_rx_mb / bytes_tx_mb / packets_rx / packets_tx - дельта счетчиков
                интерфейса за окно. Пакеты - ПОСЛЕ offload (TSO/GSO/GRO
                склеивают сегменты), сравнимы только внутри одной пары машин.
- steal_ticks - дельта поля steal из /proc/stat за окно (тик 10 ms).
- ts / ts_end - wall-clock границы окна (time.time(), UTC epoch, мс) - для
                джойна с телеметрией. Длительности остаются на perf_counter
                (монотонность); границы - ТОЛЬКО wall clock: эпоха
                perf_counter произвольна и к телеметрии не привязывается.
- status / error_class - ok/error/timeout/skipped + класс ошибки. Строка
                пишется ВСЕГДА, даже при падении пути: полнота серии
                (--list-cases = фактические строки) сверяется механически.
- rows        - число полученных строк (санити-чек объема; типы и ширину
                колонок санити НЕ проверяет).

Точность записи (срез v1.5, находка F2): длительности - ШЕСТЬ знаков
(wall_s, ttfb_s, extra_s, connect_s, extra2_s), байты - ТРИ (bytes_rx_mb,
bytes_tx_mb). Три знака у времени клали точечную ось Д1 на PostgreSQL в
0.000 и молча выключали гейты разброса и steal; один знак у байтов был
грубее допуска гейта счетчиков и давал ложные доборы.

Строки печатаются в stdout и дописываются в results/local-results.csv.
stdout занят CSV: вся диагностика - stderr со строками '# ...'.
"""

import argparse
import csv
import gc
import os
import resource
import subprocess
import sys
import time
import traceback
from pathlib import Path
from typing import Callable, Optional

# Схема - строгое надмножество прежней 17-колоночной: первые 17 колонок под
# теми же именами в том же порядке (старые CSV и сверка моста читаются без
# конвертации), новые колонки дописаны строго в конец.
#   server_time_s    - серверное время исполнения БЕЗ сериализации
#                      (PG: EXPLAIN ANALYZE SERIALIZE NONE; CH: FORMAT Null)
#   serialize_time_s - серверное время сериализации ответа (семантики PG и CH
#                      РАЗНЫЕ - кросс-движковое сравнение лоб в лоб незаконно)
#   out_kb           - объем вывода по версии САМОГО сервера
#   digest           - sha256 канонической выдачи (гейт идентичности данных)
#   fmt              - формат ответа (FORMAT у CH, кодировка значений у PG)
#   codec            - режим сжатия, ЯВНО заданный, а не унаследованный из
#                      дефолта драйвера
#   exec_no          - номер исполнения внутри одного процесса и соединения
#                      (ось самоускорения; cold = 1)
#   target_struct    - целевая структура в памяти клиента
#   client_dtype     - фактические типы первой строки (ловля деградации)
#   extra_s          - фаза 2: конверсия в DataFrame
#   extra2_s         - фаза 3: восстановление типов из строк (текстовые пути)
#   canvas/mat_level - холст и уровень материализации; обязательны у всех
#                      клеток - на них держится запрет незаконных сравнений
#   proto_mode       - фактический режим драйвера PG (simple/extended/
#                      prepared, prepareThreshold) - скрытая переменная
#                      packets_* и ttfb; не ось, но фиксируется
#   lane/contour     - полоса (VM) и контур (remote/direct)
#   round            - номер зачетного раунда
#   codec_level      - уровень транспортного кодека (ось внутри A6; при
#                      codec=none пусто - уровня у несжатой клетки нет)
#   drain_class      - класс слива: raw | rows | arrow | buffered | srvcost;
#                      у не-drain клеток пусто. Слово drain в mat_level
#                      склеивает пять физически разных уровней (F4/GRID-03),
#                      и запрет сравнений держится именно на этой колонке
#   frame            - размер кадра чтения одной строкой: itersize=2000,
#                      max_block_size=65536, fetchSize=200, http=1048576;
#                      пусто, если у пути кадра нет (F4)
HEADER = ["scenario_id", "path", "wall_s", "ttfb_s", "peak_rss_mb", "rows",
          "extra_s", "bytes_rx_mb",
          "server_time_s", "serialize_time_s", "out_kb", "digest",
          "fmt", "codec", "exec_no", "target_struct", "client_dtype",
          "connect_s", "extra2_s", "rss_settled_mb", "retained_mb",
          "cpu_user_s", "cpu_sys_s", "cpu_scope",
          "bytes_tx_mb", "packets_rx", "packets_tx", "steal_ticks",
          "canvas", "mat_level", "proto_mode", "lane", "contour", "round",
          "ts", "ts_end", "status", "error_class",
          # срез v1.5, F4: новые колонки только в КОНЕЦ строки - читатели
          # старого 38-колоночного сырья не ломаются, недостающее пусто
          "codec_level", "drain_class", "frame",
          # срез stand-g-v1.0, F07 и F01: те же правила - только в КОНЕЦ.
          # cleanup_s   - освобождение удерживаемой структуры после stop
          # post_check_s - служебная проба пути после окна замера
          "cleanup_s", "post_check_s",
          # срез C1 (D42-D46): правило прежнее - только в КОНЕЦ строки.
          # timer_contract    - идентификатор контракта измерения: какие
          #     интервалы входят в окно. Строки разных контрактов нельзя
          #     класть в одну таблицу молча (шаг 2 плана C1);
          # codec_id          - идентификатор кодека сверки значений; режим
          #     проверки и число строк входят в само значение digest;
          # schema_sig        - подпись схемы результата (Arrow: имена, типы,
          #     nullability, единицы времени и зона; DataFrame: карта типов и
          #     порядок; кортежи: перепись типов). Численная пара публикуется
          #     только при совпадении подписей обоих плеч;
          # extra_stage       - СТАДИЯ величины extra_s: у разных путей она
          #     означает разные интервалы, и арифметика над величинами разных
          #     стадий запрещена (шаг 6 плана C1);
          # block_attempt_id  - попытка блока: расширяет ключ уникальности
          #     строки сырья, иначе метка, исполненная в двух блоках, роняет
          #     склейку корпуса;
          # entry_id / peer_addr / peer_port - объявленная точка входа и
          #     ФАКТИЧЕСКИЕ адрес и порт соединения: имя переменной окружения
          #     доказательством не является;
          # tls_observed / tls_observed_by - наблюдаемый режим шифрования и
          #     способ наблюдения (чем именно он наблюден);
          # mem_limit_applied - фактически прочитанные пределы памяти клетки
          "timer_contract", "codec_id", "schema_sig", "extra_stage",
          "block_attempt_id", "entry_id", "peer_addr", "peer_port",
          "tls_observed", "tls_observed_by", "mem_limit_applied"]

# Все колонки после bytes_rx_mb пути передают словарем extras.
EXTRA_COLS = HEADER[8:]

# Осевые факты, которые пути отдают вместе с колонками (bench_axes.axis_extras
# кладет их в тот же словарь), но которых в схеме строки НЕТ и не будет: схема
# закрыта 54 колонками (41 среза v1.5, cleanup_s и post_check_s среза
# stand-g-v1.0, одиннадцать колонок среза C1). Их место - паспорт прогона
# (F25/GRID-14: по сырью F
# клетку с потолком max_threads не отличить от клетки без него). Молча
# игнорируем именно ЭТИ имена, а не любые незнакомые: опечатка в имени колонки
# по-прежнему обязана падать, иначе значение потеряется без следа.
#
# R2-06: сюда же adbc_numeric_as. Опция BENCH_ADBC_NUMERIC_AS - проба, и
# драйвер вправе ключ не принять; тогда клетка t3-decbound-adbc-numasdouble
# дает строку сырья, побайтово неотличимую от обычной клетки того же пути, и
# гипотеза T3 остается без доказательства, что опция вообще применилась. Путь
# отдает ФАКТ применения через extras (paths_pg), обвязка принимает его молча
# и не роняет замер: колонки под него в схеме нет и не будет, место факта -
# паспорт прогона и журнал полосы.
AXIS_ONLY_KEYS = {"max_threads", "adbc_numeric_as"}

# Поля, которые раннер задает окружением - подставляются в строку сами,
# чтобы сценарии не дублировали их в каждом пути. Заполняются и в строках
# error/skipped - гейт полноты серии сверяет клетку по этим осям.
ENV_COLS = {"fmt": "BENCH_FMT", "codec": "BENCH_CODEC",
            "target_struct": "BENCH_TARGET_STRUCT",
            "canvas": "BENCH_CANVAS", "mat_level": "BENCH_MAT_LEVEL",
            "lane": "BENCH_LANE", "contour": "BENCH_CONTOUR",
            "round": "BENCH_ROUND"}

# Имя пути текущего процесса (ключ словаря PATHS сценария, оно же поле path
# спецификации клетки). Нужно для класса слива и кадра чтения: метка клетки
# (колонка path в CSV) для этого не годится - она произвольная.
#
# S3: имя приходит двумя дорогами. Штатная - аргумент --path (run_scenario
# зовет set_path_kind до исполнения пути). Запасная - переменная
# BENCH_PATH_KIND для вызовов МИМО сценария: emit_skipped из раннера, пол
# провода, c1_run. Переменную читаем ЛЕНИВО, при каждом обращении: снимок на
# импорте оставлял бы класс слива и кадр пустыми у всех, кто правит
# os.environ уже после import _measure (так делают и wirefloor, и c1_run).
_PATH_KIND = ""


def set_path_kind(name: str) -> None:
    """Объявить имя пути процесса (поле 3 спецификации клетки).

    Зовется точкой входа сценария; отдельная функция нужна вызовам мимо
    сетки - они импортируют модуль и объявляют путь сами."""
    global _PATH_KIND
    _PATH_KIND = (name or "").strip()


def path_kind() -> str:
    """Имя пути: объявленное явно сильнее переменной окружения."""
    return _PATH_KIND or os.environ.get("BENCH_PATH_KIND", "").strip()

# Классы слива по пути и режиму (F4/GRID-03) - ЗАПАСНАЯ таблица обвязки.
# Основную держат bench_axes.drain_class / bench_runtime.drain_class_of (их
# и зовут пути, отдавая класс через extras) и ее двойник в runner/lib.sh для
# --list-cases; здесь она нужна тем вызовам, которые идут мимо сетки (пол
# провода, c1_run, emit_skipped из lib.sh). Импортировать bench_axes нельзя:
# он валидирует оси на импорте и падает вне клетки. Значения обязаны
# совпадать с bench_axes - расхождение и есть тот самый склеенный drain.
# Девятое поле спецификации клетки НЕ вводится (формат 8 полей заморожен).
_DRAIN_CLASS = {
    "ch_http": "raw",          # сырые байты HTTP-ответа, строк не строит
    "pg_copy": "raw",          # поток COPY, куски отпускаются как есть
    "file_export": "raw",      # чтение файла кусками байт
    "ch_native": "rows",       # драйвер собирает строки, мы их отпускаем
    "pg_psycopg": "rows",      # серверный курсор, строка за строкой
    "pg_server_cursor": "rows",
    "ch_pg_emu": "rows",
    "ch_mysql_emu": "rows",
    "jdbc_pg": "rows",         # только с fetchSize; без него - buffered
    "jdbc_ch": "rows",
    "pg_adbc": "arrow",        # батчи Arrow
    "ch_flightsql": "arrow",
    "ch_adbc_http": "arrow",
    "pg_flightsql": "arrow",
    "duckdb_file": "arrow",
    "pg_execs": "buffered",    # fetchmany по уже вычитанному ответу
    "cx_pg": "buffered",       # connectorx строит структуру целиком
    "pg_pandas": "buffered",   # read_sql собирает фрейм, потока нет
}


def drain_class_of(path: str = None, mode: str = None, fmt: str = None,
                   drain_form: str = None) -> str:
    """Класс слива клетки: raw | rows | arrow | buffered | srvcost.

    Функция от (путь, BENCH_MODE, BENCH_FMT, BENCH_DRAIN_FORM) - ровно та же,
    что в runner/lib.sh. У не-drain клеток класса нет (пустая строка): слив
    там не происходит, и колонка не должна изображать уровень.
    """
    path = path_kind() if path is None else path
    mode = os.environ.get("BENCH_MODE", "") if mode is None else mode
    if mode == "srvcost":
        return "srvcost"
    if mode != "drain":
        return ""
    form = os.environ.get("BENCH_DRAIN_FORM", "") if drain_form is None \
        else drain_form
    # pgJDBC без fetchSize НЕ стримит: он выкачивает весь ResultSet в heap, и
    # это ровно тот буферный слив, что у psycopg с BENCH_DRAIN_FORM=buffered
    # (запрос java, п.1; факт печатает сам клиент - drain:buffered). Именно
    # эту ловушку меряет ось порции a8-jdbc_pg-batch0, и объявлять ее rows
    # значит склеивать ее с построчными клетками - тот самый дефект F4.
    # BENCH_BATCH=0 - это НЕ "кадр в ноль строк", а именно "fetchSize не
    # задан" (так записана ступень a8-jdbc_pg-batch0 в сетке): пустая строка
    # и ноль означают одно и то же, и различать их значило бы объявить
    # половину оси порции построчной по опечатке в спецификации
    if path == "jdbc_pg" and os.environ.get("BENCH_BATCH", "").strip() in ("", "0"):
        return "buffered"
    # буферный слив объявляется осью и перекрывает построчный: клиент
    # физически не стримит, клетка не сравнивается по памяти и ttfb. Ось
    # заведена ТОЛЬКО для psycopg (контракт среза): у остальных клиентов
    # формы слива не выбирают, и молча перекрашивать их класс нельзя
    if path == "pg_psycopg":
        return "buffered" if form == "buffered" else "rows"
    return _DRAIN_CLASS.get(path, "")


# СТАДИЯ дополнительного времени extra_s (срез C1, шаг 6).
# Колонка extra_s есть у всех путей, но означает у них РАЗНЫЕ интервалы, и
# до среза C1 это нигде не было записано. Отсюда соблазн вычесть extra_s
# одного пути из extra_s другого и назвать разность "ценой преобразования" -
# то есть сложить интервалы разной природы.
#
#   convert_after_receive  преобразование ПОСЛЕ полного приема выдачи:
#       выдача уже в Arrow (или в буфере драйвера), extra_s меряет только
#       сборку целевой структуры - DataFrame, polars, кортежи;
#   full_query_with_frame  ВЕСЬ запрос вместе со сборкой фрейма: клиент не
#       дает разделить прием и сборку (connectorx, pandas.read_sql), и
#       extra_s тут - не добавка, а почти весь путь;
#   materialize_raw        материализация сырого буфера ответа (COPY, HTTP
#       без разбора): строк нет, есть байты;
#   frame_normalize        нормализация фрейма к объявленной схеме (гейт E1):
#       интервал поверх уже собранного фрейма.
#
# Арифметика над величинами РАЗНЫХ стадий запрещена; запрет держат сводка и
# таблицы (summarize.py: у группы с несколькими стадиями extra не считается).
_EXTRA_STAGE = {
    "cx_pg": "full_query_with_frame",       # connectorx строит фрейм сам
    "pg_pandas": "full_query_with_frame",   # read_sql: приема отдельно нет
    "pg_copy": "materialize_raw",
    "ch_http": "materialize_raw",
    "file_export": "materialize_raw",
}


def extra_stage_of(path: str = None, target: str = None) -> str:
    """Стадия величины extra_s у клетки: см. таблицу _EXTRA_STAGE.

    Пусто означает, что дополнительного времени у клетки нет вовсе (путь
    отдает выдачу без отдельной сборки), а не "стадия неизвестна": колонка
    extra_s у такой строки тоже пуста.
    """
    path = path_kind() if path is None else path
    if path in _EXTRA_STAGE:
        return _EXTRA_STAGE[path]
    target = os.environ.get("BENCH_TARGET_STRUCT", "") if target is None         else target
    mode = os.environ.get("BENCH_MODE", "")
    if mode == "dfgate":
        return "frame_normalize"
    if mode == "drain":
        return ""            # слив ничего не собирает - собирать нечего
    # остальные пути (psycopg, ch_native, ADBC, Flight, HTTP Arrow) сначала
    # принимают выдачу целиком, а потом строят целевую структуру
    if target in ("pandas", "polars", "tuples", "arrow", "df"):
        return "convert_after_receive"
    return ""


# Кадр чтения HTTP-потока: та же константа, что HTTP_BLOCK_BYTES в
# bench_axes/bench_runtime (кусок читает bench_runtime, называет колонка
# frame). Значение не ось: BENCH_BATCH до чтения потока не доходит, и
# печатать кадр только при какой-то переменной значило бы врать пустотой.
HTTP_FRAME_BYTES = 1 << 20


def frame_of(path: str = None) -> str:
    """Размер кадра чтения одной строкой (F4): itersize/max_block_size/
    fetchSize/fetch/http. Пусто там, где кадра нет или он не задан осью.

    Разбор случая - зеркало bench_axes.frame_col (запрос axes, п.1 и п.3):
    расхождение двух копий и есть тот дефект, из-за которого по строке нельзя
    было понять, каким кадром снята клетка.
    """
    path = path_kind() if path is None else path
    batch = os.environ.get("BENCH_BATCH", "").strip()
    mode = os.environ.get("BENCH_MODE", "")
    form = os.environ.get("BENCH_DRAIN_FORM", "").strip() or "cursor"
    # дефолты те же, что в bench_axes: BATCH 10000, ITERSIZE = BATCH
    itersize = os.environ.get("BENCH_ITERSIZE", "").strip() or batch or "10000"
    if path.startswith("jdbc_"):
        # ноль и пустота у Java-клиента означают одно: fetchSize не задан
        # (та же ветка, что в классе слива и в bench_axes -
        # _jdbc_fetch_size_set).
        # Печатать fetchSize=0 значило бы объявить кадром его отсутствие
        return f"fetchSize={batch}" if batch not in ("", "0") else ""
    if path == "pg_server_cursor":
        return f"itersize={itersize}"
    if path == "pg_psycopg":
        # кадр есть только у серверного курсора: буферный слив и mat читают
        # ответ целиком, никакого itersize там не применяется
        return f"itersize={itersize}" if mode == "drain" and form == "cursor" \
            else ""
    if path in ("pg_execs", "cx_pg", "pg_pandas"):
        return f"fetch={batch}" if batch else ""
    if path in ("ch_native", "ch_pg_emu", "ch_mysql_emu"):
        return f"max_block_size={batch}" if batch else ""
    if path == "ch_http":
        return f"http={HTTP_FRAME_BYTES}"
    return ""


def codec_level_of() -> str:
    """Уровень транспортного кодека в колонку codec_level (запасной расчет;
    основной - CODEC_LEVEL_COL в bench_axes, пути отдают его через extras).

    Пусто в двух случаях, и оба означают "ручки уровня в этой клетке нет":
    сжатия нет вовсе и сжатие живет ВНУТРИ формата (транспортная ручка
    сервера до форматного слоя не доходит). Ноль вместо пустоты был бы
    враньем: у zstd ноль - точка шкалы.
    """
    if (os.environ.get("BENCH_CODEC", "none").strip() or "none") == "none":
        return ""
    if os.environ.get("BENCH_CH_CODEC_LAYER", "transport").strip() == "format":
        return ""
    return os.environ.get("BENCH_CH_CODEC_LEVEL", "").strip() or "1"

# Callable пути. Два контракта:
#   1) (rows, ttfb_s | None) ИЛИ (rows, ttfb_s, extra_s) ИЛИ
#      (rows, ttfb_s, extra_s, extras_dict) - один замер, wall/cpu/сеть/ts
#      считает обвязка. extras_dict - словарь с любыми ключами из EXTRA_COLS.
#   2) list[dict] - НЕСКОЛЬКО замеров за один процесс (ось самоускорения:
#      серия исполнений в одном соединении). Каждый dict: wall_s, rows и
#      опционально ttfb_s, extra_s, bytes_rx_mb, peak_rss_mb плюс ключи из
#      EXTRA_COLS. Свое время, байты, пик (reset_peak_rss()/peak_rss_mb()
#      между исполнениями) и границы ts/ts_end в этом режиме считает сам
#      путь; чего путь не отдал, обвязка заполняет границами и процессной
#      cpu-дельтой ВСЕГО вызова (cpu_scope=process).
PathFn = Callable[[], object]

_PROC_STATUS = Path("/proc/self/status")
_CLEAR_REFS = Path("/proc/self/clear_refs")
_PEAK_FALLBACK_NOTED = False
_CLEAR_REFS_NOTED = False


def _now_ms() -> int:
    """Wall-clock метка (UTC epoch, мс) - единственная законная для границ."""
    return int(time.time() * 1000)


def _proc_status_kb(field: str) -> Optional[int]:
    """Поле VmHWM/VmRSS из /proc/self/status, в KB; None вне Linux."""
    try:
        for line in _PROC_STATUS.read_text().splitlines():
            if line.startswith(field + ":"):
                return int(line.split()[1])
    except (OSError, ValueError, IndexError):
        return None
    return None


def reset_peak_rss() -> bool:
    """Сброс пика RSS (VmHWM) к текущему RSS: echo 5 > /proc/self/clear_refs.
    Вызывается обвязкой перед окном и путями между исполнениями серии -
    иначе все исполнения получат watermark самого тяжелого. Без прав или
    вне /proc возвращает False (пик останется историческим)."""
    global _CLEAR_REFS_NOTED
    try:
        _CLEAR_REFS.write_text("5")
        return True
    except OSError:
        if not _CLEAR_REFS_NOTED:
            _CLEAR_REFS_NOTED = True
            print("# peak_rss: clear_refs недоступен - пик не сбрасывается "
                  "между исполнениями, в серии все строки получат общий "
                  "watermark", file=sys.stderr)
        return False


def peak_rss_mb() -> float:
    """Пиковый RSS процесса, MB: VmHWM из /proc/self/status (сбрасываемый
    через reset_peak_rss); фолбэк без /proc - ru_maxrss (Linux - KB,
    macOS - байты; несбрасываемый, помечается в stderr)."""
    global _PEAK_FALLBACK_NOTED
    hwm = _proc_status_kb("VmHWM")
    if hwm is not None:
        return hwm / 2**10
    if not _PEAK_FALLBACK_NOTED:
        _PEAK_FALLBACK_NOTED = True
        print("# peak_rss: /proc недоступен - фолбэк ru_maxrss "
              "(несбрасываемый watermark процесса)", file=sys.stderr)
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return peak / 2**20
    return peak / 2**10


def settled_rss_mb() -> Optional[float]:
    """RSS в покое, MB: gc.collect() + malloc_trim(0), затем VmRSS.
    Без malloc_trim аллокатор не отдает ОС фрагментированную кучу и
    структуры из мелких объектов выглядят систематически тяжелее колоночных.
    Вне Linux (нет /proc или glibc) возвращает None - колонка пустая."""
    if _proc_status_kb("VmRSS") is None:
        return None
    gc.collect()
    try:
        import ctypes
        ctypes.CDLL("libc.so.6", use_errno=True).malloc_trim(0)
    except OSError:
        print("# rss_settled: malloc_trim недоступен - RSS может быть "
              "завышен нефрагментированной кучей", file=sys.stderr)
    rss = _proc_status_kb("VmRSS")
    return rss / 2**10 if rss is not None else None


def _net_counters() -> Optional[dict]:
    """Счетчики сетевого интерфейса (Linux): rx/tx bytes и packets.
    Дельта до/после пути = трафик окна. BENCH_RX_IFACE переопределяет
    интерфейс: когда сервер на той же машине, это lo - на loopback
    счетчики считают ОБЕ стороны диалога, но запросы крошечные и
    доминирует ответ. Пакеты - после offload (TSO/GSO/GRO), сравнивать
    только внутри одной пары машин. На macOS возвращает None."""
    override = os.environ.get("BENCH_RX_IFACE")
    ifaces = (override,) if override else ("eth0", "ens5", "enp0s5")
    for iface in ifaces:
        base = Path(f"/sys/class/net/{iface}/statistics")
        if base.exists():
            try:
                return {name: int((base / name).read_text())
                        for name in ("rx_bytes", "tx_bytes",
                                     "rx_packets", "tx_packets")}
            except (OSError, ValueError):
                return None
    return None


def steal_ticks() -> Optional[int]:
    """Кумулятивный steal из /proc/stat (агрегат по vCPU, тик 10 ms).
    Дельта за окно - в колонку steal_ticks; гейт по ней относительный
    (steal * 10ms / wall_s), абсолютный ноль нервный и слепой."""
    try:
        with open("/proc/stat") as f:
            fields = f.readline().split()
        if fields and fields[0] == "cpu":
            return int(fields[8])  # user nice system idle iowait irq softirq steal
    except (OSError, ValueError, IndexError):
        pass
    return None


def _mem_bytes(text: str):
    """Размер памяти из строки спецификации или из memory.max в байты."""
    text = (text or "").strip()
    if not text or text == "max":
        return None
    mult = 1
    if text[-1:] in "KMGT":
        mult = {"K": 2**10, "M": 2**20, "G": 2**30, "T": 2**40}[text[-1]]
        text = text[:-1]
    try:
        return int(float(text) * mult)
    except ValueError:
        return None


def _mem_label(nbytes) -> str:
    """Байты обратно в запись спецификации (8589934592 -> 8G).

    Класс ошибки обязан называть лимит ТОЙ ЖЕ строкой, что раннер в своей
    ветке (oom:$mem по полю спецификации), иначе клетки одной пары попадут
    в разные классы и сравнение "доехало под лимитом" развалится."""
    if not nbytes:
        return ""
    for suffix, unit in (("G", 2**30), ("M", 2**20), ("K", 2**10)):
        if nbytes % unit == 0 and nbytes >= unit:
            return f"{nbytes // unit}{suffix}"
    return str(nbytes)


def mem_limit_label() -> str:
    """Лимит памяти клетки строкой спецификации (8G) или пусто.

    RPY-02: BENCH_MEM_LIMIT до окружения клетки НЕ доезжает - раннер вынимает
    его из extra_env и отдает systemd-run, в cleaned переменная не попадает.
    Поэтому лимит читается там же, где его читают клиент и paths_jvm
    (_cgroup_mem_max_bytes, BenchJdbc.cgroupMemMaxMb): своя строка
    /proc/self/cgroup (v2) и memory.max этой группы. Переменная окружения -
    запасной вход ручного прогона. Вне Linux файлов нет, и это не ошибка:
    пустой лимит означает "клетка шла без cgroup-лимита", и класс ошибки
    честно остается exit:<код>.
    """
    env = os.environ.get("BENCH_MEM_LIMIT", "").strip()
    if env:
        return env
    try:
        own = ""
        with open("/proc/self/cgroup", encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("0::"):      # единая иерархия (cgroup v2)
                    own = line.strip()[3:]
                    break
        if not own:
            return ""
        with open(f"/sys/fs/cgroup{own}/memory.max", encoding="utf-8") as fh:
            return _mem_label(_mem_bytes(fh.read()))
    except OSError:
        return ""


def _cgroup_value(name: str):
    """Значение файла своей cgroup v2 (memory.max / memory.swap.max).

    Возвращает строку как есть либо None, если файлов нет (не Linux, cgroup
    v1, прогон вне группы). None - это НЕ ошибка: клетка шла без лимита, и
    так и будет записано.
    """
    try:
        own = ""
        with open("/proc/self/cgroup", encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("0::"):
                    own = line.strip()[3:]
                    break
        if not own:
            return None
        with open(f"/sys/fs/cgroup{own}/{name}", encoding="utf-8") as fh:
            return fh.read().strip()
    except OSError:
        return None


def assert_mem_limit_applied() -> str:
    """Проверить, что объявленный предел памяти РЕАЛЬНО применен (шаг 6 C1).

    Клетки профиля объявляют memory_limit и swap_max. До среза C1 это было
    обещание: раннер передавал лимит systemd-run, и если тот его не принял
    (нет делегированной группы, нет прав, опечатка в единице), клетка шла
    БЕЗ лимита и приходила в корпус с правильной меткой и чужой физикой -
    та самая ловушка, из-за которой пара "доехало под лимитом / упало под
    лимитом" сравнивала бы два разных опыта.

    Проверка читает СВОИ значения memory.max и memory.swap.max в начале
    клетки. Расхождение с объявленным - остановка с понятным классом ошибки
    limit:<что именно>, а не тихое продолжение.

    Возвращает строку для колонки mem_limit_applied: "<memory.max>/<swap>".
    Пустая строка означает, что лимит не объявлялся и не применялся.
    """
    want = os.environ.get("BENCH_MEM_LIMIT", "").strip()
    want_swap = os.environ.get("BENCH_SWAP_MAX", "").strip()
    got = _cgroup_value("memory.max")
    got_swap = _cgroup_value("memory.swap.max")
    if got is None:
        if want:
            raise PathExit(
                "limit:no_cgroup",
                f"клетка объявляет предел памяти {want}, но своей cgroup v2 "
                "не видно - предел не применен, и физика клетки не та, что "
                "обещает метка")
        return ""
    applied = f"{got}/{got_swap if got_swap is not None else '-'}"
    if want:
        want_b = _mem_bytes(want)
        got_b = _mem_bytes(got)
        if got_b is None:
            raise PathExit(
                "limit:memory_max_unset",
                f"клетка объявляет предел памяти {want}, а memory.max своей "
                f"группы = {got!r} (без предела) - предел не применен")
        if want_b is not None and got_b != want_b:
            raise PathExit(
                "limit:memory_max_mismatch",
                f"клетка объявляет предел памяти {want} ({want_b} Б), а "
                f"memory.max своей группы = {got!r} ({got_b} Б)")
    if want_swap:
        want_sb = _mem_bytes(want_swap) or 0
        got_sb = _mem_bytes(got_swap) if got_swap is not None else None
        # ноль подкачки читается из файла как "0", и _mem_bytes отдает 0
        if got_swap is None:
            raise PathExit(
                "limit:swap_max_unset",
                f"клетка объявляет предел подкачки {want_swap}, а "
                "memory.swap.max своей группы не читается")
        if (got_sb or 0) != want_sb:
            raise PathExit(
                "limit:swap_max_mismatch",
                f"клетка объявляет предел подкачки {want_swap}, а "
                f"memory.swap.max своей группы = {got_swap!r}")
    return applied


def results_file() -> Path:
    default = Path(__file__).resolve().parent.parent / "results" / "local-results.csv"
    return Path(os.environ.get("RESULTS_FILE", default))


def _fmt_extra(key: str, value) -> str:
    """Числовые дополнительные колонки печатаем с фиксированной точностью,
    текстовые - как есть (запятые в них ломали бы CSV, поэтому режем)."""
    if value is None or value == "":
        return ""
    if key in ("server_time_s", "serialize_time_s"):
        # серверные числа (Execution Time плана, query_duration_ms): их
        # разрешение задает сервер, не наш perf_counter - печать не трогаем
        return f"{float(value):.4f}"
    if key in ("cpu_user_s", "cpu_sys_s"):
        # getrusage отдает микросекунды; четыре знака (0.1 мс) обнуляли cpu
        # точечных исполнений Д1 и коротких клеток - шесть знаков, как у
        # остальных длительностей окна (F2). Сопоставимость с G не страдает:
        # старые значения читаются теми же числами, точнее становится хвост
        return f"{float(value):.6f}"
    # F2: миллисекунда разрешения обнуляла точечную ось Д1 и connect_s
    # ленивых клиентов; perf_counter дает наносекунды, цена шести знаков ноль
    if key in ("connect_s", "extra2_s", "cleanup_s", "post_check_s"):
        return f"{float(value):.6f}"
    # F2: шаг печати байтов 0.1 МБ был грубее допуска гейта счетчиков (0.05)
    # и давал 49 ложных доборов из 63 - три знака снимают их все
    if key == "bytes_tx_mb":
        return f"{float(value):.3f}"
    if key in ("out_kb", "rss_settled_mb", "retained_mb"):
        return f"{float(value):.1f}"
    if key in ("packets_rx", "packets_tx", "steal_ticks", "ts", "ts_end",
               "codec_level"):
        return str(int(value))
    return str(value).replace(",", ";")


# Объявленные исключения таблицы CONTRACT п. 7.2: путь, у которого
# ФАКТИЧЕСКИЙ слив заведомо не совпадает с объявленным классом. Случай
# ровно один - pg-эмуляция ClickHouse: она физически не стримит
# (client_dtype остается drain:buffered), но объявлена rows, чтобы пара
# "psycopg против эмуляции" сравнивалась внутри ОДНОГО класса. Ключ -
# (путь, объявленный класс), значение - допустимый фактический.
# Список закрытый: любое другое расхождение - дефект клетки (RPY-01/RS-06).
DRAIN_DECLARED_EXCEPTIONS = {("ch_pg_emu", "rows"): "buffered"}


class DrainClassMismatch(RuntimeError):
    """Объявленный класс слива клетки разошелся с фактическим."""


def _check_drain_class(label: str, extras: dict) -> Optional[str]:
    """Сверка объявленного класса слива с фактическим (F4). Возвращает текст
    расхождения либо None.

    Фактический класс путь называет сам в client_dtype (drain:<klass>).
    Объявленный класс колонки НЕ переписываем (RPY-01/RS-06): он часть
    спецификации клетки - по нему раннер печатает --list-cases и проверяет
    однородность блока, и подмена колонки фактическим значением разводила
    сырье с сеткой молча. Расхождение вне списка исключений останавливает
    клетку, как обещает CONTRACT 7.2; строка сырья к этому моменту уже
    записана, поэтому доказательство расхождения остается в корпусе.
    """
    dtype = str(extras.get("client_dtype") or "")
    if not dtype.startswith("drain:"):
        return None
    actual = dtype.split(":", 1)[1].split(":", 1)[0]
    if not actual:
        return None
    declared = extras.get("drain_class") or ""
    if not declared:
        # путь позвали не через сетку (пол провода, служебные запуски):
        # объявленного класса нет, берем фактический молча
        extras["drain_class"] = actual
        return None
    if actual == declared:
        return None
    if DRAIN_DECLARED_EXCEPTIONS.get((path_kind(), declared)) == actual:
        # оговорка контракта, а не дефект: клетка сравнивается по
        # объявленному классу, физика слива видна в client_dtype
        return None
    kind = path_kind() or "неизвестен"
    return (f"класс слива клетки {label}: объявлен {declared}, фактически "
            f"{actual} (client_dtype={dtype}, путь={kind})")


def _write_row(row: list) -> None:
    print(",".join(row), flush=True)
    out = results_file()
    out.parent.mkdir(parents=True, exist_ok=True)
    new_file = not out.exists()
    with out.open("a", newline="") as f:
        writer = csv.writer(f)
        if new_file:
            writer.writerow(HEADER)
        writer.writerow(row)


def emit(scenario_id: str, path: str, wall_s: Optional[float],
         ttfb_s: Optional[float], rows: Optional[int],
         extra_s: Optional[float] = None,
         bytes_rx_mb: Optional[float] = None,
         extras: Optional[dict] = None,
         *, peak_mb=None) -> None:
    """Одна строка CSV. status по умолчанию ok; строки error/timeout/skipped
    идут через этот же путь - с пустыми rows/peak (peak_mb="" отключает
    снятие пика: у неисполнявшейся клетки он бессмыслен)."""
    extras = dict(extras or {})
    for key in AXIS_ONLY_KEYS:
        extras.pop(key, None)
    unknown = set(extras) - set(EXTRA_COLS)
    if unknown:
        raise ValueError(f"неизвестные колонки: {sorted(unknown)}")
    for col, env in ENV_COLS.items():  # раннер задает окружением
        if not extras.get(col) and os.environ.get(env):
            extras[col] = os.environ[env]
    if not extras.get("status"):
        extras["status"] = "ok"
    # F4: класс слива, кадр и уровень кодека - производные от осей клетки;
    # путь может отдать их сам (он знает точнее), иначе считаем по контракту
    if not extras.get("codec_level"):
        extras["codec_level"] = codec_level_of()
    if not extras.get("frame"):
        extras["frame"] = frame_of()
    if not extras.get("drain_class"):
        extras["drain_class"] = drain_class_of()
    # срез C1, шаг 6: фактически прочитанные пределы памяти и объявленная
    # точка входа. Точку входа задает раннер переменной окружения - клетка
    # не выдумывает ее сама; фактические адрес, порт и наблюдаемое
    # шифрование кладет путь (наблюдение, а не объявление)
    if not extras.get("mem_limit_applied") and _MEM_APPLIED[0]:
        extras["mem_limit_applied"] = _MEM_APPLIED[0]
    if not extras.get("entry_id") and os.environ.get("BENCH_ENTRY_ID"):
        extras["entry_id"] = os.environ["BENCH_ENTRY_ID"].strip()
    # попытка блока: ее задает секвенсор окружением, и она входит в ключ
    # уникальности зачетной строки корпуса. Без нее метка, исполненная в
    # двух блоках, дает два комплекта раундов под одним ключом
    if not extras.get("block_attempt_id") \
            and os.environ.get("BENCH_BLOCK_ATTEMPT"):
        extras["block_attempt_id"] = os.environ["BENCH_BLOCK_ATTEMPT"].strip()
    mismatch = _check_drain_class(path, extras)

    peak = peak_rss_mb() if peak_mb is None else peak_mb
    row = [
        scenario_id,
        path,
        # F2: шесть знаков у всех длительностей. Три знака клали точечную ось
        # Д1 на PG в 0.000 (1142 строки) и молча выключали гейты spread/steal
        f"{wall_s:.6f}" if wall_s is not None else "",
        f"{ttfb_s:.6f}" if ttfb_s is not None else "",
        f"{peak:.1f}" if isinstance(peak, (int, float)) else "",
        str(rows) if rows is not None else "",
        f"{extra_s:.6f}" if extra_s is not None else "",
        f"{bytes_rx_mb:.3f}" if bytes_rx_mb is not None else "",
    ] + [_fmt_extra(c, extras.get(c)) for c in EXTRA_COLS]
    _write_row(row)
    if mismatch:
        # CONTRACT 7.2: расхождение объявленного и фактического класса слива -
        # ОСТАНОВКА клетки, а не пометка (RS-06). Строку сырья пишем раньше
        # остановки: без нее расхождение осталось бы только в журнале полосы,
        # а раннер дописал бы за клетку безымянный exit. Код возврата 3 -
        # раннер видит ненулевой код, строку не дублирует и снимает
        # оставшиеся раунды метки
        print(f"# ОСТАНОВКА {mismatch}", file=sys.stderr)
        print("# сверять клетки разных классов слива нельзя (CONTRACT 7.2): "
              "либо путь объявил класс неверно, либо ось клетки поменяла "
              "физику слива - разбирать до переноса цифр", file=sys.stderr)
        raise SystemExit(3)


def emit_skipped(label: str, status: str, error_class: str,
                 scenario_id: Optional[str] = None,
                 path_name: Optional[str] = None) -> None:
    """Строка за клетку, которую путь НЕ исполнял: срезанный кейс
    (status=skipped, error_class=cut:<причина>) или таймаут раннера
    (status=timeout). Гейт полноты серии требует строку на каждый кейс
    сетки. Вызов из раннера:
        python3 -c 'import _measure; _measure.emit_skipped(
            "метка", "timeout", "timeout:3600s")'
    scenario_id - аргументом либо переменной BENCH_SCENARIO_ID.
    path_name - имя пути кейса (поле 3 спецификации) аргументом либо
    переменной BENCH_PATH_KIND: без него у строки-заглушки нет ни класса
    слива, ни кадра чтения, и гейт полноты видит клетку без осей (S3)."""
    sid = scenario_id if scenario_id is not None \
        else os.environ.get("BENCH_SCENARIO_ID", "")
    if path_name:
        set_path_kind(path_name)
    # rows - базовая колонка, не из ENV_COLS: объем клетки в заглушке берется
    # из BENCH_ROWS раннера, чтобы срезанную/упавшую клетку можно было
    # опознать по осям, а не только по метке (CONTRACT 5а)
    rows = None
    raw_rows = os.environ.get("BENCH_ROWS", "").strip()
    if raw_rows.isdigit():
        rows = int(raw_rows)
    emit(sid, label, None, None, rows,
         extras={"status": status, "error_class": error_class}, peak_mb="")


def emit_baseline(scenario_id: str, label: str) -> None:
    """Клетка-калибровка бейслайна импортов: пик процесса сразу после
    импортов стека, до какой-либо работы пути. Этот бейслайн (у разных
    драйверов свой) входит в peak_rss_mb боевых клеток пути - для
    межпутевых сравнений его вычитают по этой строке."""
    now = _now_ms()
    emit(scenario_id, f"{label}-baseline", 0.0, None, 0,
         extras={"ts": now, "ts_end": now,
                 "client_dtype": "baseline:imports"})


class PathExit(SystemExit):
    """Остановка клетки с ЯВНЫМ классом ошибки для колонки error_class.

    Обычный SystemExit со строкой уезжает в сырье как exit:1 - имя причины
    остается только в журнале полосы. Гейт сессии (F02/F04) обязан быть
    виден в самой строке клетки, поэтому у него свой класс вида
    session:<ключ>. Код возврата остается ненулевым, как у любого падения.
    """

    def __init__(self, error_class: str, message: str = "", code: int = 1):
        super().__init__(message or error_class)
        self.error_class = error_class
        self.exit_code = code


# --- F07: holder результата (структура жива в момент остановки часов) ------
# Путь кладет сюда готовую структуру, обвязка отпускает ее ПОСЛЕ stop и
# пишет цену освобождения отдельной колонкой cleanup_s. До этого среза python
# успевал освободить кортежи и датафреймы внутри окна (последняя ссылка
# умирала на возврате из пути), а Java держала объект до записи строки - и
# кросс-языковая пара сравнивала два разных контракта.
_LIVE = []

# фактически прочитанные пределы памяти клетки: их пишет каждая строка окна
# (колонка mem_limit_applied), иначе "лимит был" остается обещанием
_MEM_APPLIED = [""]


def keep_alive(obj):
    """Удержать структуру до остановки часов; возвращает тот же объект."""
    _LIVE.append(obj)
    return obj


def release_live() -> Optional[float]:
    """Отпустить удержанные структуры и вернуть цену освобождения, секунды.

    None - удерживать было нечего (пути слива, srvcost, заглушки): нулем это
    писать нельзя, у клетки без holder цены освобождения не существует.
    gc.collect здесь НЕ зовется: пересчет ссылок освобождает структуру сразу,
    а полная сборка добавила бы в колонку чужую работу.
    """
    if not _LIVE:
        return None
    t = time.perf_counter()
    _LIVE.clear()
    return time.perf_counter() - t


# --- F01: служебные пробы пути идут ПОСЛЕ окна замера ----------------------
# Проба кодека ch_native при EXECS=1 сидела внутри wall/CPU/bytes и стоила
# сжатым клеткам 0.005-0.0095 с. Теперь путь ее РЕГИСТРИРУЕТ, а зовет
# обвязка - после остановки часов и чтения счетчиков.
_POST_CHECKS = []


# длительность последней пробы: нужна ветке падения - строка error обязана
# показать, сколько стоила проверка, которая клетку и уронила
_POST_CHECK_S = None


def register_post_check(fn) -> None:
    """Зарегистрировать служебную проверку пути (зовется после окна замера)."""
    _POST_CHECKS.append(fn)


def reset_post_checks() -> None:
    """Сбросить реестр проб: один вызов пути - один набор проверок."""
    global _POST_CHECK_S
    _POST_CHECKS.clear()
    _POST_CHECK_S = None


def run_post_checks() -> Optional[float]:
    """Выполнить зарегистрированные пробы, вернуть их длительность, секунды.

    None - проб не было. SystemExit пробы наружу не гасится: клетка обязана
    падать так же громко, как когда проба стояла внутри окна.
    """
    global _POST_CHECK_S
    _POST_CHECK_S = None
    if not _POST_CHECKS:
        return None
    checks = list(_POST_CHECKS)
    _POST_CHECKS.clear()
    t = time.perf_counter()
    try:
        for check in checks:
            check()
    finally:
        _POST_CHECK_S = time.perf_counter() - t
    return _POST_CHECK_S


def _run_post_checks_or_die(scenario_id: str, label: str, wall: float,
                            ts0: int, ts1: int,
                            cleanup_s: Optional[float]) -> Optional[float]:
    """Служебные пробы пути после окна замера; падение пробы - строка error.

    До среза stand-g-v1.0 проба стояла внутри пути и ее падение уходило через
    общий обработчик _run_path: строки данных не оставалось, клетка падала
    целиком. Это поведение сохраняем дословно - меняется только момент, когда
    проба выполняется (F01). Дословно - значит и для обычного исключения
    (проба поднимает свое соединение, и сетевая ошибка в ней - не SystemExit):
    без этой ветки клетка уходила бы вовсе без строки, а раннер дописывал бы
    за нее безымянный exit:1 вместо имени причины.
    """
    try:
        return run_post_checks()
    except Exception as exc:   # noqa: BLE001 - ровно прежний общий обработчик
        for line in traceback.format_exc().splitlines():
            print(f"# {line}", file=sys.stderr)
        extras = {"status": "error", "error_class": type(exc).__name__,
                  "ts": ts0, "ts_end": ts1}
        if cleanup_s is not None:
            extras["cleanup_s"] = cleanup_s
        if _POST_CHECK_S is not None:
            extras["post_check_s"] = _POST_CHECK_S
        emit(scenario_id, label, wall, None, None, extras=extras, peak_mb="")
        raise SystemExit(1)
    except SystemExit as exc:
        code = exc.code
        if not isinstance(code, int) and code:
            for line in str(code).splitlines():
                print(f"# {line}", file=sys.stderr)
        code = code if isinstance(code, int) else 1
        if code == 0:  # проба сказала "все хорошо" кодом 0 - это не падение
            return _POST_CHECK_S
        err_class = getattr(exc, "error_class", None) or f"exit:{code}"
        print(f"# служебная проба клетки упала (код {code}, {err_class}) - "
              "окно замера она уже не искажает, но клетка недействительна",
              file=sys.stderr)
        extras = {"status": "error", "error_class": err_class,
                  "ts": ts0, "ts_end": ts1}
        if cleanup_s is not None:
            extras["cleanup_s"] = cleanup_s
        if _POST_CHECK_S is not None:
            extras["post_check_s"] = _POST_CHECK_S
        emit(scenario_id, label, wall, None, None, extras=extras, peak_mb="")
        raise SystemExit(code)


def _run_path(scenario_id: str, label: str, fn: PathFn) -> None:
    """Исполнение одного пути с полной обвязкой окна: сброс пика, дельты
    cpu/сети/steal, границы ts/ts_end, строка при падении."""
    if os.environ.get("BENCH_BASELINE") == "1":
        # калибровка: импорты стека уже подняты на верх модуля сценария
        emit_baseline(scenario_id, label)
        return

    # F07/F01: один вызов пути - свой holder и свой набор служебных проб.
    # Реестры модульные, и остаток от прошлого вызова (оркестратор гоняет
    # пути подпроцессами, но самотест и гейты зовут _run_path подряд)
    # приписал бы клетке чужую пробу или чужую структуру
    _LIVE.clear()
    reset_post_checks()

    # Шаг 6 C1: предел памяти обязан быть ПРИМЕНЕН, а не только объявлен.
    # Проверка идет до окна замера и до любой работы пути: клетка без
    # примененного предела несет чужую физику под правильной меткой
    try:
        _MEM_APPLIED[0] = assert_mem_limit_applied()
    except PathExit as exc:
        print(f"# {exc}", file=sys.stderr)
        emit(scenario_id, label, None, None, None,
             extras={"status": "error", "error_class": exc.error_class,
                     "ts": _now_ms(), "ts_end": _now_ms()}, peak_mb="")
        raise SystemExit(exc.exit_code)

    net0 = _net_counters()
    steal0 = steal_ticks()
    reset_peak_rss()
    ru0 = resource.getrusage(resource.RUSAGE_SELF)
    ts0 = _now_ms()
    t0 = time.perf_counter()
    try:
        result = fn()
    except SystemExit as exc:
        # HM4/запрос java п.11: путь, который поднял SystemExit (обычно это
        # подпроцесс-клиент, убитый ядром под cgroup-лимитом памяти), уходил
        # мимо except Exception - строки в сырье не оставалось вовсе, и
        # раннер писал за клетку exit:1 вместо oom:<лимит>. Строку пишем мы:
        # мы одни знаем осевое окружение клетки и границы окна.
        wall = time.perf_counter() - t0
        ts1 = _now_ms()
        code = exc.code
        # RPY-03: SystemExit со СТРОКОЙ - это диагностика пути (кривая ось,
        # файл выгружен другим запросом, java упала). Обычно текст печатает
        # сам python, но мы исключение перехватили и подменяем его кодом -
        # без этой печати в журнале полосы остается только "код 1"
        if not isinstance(code, int) and code:
            for line in str(code).splitlines():
                print(f"# {line}", file=sys.stderr)
        code = code if isinstance(code, int) else 1
        if code in (0,):
            raise
        # отрицательный код - сигнал (subprocess.returncode = -9), 137 - тот
        # же SIGKILL в терминах шелла; под заявленным лимитом памяти это
        # почти всегда OOM-killer, и класс обязан называть лимит (F23) -
        # ровно та же таблица, что в runner/lib.sh
        sig = -code if code < 0 else (code - 128 if code > 128 else 0)
        limit = mem_limit_label()
        if sig == 9 and limit:
            err_class = f"oom:{limit}"
        else:
            err_class = f"exit:{128 + sig if sig else code}"
        # F02/F04: у остановки с объявленным классом (PathExit) имя причины
        # уезжает в саму строку клетки, а не только в журнал полосы
        err_class = getattr(exc, "error_class", None) or err_class
        print(f"# путь завершился кодом {code} ({err_class})", file=sys.stderr)
        emit(scenario_id, label, wall, None, None,
             extras={"status": "error", "error_class": err_class,
                     "ts": ts0, "ts_end": ts1}, peak_mb="")
        # код возврата сохраняем в шелловой форме: раннер по нему отличает
        # OOM от прочих падений и не переписывает уже дописанную строку
        raise SystemExit(128 + sig if sig else code)
    except Exception as exc:
        wall = time.perf_counter() - t0
        ts1 = _now_ms()
        for line in traceback.format_exc().splitlines():
            print(f"# {line}", file=sys.stderr)
        # строка пишется всегда: wall фактический, остальное пусто
        emit(scenario_id, label, wall, None, None,
             extras={"status": "error", "error_class": type(exc).__name__,
                     "ts": ts0, "ts_end": ts1}, peak_mb="")
        raise SystemExit(1)
    wall = time.perf_counter() - t0
    ts1 = _now_ms()
    ru1 = resource.getrusage(resource.RUSAGE_SELF)
    net1 = _net_counters()
    steal1 = steal_ticks()
    cpu_user = ru1.ru_utime - ru0.ru_utime
    cpu_sys = ru1.ru_stime - ru0.ru_stime
    # F07: пик снимается ПРИ ЖИВОЙ структуре и до служебных проб - иначе в
    # него уехало бы соединение пробы, а не работа пути
    peak_now = peak_rss_mb()
    # F07: структура жива до этой точки; цена освобождения - отдельная колонка
    cleanup_s = release_live()
    # F01: служебные пробы пути идут ЗДЕСЬ - часы уже стоят, счетчики сняты
    post_check_s = _run_post_checks_or_die(
        scenario_id, label, wall, ts0, ts1, cleanup_s)

    if isinstance(result, list):
        # серия исполнений в одном процессе: время, байты, пик и границы
        # каждого исполнения считает сам путь; недостающее - границы и
        # процессная cpu-дельта всего вызова
        # список из ОДНОЙ записи (путь отдал серию одним исполнением, как
        # file_export): окно вызова и есть окно исполнения, и дельты процесса
        # (сеть, steal) принадлежат ему целиком - добиваем их, как в одиночном
        # режиме, если путь не принес свои. При нескольких записях дельты по
        # исполнениям не раскладываются: колонки остаются за путем (_execs
        # снимает их вокруг каждого исполнения сам), пустота честнее общей
        # на серию суммы под меткой одного исполнения
        single = len(result) == 1
        for i, rec in enumerate(result, start=1):
            rec = dict(rec)
            peak = rec.pop("peak_rss_mb", None)
            extras = {k: rec.pop(k) for k in EXTRA_COLS if k in rec}
            extras.setdefault("exec_no", i)
            if single:
                if net0 is not None and net1 is not None:
                    if rec.get("bytes_rx_mb") in (None, ""):
                        rec["bytes_rx_mb"] = (
                            net1["rx_bytes"] - net0["rx_bytes"]) / 2**20
                    if not extras.get("bytes_tx_mb"):
                        extras["bytes_tx_mb"] = (
                            net1["tx_bytes"] - net0["tx_bytes"]) / 2**20
                    if not extras.get("packets_rx"):
                        extras["packets_rx"] = (net1["rx_packets"]
                                                - net0["rx_packets"])
                        extras["packets_tx"] = (net1["tx_packets"]
                                                - net0["tx_packets"])
                if steal0 is not None and steal1 is not None \
                        and not extras.get("steal_ticks"):
                    extras["steal_ticks"] = steal1 - steal0
            if not extras.get("ts"):
                extras["ts"] = ts0
                extras["ts_end"] = ts1
            if not extras.get("cpu_user_s") and extras.get("cpu_user_s") != 0.0:
                extras["cpu_user_s"] = cpu_user
                extras["cpu_sys_s"] = cpu_sys
                extras["cpu_scope"] = "process"
            elif not extras.get("cpu_scope"):
                extras["cpu_scope"] = "exec"
            if i == len(result):
                # проба одна на весь вызов пути и идет ПОСЛЕ последнего
                # исполнения - размазывать ее по всем строкам серии значило
                # бы обещать корпусу проверку на каждом исполнении
                if post_check_s is not None:
                    extras.setdefault("post_check_s", post_check_s)
                # то же с holder: в списочном режиме цену освобождения обычно
                # кладет сам каркас исполнений (_execs) для каждого замера, но
                # путь, отдавший список одной записью (file_export), держит
                # структуру до возврата - тогда ее отпускает обвязка
                if cleanup_s is not None:
                    extras.setdefault("cleanup_s", cleanup_s)
            emit(scenario_id, label, rec["wall_s"], rec.get("ttfb_s"),
                 rec["rows"], rec.get("extra_s"), rec.get("bytes_rx_mb"),
                 extras, peak_mb=peak)
        return

    rows, ttfb = result[0], result[1]
    extra = result[2] if len(result) > 2 else None
    extras = dict(result[3]) if len(result) > 3 else {}
    # одиночная клетка = первое исполнение в новом соединении = cold по
    # спеке (cold = exec_no 1). Пустой exec_no остается только у строк
    # старых серий и у незачетных (error/skipped) - см. CONTRACT
    extras.setdefault("exec_no", 1)
    bytes_rx = None
    if net0 is not None and net1 is not None:
        bytes_rx = (net1["rx_bytes"] - net0["rx_bytes"]) / 2**20
        if not extras.get("bytes_tx_mb"):
            extras["bytes_tx_mb"] = (net1["tx_bytes"] - net0["tx_bytes"]) / 2**20
        if not extras.get("packets_rx"):
            extras["packets_rx"] = net1["rx_packets"] - net0["rx_packets"]
            extras["packets_tx"] = net1["tx_packets"] - net0["tx_packets"]
    if steal0 is not None and steal1 is not None and not extras.get("steal_ticks"):
        extras["steal_ticks"] = steal1 - steal0
    if not extras.get("ts"):
        extras["ts"] = ts0
        extras["ts_end"] = ts1
    if not extras.get("cpu_user_s") and extras.get("cpu_user_s") != 0.0:
        extras["cpu_user_s"] = cpu_user
        extras["cpu_sys_s"] = cpu_sys
        # окно cpu совпадает с окном wall: одно исполнение = один вызов пути
        extras.setdefault("cpu_scope", "exec")
    elif not extras.get("cpu_scope"):
        extras["cpu_scope"] = "exec"
    # rss_settled_mb обвязка НЕ заполняет: после возврата пути структура уже
    # отпущена, и gc.collect мерил бы покой без нее - у колонки было бы два
    # смысла без маркера. Колонку заполняют только клетки Д6 (BENCH_RETAINED=1)
    # изнутри пути, при живой структуре (см. _maybe_retained и CONTRACT).
    if cleanup_s is not None:
        extras.setdefault("cleanup_s", cleanup_s)
    if post_check_s is not None:
        extras.setdefault("post_check_s", post_check_s)
    # peak снят до освобождения структуры и до служебной пробы
    emit(scenario_id, label, wall, ttfb, rows, extra, bytes_rx, extras,
         peak_mb=peak_now)


def run_scenario(scenario_id: str, paths: dict) -> None:
    """Точка входа сценария.

    Без аргументов - оркестратор: гоняет каждый путь отдельным подпроцессом
    (чистый peak RSS). С --path NAME - рабочий режим: исполняет один путь.
    Упавший путь не роняет остальные (нет optional-зависимости, недоступен
    движок) - но строку со status=error оставляет всегда.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", choices=sorted(paths), default=None)
    args = parser.parse_args()

    if args.path is not None:
        # F4: имя пути нужно классу слива и кадру чтения - метка клетки для
        # этого не годится, она произвольная
        set_path_kind(args.path)
        # MEASURE_LABEL позволяет различить варианты одного пути (например,
        # jdbc с разным fetchSize) в колонке path результирующего CSV.
        label = os.environ.get("MEASURE_LABEL", args.path)
        _run_path(scenario_id, label, paths[args.path])
        return

    print(f"# {scenario_id}: пути - {', '.join(sorted(paths))}", file=sys.stderr)
    print(",".join(HEADER), flush=True)
    failed = 0
    # MEASURE_LABEL не наследуем: в оркестраторном режиме одна метка склеила бы
    # строки всех путей сценария в одну группу
    child_env = {k: v for k, v in os.environ.items() if k != "MEASURE_LABEL"}
    for name in sorted(paths):
        proc = subprocess.run(
            [sys.executable, sys.argv[0], "--path", name],
            env=child_env,
        )
        if proc.returncode != 0:
            failed += 1
            print(f"# путь {name} упал (код {proc.returncode}) - "
                  f"пропускаю, см. stderr выше", file=sys.stderr)
    print(f"# результаты дописаны в {results_file()}", file=sys.stderr)
    if failed:
        sys.exit(1)


def _selftest() -> None:
    """Самотест обвязки без баз: фейковые пути, проверка схемы и статусов.
    Вне Linux допустимы пустые линукс-колонки (сеть, steal, rss_settled)."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        os.environ["RESULTS_FILE"] = str(Path(tmp) / "selftest.csv")
        os.environ["BENCH_LANE"] = "selftest"
        os.environ["BENCH_CANVAS"] = "gen"
        os.environ.pop("BENCH_BASELINE", None)

        def fake_series():
            out = []
            for i in (1, 2):
                reset_peak_rss()
                t0 = time.perf_counter()
                data = [tuple(range(100)) for _ in range(2000)]
                out.append({"wall_s": time.perf_counter() - t0,
                            "rows": len(data), "exec_no": i,
                            "peak_rss_mb": peak_rss_mb()})
            return out

        def fake_single():
            return 1000, 0.001, None, {"digest": "cafe01"}

        def fake_error():
            raise RuntimeError("проверка записи строки при падении")

        def fake_oom():
            # HM4: так выглядит путь, чей клиент убит ядром под лимитом
            # (subprocess.returncode = -9 -> SystemExit(-9) в пути)
            raise SystemExit(-9)

        _run_path("selftest", "fake-series", fake_series)
        _run_path("selftest", "fake-single", fake_single)
        try:
            _run_path("selftest", "fake-error", fake_error)
        except SystemExit:
            pass
        os.environ["BENCH_MEM_LIMIT"] = "8G"
        try:
            _run_path("selftest", "fake-oom", fake_oom)
        except SystemExit as exc:
            assert exc.code == 137, f"код возврата OOM-клетки: {exc.code}"
        os.environ.pop("BENCH_MEM_LIMIT", None)
        emit_skipped("fake-skip", "skipped", "cut:selftest",
                     scenario_id="selftest")

        # RPY-01: объявленный класс слива не переписывается фактическим.
        # Оговорка контракта (ch_pg_emu: объявлен rows, физически buffered)
        # проходит молча, а любое другое расхождение останавливает клетку
        set_path_kind("ch_pg_emu")
        os.environ["BENCH_MODE"] = "drain"
        emit("selftest", "fake-emu", 0.5, 0.1, 10,
             extras={"client_dtype": "drain:buffered", "drain_class": "rows"})
        set_path_kind("pg_psycopg")
        try:
            emit("selftest", "fake-mismatch", 0.5, 0.1, 10,
                 extras={"client_dtype": "drain:buffered",
                         "drain_class": "rows"})
        except SystemExit as exc:
            assert exc.code == 3, f"код остановки клетки: {exc.code}"
        else:
            raise AssertionError(
                "расхождение класса слива не остановило клетку")
        os.environ.pop("BENCH_MODE", None)
        set_path_kind("")

        with open(os.environ["RESULTS_FILE"], newline="") as f:
            rows = list(csv.reader(f))

    header, body = rows[0], rows[1:]
    assert header == HEADER, f"заголовок разошелся со схемой: {header}"
    assert len(body) == 8, f"ожидалось 8 строк, получено {len(body)}"
    for r in body:
        assert len(r) == len(HEADER), \
            f"строка из {len(r)} полей вместо {len(HEADER)}: {r}"
    idx = {c: i for i, c in enumerate(HEADER)}
    statuses = [r[idx["status"]] for r in body]
    assert statuses == ["ok", "ok", "ok", "error", "error", "skipped",
                        "ok", "ok"], statuses
    # обе клетки сохранили ОБЪЯВЛЕННЫЙ класс: и оговорка, и расхождение
    assert body[6][idx["drain_class"]] == "rows", body[6][idx["drain_class"]]
    assert body[7][idx["drain_class"]] == "rows", body[7][idx["drain_class"]]
    assert body[3][idx["error_class"]] == "RuntimeError"
    # строка клетки, убитой под лимитом, обязана быть в сырье и называть лимит
    assert body[4][idx["error_class"]] == "oom:8G", body[4][idx["error_class"]]
    assert body[0][idx["exec_no"]] == "1" and body[1][idx["exec_no"]] == "2"
    assert body[2][idx["ts"]] and body[2][idx["ts_end"]], "ts не заполнен"
    assert body[2][idx["cpu_user_s"]] != "", "cpu_user_s не заполнен"
    assert body[2][idx["cpu_scope"]] == "exec"
    assert body[0][idx["cpu_scope"]] == "process", \
        "серия без cpu от пути обязана получить процессную дельту"
    assert body[2][idx["lane"]] == "selftest" and body[2][idx["canvas"]] == "gen"
    assert body[5][idx["error_class"]] == "cut:selftest"
    # rss_settled_mb сюда не входит: колонку заполняют только клетки Д6
    linux_only = ["bytes_rx_mb", "steal_ticks"]
    empty = [c for c in linux_only if body[2][idx[c]] == ""]
    print(f"# selftest ok: {len(body)} строк, {len(HEADER)} колонок; "
          f"пустые линукс-колонки: {empty or 'нет'}", file=sys.stderr)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        _selftest()
    else:
        print("# _measure.py - модуль обвязки, импортируется сценариями; "
              "самотест: --selftest", file=sys.stderr)
