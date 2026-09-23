#!/usr/bin/env python3
"""Build the public one-table benchmark result from an accepted result package."""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import math
import shutil
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

from openpyxl import Workbook
from openpyxl.cell import WriteOnlyCell
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

TITLE = "Измерения путей доставки данных из PostgreSQL и ClickHouse"
ALLOWED = {"allowed", "with_caveat"}
NAVY, BLUE, WHITE, INK = "26384A", "D9EEF7", "FFFFFF", "202B35"
SCRIPT_FILES = ["build.py", "verify.py", "requirements.txt"]


def field(name, kind, unit, description, source, grain, aggregation, role):
    return {
        "field": name, "type": kind, "unit": unit, "description": description,
        "source": source, "grain": grain, "default_aggregation": aggregation,
        "datalens_role": role,
    }


FIELDS = [
    field("record_id", "string", "", "Уникальный идентификатор строки.", "observation_id или outcome:cell_id", "record", "none", "dimension"),
    field("record_type", "string", "", "execution — фактическое исполнение; declared_outcome — исход без строки измерения.", "derived", "record", "none", "dimension"),
    field("benchmark_session_id", "string", "", "Идентификатор измерительной сессии.", "run_id", "record", "none", "dimension"),
    field("cell_id", "string", "", "Идентификатор клетки бенчмарка.", "cell_id", "cell", "none", "dimension"),
    field("attempt_id", "string", "", "Идентификатор попытки блока.", "attempt_id", "execution", "none", "dimension"),
    field("block_attempt_id", "string", "", "Идентификатор попытки с областью блока.", "block_attempt_id", "execution", "none", "dimension"),
    field("execution_no", "int", "номер", "Порядковый номер исполнения в исходном файле.", "exec_no", "execution", "none", "dimension"),
    field("round", "int", "раунд", "Номер раунда; прогрев всегда имеет значение 0.", "round", "execution", "none", "dimension"),
    field("round_role", "string", "", "Роль раунда: primary, warmup, redo или service.", "round_role", "execution", "none", "dimension"),
    field("is_warmup", "bool", "", "Признак прогрева.", "is_warmup", "execution", "none", "dimension"),
    field("is_selected_attempt", "bool", "", "Исполнение относится к выбранной попытке клетки.", "selected_attempt", "execution", "none", "dimension"),
    field("include_in_default_stats", "bool", "", "Строка участвует в принятой основной статистике.", "include_in_default_stats", "execution", "none", "filter"),
    field("is_cell_anchor", "bool", "", "Ровно одна строка на клетку для графиков с полями cell_*.", "derived", "record", "none", "filter"),
    field("measurement_role", "string", "", "Роль измерения: путь, диагностика сервера, удержание памяти или мост.", "measurement_role", "record", "none", "dimension"),
    field("measurement_status", "string", "", "Статус конкретного исполнения или объявленного исхода.", "status/cell_validity", "record", "none", "dimension"),
    field("validity_status", "string", "", "Статус пригодности исходного наблюдения.", "validity_status", "execution", "none", "dimension"),
    field("exclusion_reason", "string", "", "Причина исключения исполнения из основной статистики.", "exclusion_reason", "execution", "none", "dimension"),
    field("cell_validity", "string", "", "Итоговый статус клетки.", "01-results.cell_validity", "cell", "none", "dimension"),
    field("failure_stage", "string", "", "Стадия отказа клетки.", "01-results.failure_stage", "cell", "none", "dimension"),
    field("failure_reason", "string", "", "Причина отказа клетки.", "01-results.failure_reason", "cell", "none", "dimension"),
    field("memory_evidence_level", "string", "", "Уровень свидетельства ограничения памяти.", "01-results.memory_evidence_level", "cell", "none", "dimension"),
    field("engine", "string", "", "Код движка: pg или ch.", "lane/engine", "cell", "none", "dimension"),
    field("engine_name", "string", "", "Название СУБД.", "01-results.engine_label", "cell", "none", "dimension"),
    field("path_id", "string", "", "Канонический идентификатор пути доставки.", "01-results.path_id", "cell", "none", "dimension"),
    field("path_name", "string", "", "Человекочитаемое название пути.", "01-results.path_label", "cell", "none", "dimension"),
    field("library", "string", "", "Клиентская библиотека.", "01-results.library", "cell", "none", "dimension"),
    field("api", "string", "", "Фактически вызванный API.", "01-results.api", "cell", "none", "dimension"),
    field("transport", "string", "", "Транспорт или протокол.", "01-results.transport", "cell", "none", "dimension"),
    field("wire_format", "string", "", "Формат данных на проводе.", "execution/01-results.wire_format", "record", "none", "dimension"),
    field("codec", "string", "", "Кодек сжатия.", "execution/01-results.codec", "record", "none", "dimension"),
    field("codec_level", "string", "", "Уровень кодека.", "execution/01-results.codec_level", "record", "none", "dimension"),
    field("target", "string", "", "Конечная структура результата.", "execution/01-results.target", "record", "none", "dimension"),
    field("materialization", "string", "", "Уровень материализации результата.", "execution/01-results.mat_level", "record", "none", "dimension"),
    field("type_restoration", "string", "", "Режим восстановления типов.", "01-results.type_restoration", "cell", "none", "dimension"),
    field("result_schema_id", "string", "", "Идентификатор ожидаемой схемы результата.", "01-results.result_schema_id", "cell", "none", "dimension"),
    field("logical_schema_id", "string", "", "Идентификатор логической схемы.", "01-results.logical_schema_id", "cell", "none", "dimension"),
    field("observed_schema_id", "string", "", "Идентификатор фактически наблюдаемой схемы.", "execution/01-results.observed_schema_id", "record", "none", "dimension"),
    field("dataset_id", "string", "", "Идентификатор набора данных.", "01-results.dataset_id", "cell", "none", "dimension"),
    field("dataset_snapshot_id", "string", "", "Версия снимка данных.", "01-results.dataset_snapshot_id", "cell", "none", "dimension"),
    field("table_id", "string", "", "Логическое имя таблицы теста.", "01-results.table", "cell", "none", "dimension"),
    field("width", "string", "", "Класс ширины набора.", "01-results.width", "cell", "none", "dimension"),
    field("rows_expected", "int", "строк", "Ожидаемое число строк.", "01-results.rows_expected", "cell", "median", "measure"),
    field("rows_observed_cell", "int", "строк", "Проверенное число строк результата клетки.", "01-results.rows_observed", "cell", "max", "measure"),
    field("question_id", "string", "", "Вопрос исследовательской сетки.", "01-results.question_id", "cell", "none", "dimension"),
    field("comparison_id", "string", "", "Идентификатор объявленного сравнения.", "01-results.comparison_id", "cell", "none", "dimension"),
    field("rating_group_id", "string", "", "Публичная группа расчета кратности.", "derived from rating dimensions", "cell", "none", "dimension"),
    field("rating_baseline_cell_id", "string", "", "Клетка-знаменатель группы рейтинга.", "01-results.rating_baseline_cell_id", "cell", "none", "dimension"),
    field("rating_scope", "string", "", "Область рейтинга.", "01-results.rating_scope", "cell", "none", "dimension"),
    field("condition_id", "string", "", "Идентификатор условий измерения.", "condition_id", "record", "none", "dimension"),
    field("timer_contract", "string", "", "Граница измеряемого времени.", "timer_contract", "record", "none", "dimension"),
    field("connect_included", "bool", "", "Подключение входит в контракт секундомера.", "01-results.connect_included", "cell", "none", "dimension"),
    field("connection_close_included", "bool", "", "Закрытие подключения входит в контракт секундомера.", "01-results.connection_close_included", "cell", "none", "dimension"),
    field("result_cleanup_included", "bool", "", "Очистка результата входит в контракт секундомера.", "01-results.result_cleanup_included", "cell", "none", "dimension"),
    field("cpu_scope", "string", "", "Область измерения CPU.", "execution/01-results.cpu_scope", "record", "none", "dimension"),
    field("memory_scope", "string", "", "Область измерения памяти.", "01-results.memory_scope", "cell", "none", "dimension"),
    field("harness_version", "string", "", "Версия измерительного харнесса.", "06-conditions.harness_tag_measured", "condition", "none", "dimension"),
    field("engine_version", "string", "", "Заявленная версия движка.", "06-conditions.engine_version_declared", "condition", "none", "dimension"),
    field("client_cpu_model", "string", "", "Модель процессора клиента.", "06-conditions.client_cpu_model", "condition", "none", "dimension"),
    field("client_cpu_count", "int", "ядер", "Число доступных логических CPU клиента.", "06-conditions.client_nproc", "condition", "max", "measure"),
    field("client_memory", "string", "", "Объем памяти клиента в исходной записи условий.", "06-conditions.client_mem_total", "condition", "none", "dimension"),
    field("client_python", "string", "", "Версия Python клиента.", "06-conditions.client_python", "condition", "none", "dimension"),
    field("client_kernel", "string", "", "Версия ядра клиента.", "06-conditions.client_kernel_release", "condition", "none", "dimension"),
    field("network_profile", "string", "", "Профиль сети.", "06-conditions.net_profile", "condition", "none", "dimension"),
    field("library_versions", "string", "", "Версии клиентских библиотек в JSON.", "06-conditions.library_versions_json", "condition", "none", "dimension"),
    field("polars_max_threads", "int", "потоков", "Ограничение потоков Polars.", "06-conditions.polars_max_threads", "condition", "max", "measure"),
    field("environment_caveat", "string", "", "Оговорка об условиях измерения.", "06-conditions.environment_caveat", "condition", "none", "dimension"),
    field("started_at_utc", "timestamp", "UTC", "Начало исполнения.", "02-executions.ts_start_utc", "execution", "none", "dimension"),
    field("finished_at_utc", "timestamp", "UTC", "Конец исполнения.", "02-executions.ts_end_utc", "execution", "none", "dimension"),
    field("wall_s", "float", "s", "Время исполнения по контракту секундомера.", "02-executions.wall_s", "execution", "median", "measure"),
    field("connect_s", "float", "s", "Время подключения.", "02-executions.connect_s", "execution", "median", "measure"),
    field("ttfb_s", "float", "s", "Время до первого байта в единицах источника.", "02-executions.ttfb_s", "execution", "median", "measure"),
    field("cleanup_s", "float", "s", "Время очистки результата.", "02-executions.cleanup_s", "execution", "median", "measure"),
    field("post_check_s", "float", "s", "Время проверки после измерения.", "02-executions.post_check_s", "execution", "median", "measure"),
    field("extra_s", "float", "s", "Дополнительная стадия измерения.", "02-executions.extra_s", "execution", "median", "measure"),
    field("server_time_s", "float", "s", "Серверное время диагностической пробы.", "02-executions.server_time_s", "execution", "median", "measure"),
    field("serialize_time_s", "float", "s", "Время сериализации серверной пробы.", "02-executions.serialize_time_s", "execution", "median", "measure"),
    field("peak_rss_mib", "float", "MiB", "Пиковая память процесса исполнения.", "02-executions.peak_rss_mib", "execution", "median", "measure"),
    field("rss_settled_mib", "float", "MiB", "Память после стабилизации.", "02-executions.rss_settled_mib", "execution", "median", "measure"),
    field("retained_mib", "float", "MiB", "Оценка удержанной памяти.", "02-executions.retained_mib", "execution", "median", "measure"),
    field("cpu_user_s", "float", "s", "Пользовательское время CPU.", "02-executions.cpu_user_s", "execution", "median", "measure"),
    field("cpu_sys_s", "float", "s", "Системное время CPU.", "02-executions.cpu_sys_s", "execution", "median", "measure"),
    field("cpu_total_s", "float", "s", "Суммарное время CPU.", "02-executions.cpu_total_s", "execution", "median", "measure"),
    field("cpu_cores_equivalent", "float", "cores", "Отношение cpu_total_s к wall_s.", "derived", "execution", "median", "measure"),
    field("interface_rx_mib", "float", "MiB", "Принято по счетчику интерфейса.", "02-executions.interface_rx_mib", "execution", "median", "measure"),
    field("interface_tx_mib", "float", "MiB", "Передано по счетчику интерфейса.", "02-executions.interface_tx_mib", "execution", "median", "measure"),
    field("packets_rx", "int", "packets", "Принято пакетов.", "02-executions.packets_rx", "execution", "median", "measure"),
    field("packets_tx", "int", "packets", "Передано пакетов.", "02-executions.packets_tx", "execution", "median", "measure"),
    field("payload_rx_estimated_mib", "float", "MiB", "Расчетная полезная нагрузка.", "02-executions.payload_rx_estimated_mib", "execution", "median", "measure"),
    field("rows", "int", "строк", "Число строк конкретного исполнения.", "02-executions.rows", "execution", "median", "measure"),
    field("rows_per_s", "float", "rows/s", "Строк в секунду для конкретного исполнения.", "derived: rows / wall_s", "execution", "median", "measure"),
    field("payload_mib_per_s", "float", "MiB/s", "Полезная нагрузка в секунду для конкретного исполнения.", "derived: payload / wall_s", "execution", "median", "measure"),
    field("rss_to_payload_ratio", "float", "ratio", "Пиковая память к расчетной полезной нагрузке.", "derived", "execution", "median", "measure"),
    field("server_output_kib", "float", "KiB", "Объем выхода серверной пробы.", "02-executions.server_output_kib", "execution", "median", "measure"),
    field("steal_ticks", "float", "ticks", "Steal ticks за окно исполнения.", "02-executions.steal_ticks", "execution", "median", "measure"),
    field("result_digest", "string", "", "Digest результата исполнения.", "02-executions.digest", "execution", "none", "dimension"),
    field("cell_primary_rounds", "int", "раундов", "Число принятых зачетных раундов клетки.", "01-results.n_primary_used", "cell", "max", "measure"),
    field("cell_wall_median_s", "float", "s", "Принятая медиана времени клетки.", "01-results.wall_median_s", "cell", "max", "measure"),
    field("cell_wall_min_s", "float", "s", "Минимум времени клетки.", "01-results.wall_min_s", "cell", "max", "measure"),
    field("cell_wall_max_s", "float", "s", "Максимум времени клетки.", "01-results.wall_max_s", "cell", "max", "measure"),
    field("cell_wall_spread_ratio", "float", "ratio", "Отношение максимума ко минимуму времени.", "01-results.wall_spread_ratio", "cell", "max", "measure"),
    field("cell_peak_rss_median_mib", "float", "MiB", "Медиана пика памяти клетки.", "01-results.peak_rss_median_mib", "cell", "max", "measure"),
    field("cell_peak_rss_max_mib", "float", "MiB", "Максимальный пик памяти клетки.", "01-results.peak_rss_max_mib", "cell", "max", "measure"),
    field("cell_cpu_total_median_s", "float", "s", "Медиана суммарного CPU клетки.", "01-results.cpu_total_median_s", "cell", "max", "measure"),
    field("cell_cpu_cores_equivalent", "float", "cores", "Принятый эквивалент загрузки ядер клетки.", "01-results.cpu_cores_equivalent", "cell", "max", "measure"),
    field("cell_interface_rx_median_mib", "float", "MiB", "Медиана счетчика приема клетки.", "01-results.interface_rx_median_mib", "cell", "max", "measure"),
    field("cell_payload_rx_estimated_median_mib", "float", "MiB", "Медиана расчетной полезной нагрузки клетки.", "01-results.payload_rx_estimated_median_mib", "cell", "max", "measure"),
    field("cell_rows_per_s", "float", "rows/s", "Принятая производительность клетки.", "01-results.rows_per_s", "cell", "max", "measure"),
    field("cell_payload_mib_per_s", "float", "MiB/s", "Принятая полезная нагрузка в секунду клетки.", "01-results.payload_mib_per_s", "cell", "max", "measure"),
    field("cell_rss_to_payload_ratio", "float", "ratio", "Принятое отношение памяти к полезной нагрузке.", "01-results.rss_to_payload_ratio", "cell", "max", "measure"),
    field("cell_rating_multiplier", "float", "ratio", "Кратность времени к базе рейтинговой группы.", "derived from rating baseline", "cell", "max", "measure"),
    field("time_quality", "string", "", "Пригодность времени клетки.", "01-results.time_publishability", "cell", "none", "dimension"),
    field("memory_quality", "string", "", "Пригодность памяти клетки.", "01-results.memory_publishability", "cell", "none", "dimension"),
    field("cpu_quality", "string", "", "Пригодность CPU клетки.", "01-results.cpu_publishability", "cell", "none", "dimension"),
    field("payload_quality", "string", "", "Пригодность расчетной полезной нагрузки.", "01-results.payload_publishability", "cell", "none", "dimension"),
    field("interface_counter_quality", "string", "", "Пригодность счетчика сетевого интерфейса.", "04-metric-quality.interface_bytes_rx", "cell", "none", "dimension"),
    field("rows_quality", "string", "", "Пригодность числа строк.", "04-metric-quality.rows", "cell", "none", "dimension"),
    field("server_time_quality", "string", "", "Пригодность серверного времени.", "04-metric-quality.server_time", "cell", "none", "dimension"),
    field("serialize_time_quality", "string", "", "Пригодность времени сериализации.", "04-metric-quality.serialize_time", "cell", "none", "dimension"),
    field("server_output_quality", "string", "", "Пригодность объема серверного выхода.", "04-metric-quality.server_output", "cell", "none", "dimension"),
    field("quality_summary", "string", "", "Краткое описание качества метрик клетки.", "01-results.quality_summary", "cell", "none", "dimension"),
    field("caveat", "string", "", "Оговорка для интерпретации клетки.", "01-results.caveat", "cell", "none", "dimension"),
    field("time_usable", "bool", "", "Время разрешено показывать с учетом оговорки.", "derived", "cell", "none", "filter"),
    field("memory_usable", "bool", "", "Память разрешено показывать с учетом оговорки.", "derived", "cell", "none", "filter"),
    field("cpu_usable", "bool", "", "CPU разрешено показывать с учетом оговорки.", "derived", "cell", "none", "filter"),
    field("payload_usable", "bool", "", "Полезная нагрузка разрешена к показу.", "derived", "cell", "none", "filter"),
    field("show_time_by_default", "bool", "", "Готовый фильтр основных графиков времени.", "include_in_default_stats AND time_usable", "execution", "none", "filter"),
    field("show_memory_by_default", "bool", "", "Готовый фильтр основных графиков памяти.", "include_in_default_stats AND memory_usable", "execution", "none", "filter"),
    field("show_cpu_by_default", "bool", "", "Готовый фильтр основных графиков CPU.", "include_in_default_stats AND cpu_usable", "execution", "none", "filter"),
    field("show_payload_by_default", "bool", "", "Готовый фильтр основных графиков полезной нагрузки.", "include_in_default_stats AND payload_usable", "execution", "none", "filter"),
]

HEADERS = [x["field"] for x in FIELDS]
META = {x["field"]: x for x in FIELDS}


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def read_csv(path: Path):
    with path.open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream))


def boolean(value) -> bool:
    return str(value).lower() == "true"


def number(value, integer=False):
    if value in (None, ""):
        return None
    return int(value) if integer else float(value)


def divide(a, b):
    return a / b if a is not None and b not in (None, 0) else None


def text(value):
    return "" if value is None else str(value)


def public_rating_group(value):
    parts = value.split("/")
    return "/".join(parts[:4] + parts[5:]) if len(parts) >= 7 and parts[0] == "rating" else value


def public_cell_values(cell, quality):
    if not cell:
        return {}
    q = lambda metric: quality.get((cell["cell_id"], metric), "unknown")
    return {
        "cell_id": cell["cell_id"], "cell_validity": cell["cell_validity"],
        "failure_stage": cell["failure_stage"], "failure_reason": cell["failure_reason"],
        "memory_evidence_level": cell["memory_evidence_level"], "engine": cell["engine"],
        "engine_name": cell["engine_label"], "path_id": cell["path_id"], "path_name": cell["path_label"],
        "library": cell["library"], "api": cell["api"], "transport": cell["transport"],
        "wire_format": cell["wire_format"], "codec": cell["codec"], "codec_level": cell["codec_level"],
        "target": cell["target"], "materialization": cell["mat_level"], "type_restoration": cell["type_restoration"],
        "result_schema_id": cell["result_schema_id"], "logical_schema_id": cell["logical_schema_id"],
        "observed_schema_id": cell["observed_schema_id"], "dataset_id": cell["dataset_id"],
        "dataset_snapshot_id": cell["dataset_snapshot_id"], "table_id": cell["table"], "width": cell["width"],
        "rows_expected": number(cell["rows_expected"], True), "rows_observed_cell": number(cell["rows_observed"], True),
        "question_id": cell["question_id"], "comparison_id": cell["comparison_id"],
        "rating_group_id": public_rating_group(cell["rating_group_id"]),
        "rating_baseline_cell_id": cell["rating_baseline_cell_id"],
        "rating_scope": cell["rating_scope"], "condition_id": cell["condition_id"],
        "timer_contract": cell["timer_contract"], "connect_included": boolean(cell["connect_included"]),
        "connection_close_included": boolean(cell["connection_close_included"]),
        "result_cleanup_included": boolean(cell["result_cleanup_included"]), "cpu_scope": cell["cpu_scope"],
        "memory_scope": cell["memory_scope"], "cell_primary_rounds": number(cell["n_primary_used"], True),
        "cell_wall_median_s": number(cell["wall_median_s"]), "cell_wall_min_s": number(cell["wall_min_s"]),
        "cell_wall_max_s": number(cell["wall_max_s"]), "cell_wall_spread_ratio": number(cell["wall_spread_ratio"]),
        "cell_peak_rss_median_mib": number(cell["peak_rss_median_mib"]),
        "cell_peak_rss_max_mib": number(cell["peak_rss_max_mib"]),
        "cell_cpu_total_median_s": number(cell["cpu_total_median_s"]),
        "cell_cpu_cores_equivalent": number(cell["cpu_cores_equivalent"]),
        "cell_interface_rx_median_mib": number(cell["interface_rx_median_mib"]),
        "cell_payload_rx_estimated_median_mib": number(cell["payload_rx_estimated_median_mib"]),
        "cell_rows_per_s": number(cell["rows_per_s"]), "cell_payload_mib_per_s": number(cell["payload_mib_per_s"]),
        "cell_rss_to_payload_ratio": number(cell["rss_to_payload_ratio"]),
        "time_quality": cell["time_publishability"], "memory_quality": cell["memory_publishability"],
        "cpu_quality": cell["cpu_publishability"], "payload_quality": cell["payload_publishability"],
        "interface_counter_quality": q("interface_bytes_rx"), "rows_quality": q("rows"),
        "server_time_quality": q("server_time"), "serialize_time_quality": q("serialize_time"),
        "server_output_quality": q("server_output"), "quality_summary": cell["quality_summary"], "caveat": cell["caveat"],
        "time_usable": cell["time_publishability"] in ALLOWED,
        "memory_usable": cell["memory_publishability"] in ALLOWED,
        "cpu_usable": cell["cpu_publishability"] in ALLOWED,
        "payload_usable": cell["payload_publishability"] in ALLOWED,
    }


def condition_values(condition):
    if not condition:
        return {}
    return {
        "harness_version": condition["harness_tag_measured"], "engine_version": condition["engine_version_declared"],
        "client_cpu_model": condition["client_cpu_model"], "client_cpu_count": number(condition["client_nproc"], True),
        "client_memory": condition["client_mem_total"], "client_python": condition["client_python"],
        "client_kernel": condition["client_kernel_release"], "network_profile": condition["net_profile"],
        "library_versions": condition["library_versions_json"],
        "polars_max_threads": number(condition["polars_max_threads"], True),
        "environment_caveat": condition["environment_caveat"],
    }


def execution_values(row):
    wall = number(row["wall_s"]); cpu = number(row["cpu_total_s"]); payload = number(row["payload_rx_estimated_mib"])
    rss = number(row["peak_rss_mib"]); rows = number(row["rows"], True)
    return {
        "record_id": row["observation_id"], "record_type": "execution", "benchmark_session_id": row["run_id"],
        "cell_id": row["cell_id"], "attempt_id": row["attempt_id"], "block_attempt_id": row["block_attempt_id"],
        "execution_no": number(row["exec_no"], True), "round": number(row["round"], True), "round_role": row["round_role"],
        "is_warmup": boolean(row["is_warmup"]), "is_selected_attempt": boolean(row["selected_attempt"]),
        "include_in_default_stats": boolean(row["include_in_default_stats"]), "measurement_role": row["measurement_role"],
        "measurement_status": row["status"], "validity_status": row["validity_status"], "exclusion_reason": row["exclusion_reason"],
        "engine": row["lane"], "condition_id": row["condition_id"], "timer_contract": row["timer_contract"],
        "cpu_scope": row["cpu_scope"], "started_at_utc": row["ts_start_utc"], "finished_at_utc": row["ts_end_utc"],
        "wire_format": row["wire_format"], "codec": row["codec"], "codec_level": row["codec_level"],
        "target": row["target"], "materialization": row["mat_level"], "observed_schema_id": row["observed_schema_id"],
        "wall_s": wall, "connect_s": number(row["connect_s"]), "ttfb_s": number(row["ttfb_s"]),
        "cleanup_s": number(row["cleanup_s"]), "post_check_s": number(row["post_check_s"]), "extra_s": number(row["extra_s"]),
        "server_time_s": number(row["server_time_s"]), "serialize_time_s": number(row["serialize_time_s"]),
        "peak_rss_mib": rss, "rss_settled_mib": number(row["rss_settled_mib"]), "retained_mib": number(row["retained_mib"]),
        "cpu_user_s": number(row["cpu_user_s"]), "cpu_sys_s": number(row["cpu_sys_s"]), "cpu_total_s": cpu,
        "cpu_cores_equivalent": divide(cpu, wall), "interface_rx_mib": number(row["interface_rx_mib"]),
        "interface_tx_mib": number(row["interface_tx_mib"]), "packets_rx": number(row["packets_rx"], True),
        "packets_tx": number(row["packets_tx"], True), "payload_rx_estimated_mib": payload, "rows": rows,
        "rows_per_s": divide(rows, wall), "payload_mib_per_s": divide(payload, wall),
        "rss_to_payload_ratio": divide(rss, payload), "server_output_kib": number(row["server_output_kib"]),
        "steal_ticks": number(row["steal_ticks"]), "result_digest": row["digest"],
    }


def clean_record(values):
    result = {}
    for name in HEADERS:
        value = values.get(name)
        result[name] = "" if value is None else value
    return result


def build_rows(source: Path):
    cells = read_csv(source / "01-results.csv")
    executions = read_csv(source / "02-executions.csv")
    quality_rows = read_csv(source / "04-metric-quality.csv")
    conditions = read_csv(source / "06-conditions.csv")
    cell_index = {row["cell_id"]: row for row in cells}
    condition_index = {row["condition_id"]: row for row in conditions}
    quality = {(row["entity_id"], row["metric_id"]): row["publishability"] for row in quality_rows if row["entity_type"] == "cell"}
    by_cell = defaultdict(list)
    for row in executions:
        if row["cell_id"]:
            by_cell[row["cell_id"]].append(row)
    anchors = {}
    for cell_id, rows in by_cell.items():
        anchors[cell_id] = min(rows, key=lambda row: (
            0 if boolean(row["include_in_default_stats"]) else 1,
            0 if row["round_role"] == "primary" else 1,
            number(row["round"], True) or 0,
            row["observation_id"],
        ))["observation_id"]
    baseline_times = {row["cell_id"]: number(row["wall_median_s"]) for row in cells}
    output = []
    for raw in executions:
        cell = cell_index.get(raw["cell_id"])
        values = execution_values(raw)
        values.update(public_cell_values(cell, quality))
        values.update(condition_values(condition_index.get(values.get("condition_id", ""))))
        values["is_cell_anchor"] = bool(cell and anchors.get(cell["cell_id"]) == raw["observation_id"])
        if cell:
            values["cell_rating_multiplier"] = divide(number(cell["wall_median_s"]), baseline_times.get(cell["rating_baseline_cell_id"]))
        include = values["include_in_default_stats"]
        values["show_time_by_default"] = include and values.get("time_usable", False)
        values["show_memory_by_default"] = include and values.get("memory_usable", False)
        values["show_cpu_by_default"] = include and values.get("cpu_usable", False)
        values["show_payload_by_default"] = include and values.get("payload_usable", False)
        output.append(clean_record(values))
    for cell in cells:
        if cell["cell_id"] in by_cell:
            continue
        values = {
            "record_id": "outcome:" + cell["cell_id"], "record_type": "declared_outcome",
            "benchmark_session_id": cell["run_id"], "cell_id": cell["cell_id"], "is_warmup": False,
            "is_selected_attempt": False, "include_in_default_stats": False, "is_cell_anchor": True,
            "measurement_role": cell["measurement_role"], "measurement_status": cell["cell_validity"],
            "condition_id": cell["condition_id"],
        }
        values.update(public_cell_values(cell, quality))
        values.update(condition_values(condition_index.get(cell["condition_id"])))
        values["cell_rating_multiplier"] = divide(number(cell["wall_median_s"]), baseline_times.get(cell["rating_baseline_cell_id"]))
        values.update({"show_time_by_default": False, "show_memory_by_default": False, "show_cpu_by_default": False, "show_payload_by_default": False})
        output.append(clean_record(values))
    output.sort(key=lambda row: (row["engine"], row["cell_id"], row["record_type"], str(row["round"]), row["record_id"]))
    return output, len(cells), len(executions), len(anchors)


def validate_source(source: Path):
    manifest = json.loads((source / "manifest.json").read_text(encoding="utf-8"))
    acceptance = json.loads((source / "acceptance.json").read_text(encoding="utf-8"))
    if acceptance.get("summary", {}).get("failed") != 0:
        raise ValueError("Source package has failed acceptance checks")
    entries = {item["file"]: item for item in manifest["included_tables"]}
    required = ["01-results.csv", "02-executions.csv", "04-metric-quality.csv", "06-conditions.csv"]
    for name in required:
        item = entries.get(name)
        if not item or sha(source / name) != item["sha256"]:
            raise ValueError(f"Source hash mismatch: {name}")
    return {name: sha(source / name) for name in required}, sha(source / "manifest.json")


def write_csv(path: Path, headers, rows):
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=headers, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: ("true" if value is True else "false" if value is False else value) for key, value in row.items()})


def xlsx_value(value, kind):
    if value == "":
        return None
    if kind == "bool":
        return bool(value)
    if kind == "int":
        return int(value)
    if kind == "float":
        return float(value)
    if kind == "timestamp":
        return dt.datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(dt.timezone.utc).replace(tzinfo=None)
    return "'" + str(value) if str(value).startswith("=") else str(value)


def cell(ws, value, font, fill=None, alignment=None, number_format=None):
    result = WriteOnlyCell(ws, value=value); result.font = font
    if fill: result.fill = fill
    if alignment: result.alignment = alignment
    if number_format: result.number_format = number_format
    return result


def build_xlsx(path: Path, rows):
    workbook = Workbook(write_only=True)
    workbook.properties.title = TITLE
    info = workbook.create_sheet("Описание"); info.sheet_view.showGridLines = False
    info.column_dimensions["A"].width = 24; info.column_dimensions["B"].width = 100
    title_font = Font(name="Arial", size=16, bold=True, color=WHITE); body_font = Font(name="Arial", size=10, color=INK)
    header_font = Font(name="Arial", size=10, bold=True, color=WHITE); label_font = Font(name="Arial", size=10, bold=True, color=NAVY)
    navy = PatternFill("solid", fgColor=NAVY); pale = PatternFill("solid", fgColor="EEF5F8")
    wrap = Alignment(vertical="top", wrap_text=True)
    info.append([cell(info, "Результаты", title_font, navy, wrap), cell(info, TITLE, title_font, navy, wrap)])
    notes = [
        ("Зерно", "Одна строка — одно исполнение. Для клетки без строки измерения добавлен declared_outcome."),
        ("Основной ряд", "include_in_default_stats=true оставляет только принятые зачетные исполнения."),
        ("Прогрев", "round=0 и is_warmup=true. По умолчанию не входит в основные показатели."),
        ("Отказы", "declared_outcome сохраняет клетки, для которых строка измерения не появилась."),
        ("Метрики раунда", "wall_s, память, CPU, передача, число строк и производные относятся к одному исполнению."),
        ("Агрегаты", "Поля cell_* повторяются на строках клетки. Для них используйте is_cell_anchor=true."),
        ("Якорь", "is_cell_anchor=true оставляет ровно одну строку каждой клетки."),
        ("Время", "Основной график использует show_time_by_default=true."),
        ("Память", "Основной график использует show_memory_by_default=true."),
        ("CPU", "Основной график использует show_cpu_by_default=true."),
        ("Передача", "Счетчик интерфейса и расчетная полезная нагрузка имеют разные поля и признаки качества."),
        ("Качество", "Основные графики используют готовые show_*_by_default. Значение with_caveat показывается вместе с caveat."),
        ("Словарь", "Тип, единица, зерно и рекомендуемая агрегация каждого поля находятся на отдельном листе."),
    ]
    for label, value in notes:
        info.append([cell(info, label, label_font, pale, wrap), cell(info, value, body_font, alignment=wrap)])
    data = workbook.create_sheet("Измерения"); data.freeze_panes = "B2"; data.sheet_view.showGridLines = False
    data.append([cell(data, name, header_font, navy, Alignment(horizontal="center", vertical="center", wrap_text=True)) for name in HEADERS])
    widths = [max(12, min(45, len(name) + 2)) for name in HEADERS]
    for row_no, row in enumerate(rows, start=2):
        values = []
        for index, name in enumerate(HEADERS):
            meta = META[name]; value = xlsx_value(row[name], meta["type"])
            fmt = None
            if meta["type"] == "timestamp": fmt = 'yyyy-mm-dd"T"hh:mm:ss.000"Z"'
            elif meta["type"] == "int": fmt = "#,##0"
            elif meta["type"] == "float": fmt = "0.000######"
            fill = PatternFill("solid", fgColor=BLUE) if row_no % 2 == 0 else None
            align = Alignment(horizontal="right", vertical="center") if meta["type"] in {"int", "float", "timestamp"} else Alignment(horizontal="left", vertical="center", wrap_text=True)
            values.append(cell(data, value, body_font, fill, align, fmt))
            if row_no <= 101 and value is not None: widths[index] = max(widths[index], min(45, len(str(value)) + 2))
        data.append(values)
    data.auto_filter.ref = f"A1:{get_column_letter(len(HEADERS))}{len(rows)+1}"
    for index, width in enumerate(widths, 1): data.column_dimensions[get_column_letter(index)].width = width
    dictionary = workbook.create_sheet("Словарь"); dictionary.freeze_panes = "A2"; dictionary.sheet_view.showGridLines = False
    dict_headers = list(FIELDS[0])
    dictionary.append([cell(dictionary, name, header_font, navy, Alignment(horizontal="center", vertical="center", wrap_text=True)) for name in dict_headers])
    for row_no, row in enumerate(FIELDS, 2):
        fill = PatternFill("solid", fgColor=BLUE) if row_no % 2 == 0 else None
        dictionary.append([cell(dictionary, row[name], body_font, fill, wrap) for name in dict_headers])
    dictionary.auto_filter.ref = f"A1:{get_column_letter(len(dict_headers))}{len(FIELDS)+1}"
    for i, name in enumerate(dict_headers, 1): dictionary.column_dimensions[get_column_letter(i)].width = 24 if name != "description" else 64
    workbook.save(path)


def write_docs(out: Path, counts):
    total, executions, outcomes, warmups, defaults, cells = counts
    (out / "README.md").write_text(f"""# {TITLE}

Главный файл — `measurements.csv`: одна широкая таблица для анализа и DataLens. В ней {total} строк: {executions} фактических исполнений и {outcomes} объявленных исходов без строки измерения. Покрыто {cells} клеток.

- `measurements.csv` — машинный источник DataLens.
- `measurements.xlsx` — те же строки и словарь полей для просмотра.
- `dictionary.csv` — тип, единица, зерно и рекомендуемая агрегация каждого поля.
- `DATALENS.md` — модель датасета и набор графиков.
- `BUILD.md` — независимая пересборка и проверка.

Прогревов: {warmups}. Они записаны как `round=0`, `is_warmup=true` и не входят в основные показатели. Основных зачетных исполнений: {defaults}.

Таблица содержит метрики исполнения и принятые агрегаты клетки с префиксом `cell_`. На агрегатных графиках обязательно использовать `is_cell_anchor=true`; это оставляет ровно одну строку на клетку. Для основных графиков отдельных исполнений предусмотрены готовые фильтры `show_time_by_default`, `show_memory_by_default`, `show_cpu_by_default`, `show_payload_by_default`.
""", encoding="utf-8")
    (out / "BUILD.md").write_text(f"""# Сборка

Сборка работает только с локальным принятым пакетом результатов и не подключается к VM, базам данных или облаку.

```sh
python3 -m pip install -r scripts/requirements.txt
python3 scripts/build.py --source /path/to/accepted-package --out /path/to/result
python3 scripts/verify.py --result /path/to/result
```

Входы: `01-results.csv`, `02-executions.csv`, `04-metric-quality.csv`, `06-conditions.csv`, их manifest и acceptance. Сначала проверяются записанные SHA-256 и нулевое число ошибок приемки. Затем исполнения соединяются с метаданными клетки, условиями и качеством метрик. Для 23 клеток без строки измерения сохраняется объявленный исход. Ни медианы, ни рейтинги заново не оцениваются; они переносятся из принятого слоя как поля `cell_*`.

Скрипты сборки и проверки написаны на Python. Единственная внешняя зависимость нужна для XLSX и указана в `scripts/requirements.txt`.
""", encoding="utf-8")
    (out / "DATALENS.md").write_text("""# DataLens

Используйте `measurements.csv` как единственный источник одного датасета. Джойны не нужны.

## Обязательные фильтры

- Основное время: `record_type='execution'` и `show_time_by_default=true`.
- Память: `record_type='execution'` и `show_memory_by_default=true`.
- CPU: `record_type='execution'` и `show_cpu_by_default=true`.
- Полезная нагрузка: `record_type='execution'` и `show_payload_by_default=true`.
- Агрегаты и рейтинги `cell_*`: `is_cell_anchor=true`; агрегация указана в `dictionary.csv`.
- Прогрев по умолчанию скрыт. Отдельный переключатель показывает `is_warmup=true`; это round=0.
- Отказы: отдельный график по `record_type='declared_outcome'` и `failure_stage`.

## Рекомендуемые страницы

1. Обзор: число клеток через `COUNTD(cell_id)`, доля измеренных и объявленные исходы.
2. Время: медиана `wall_s` по движку, пути, библиотеке, объему строк и ширине.
3. Стабильность: распределение раундов, `cell_wall_spread_ratio`, прогрев против зачетных раундов.
4. Память и CPU: `peak_rss_mib`, `cpu_cores_equivalent`, оговорки качества.
5. Передача: отдельно счетчик интерфейса и расчетная полезная нагрузка; не смешивать единицы и пригодность.
6. Рейтинг путей: `is_cell_anchor=true`, `cell_rating_multiplier`, обязательные `rating_group_id` и `rating_baseline_cell_id`.
7. Качество: исключения, `with_caveat`, `caveat`, отказы и уровень свидетельства ограничения памяти.

На графиках раундов используйте `MEDIAN` для времени, памяти и CPU. Для готовых полей `cell_*` используйте `MAX` при фильтре `is_cell_anchor=true`; фактически в группе остается одно значение. Не суммируйте `cell_*` и не пересчитывайте рейтинг после фильтрации строк.
""", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(); source = args.source.resolve(); out = args.out.resolve()
    if source == out: raise ValueError("Source and output must differ")
    source_hashes, source_manifest_hash = validate_source(source)
    rows, cell_count, execution_count, anchor_count = build_rows(source)
    out.mkdir(parents=True, exist_ok=True); (out / "scripts").mkdir(exist_ok=True)
    write_csv(out / "measurements.csv", HEADERS, rows)
    write_csv(out / "dictionary.csv", list(FIELDS[0]), FIELDS)
    for name in SCRIPT_FILES:
        origin = Path(__file__).resolve().parent / name; target = out / "scripts" / name
        if origin.resolve() != target.resolve(): shutil.copy2(origin, target)
    warmups = sum(row["is_warmup"] is True for row in rows)
    defaults = sum(row["include_in_default_stats"] is True for row in rows)
    outcomes = sum(row["record_type"] == "declared_outcome" for row in rows)
    write_docs(out, (len(rows), execution_count, outcomes, warmups, defaults, cell_count))
    build_xlsx(out / "measurements.xlsx", rows)
    files = ["measurements.csv", "dictionary.csv", "measurements.xlsx", "README.md", "BUILD.md", "DATALENS.md"] + ["scripts/" + x for x in SCRIPT_FILES]
    manifest = {
        "schema_version": "smartdata-wide-result/v1", "title": TITLE,
        "grain": "one execution; one declared_outcome only when a cell has no execution row",
        "counts": {"rows": len(rows), "executions": execution_count, "declared_outcomes": outcomes, "cells": cell_count, "cell_anchors": anchor_count + outcomes, "warmups": warmups, "default_executions": defaults},
        "source": {"manifest_sha256": source_manifest_hash, "table_sha256": source_hashes},
        "outputs": [{"file": name, "sha256": sha(out / name), "bytes": (out / name).stat().st_size} for name in files],
    }
    (out / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    report = out / "acceptance.json"
    subprocess.run([sys.executable, str(out / "scripts" / "verify.py"), "--result", str(out), "--report", str(report)], check=True)
    print(json.dumps({"output": str(out), **manifest["counts"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
