# Загрузка в DataLens

Используйте `measurements.csv` как единственный источник датасета. Объединять его с другими таблицами не нужно.

## Обязательные фильтры

- Время: `record_type='execution'` и `show_time_by_default=true`.
- Память: `record_type='execution'` и `show_memory_by_default=true`.
- CPU: `record_type='execution'` и `show_cpu_by_default=true`.
- Полезная нагрузка: `record_type='execution'` и `show_payload_by_default=true`.
- Показатели `cell_*`: `is_cell_anchor=true`; функция агрегации указана в `dictionary.csv`.
- Прогрев по умолчанию скрыт. Для отдельного анализа прогрева используйте `is_warmup=true`; номер прогревочного раунда — 0.
- Отказы: `record_type='declared_outcome'` с группировкой по `failure_stage`.

## Рекомендуемые страницы

1. Обзор: число вариантов через `COUNTD(cell_id)`, доля измеренных вариантов и объявленные исходы.
2. Время: медиана `wall_s` по СУБД, способу чтения, библиотеке, числу строк и ширине таблицы.
3. Стабильность: распределение раундов, `cell_wall_spread_ratio`, сравнение прогрева с основными раундами.
4. Память и CPU: `peak_rss_mib`, `cpu_cores_equivalent`, статусы качества и оговорки.
5. Передача: счетчик интерфейса и расчетная полезная нагрузка на разных графиках.
6. Сравнение способов: `is_cell_anchor=true`, `cell_rating_multiplier`, `rating_group_id` и `rating_baseline_cell_id`.
7. Качество: исключения, `with_caveat`, `caveat`, отказы и `memory_evidence_level`.

Для метрик отдельных раундов используйте `MEDIAN`. Для полей `cell_*` используйте `MAX` вместе с фильтром `is_cell_anchor=true`: после фильтра в группе остается одно значение. Не суммируйте `cell_*` и не пересчитывайте `cell_rating_multiplier` после фильтрации строк.
