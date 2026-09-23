# Запуск эксперимента

1. Подготовьте одинаковые таблицы на `pg` и `ch` по инструкции `docs/data.md`.
2. Скопируйте `config/benchmark.example.toml` в `config/benchmark.toml` и заполните параметры подключения. Пароли храните в переменных окружения, указанных в `password_env`.
3. Выполните короткую проверку на таблицах до 10 000 строк с раундами 0 и 1.
4. Перед полным прогоном на Linux проверьте, что `systemd-run --user --scope` может создать пользовательскую cgroup. План эксперимента задает ограничение памяти 120 GiB и нулевой swap.
5. Запустите PostgreSQL и ClickHouse отдельно:

```bash
python -m benchmark run --config config/benchmark.toml --engine pg --output artifacts/run-pg
python -m benchmark run --config config/benchmark.toml --engine ch --output artifacts/run-ch
python -m benchmark summarize --raw artifacts/run-pg/raw.csv --out artifacts/run-pg/summary.csv
python -m benchmark summarize --raw artifacts/run-ch/raw.csv --out artifacts/run-ch/summary.csv
```

Успешный запуск создает `raw.csv` и `journal.jsonl` в каждом выходном каталоге. Ненулевой код команды означает, что как минимум один вариант завершился ошибкой; подробности находятся в `journal.jsonl` и в строке исходного CSV со статусом ошибки.

Запишите версии СУБД, Python, библиотек и ядра ОС, а также модель CPU, объем памяти клиента, сетевую схему и время UTC. Во время измерений не запускайте на клиенте и сервере другую нагрузку. Сохраните исходные CSV и `journal.jsonl` без изменений; итоговый набор собирайте из их копии после проверки.
