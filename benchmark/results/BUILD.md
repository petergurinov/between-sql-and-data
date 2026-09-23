# Пересборка результатов

Скрипт читает локальный пакет, который уже прошел проверку. Он не подключается к виртуальным машинам или базам данных.

```bash
python3 -m pip install -r scripts/requirements.txt
python3 scripts/build.py --source /path/to/accepted-package --out /path/to/result
python3 scripts/verify.py --result /path/to/result
```

Исходный каталог должен содержать `01-results.csv`, `02-executions.csv`, `04-metric-quality.csv`, `06-conditions.csv`, а также `manifest.json` и `acceptance.json`. Сначала скрипт сверяет SHA-256 входных файлов и проверяет, что `acceptance.json` не содержит ошибок. Затем он объединяет выполнения с описанием вариантов, условиями запуска и статусами качества метрик. Для 23 вариантов без строки измерения сохраняется объявленный исход.

Медианы и рейтинги не пересчитываются: скрипт переносит принятые значения в поля `cell_*`. Для создания XLSX нужна единственная внешняя зависимость, указанная в `scripts/requirements.txt`.
