# Сетевые захваты

Каталог содержит 12 воспроизводимых примеров передачи результата из PostgreSQL и ClickHouse. Для каждого способа сохранены исходный захват, расшифровка протокола, извлеченные TCP-потоки и результат автоматической проверки.

Все соединения проходили через loopback-интерфейс `127.0.0.1`. Трафик не зашифрован, чтобы Wireshark мог показать структуру протокола и данные на проводе.

## Примеры

| Каталог | Способ передачи | Декодирование в Wireshark |
| --- | --- | --- |
| `pg-simple-text` | PostgreSQL simple query, текстовые значения | PostgreSQL, TCP 5432 |
| `pg-extended-binary` | PostgreSQL extended query, двоичные значения | PostgreSQL, TCP 5432 |
| `pg-copy-binary` | PostgreSQL COPY binary | PostgreSQL, TCP 5432 |
| `pg-adbc` | PostgreSQL ADBC | PostgreSQL, TCP 5432 |
| `pg-flight` | Flight SQL поверх PostgreSQL | HTTP/2, TCP 15436 |
| `ch-native` | ClickHouse Native TCP | встроенный разбор ClickHouse |
| `ch-http-native` | ClickHouse Native по HTTP | HTTP, TCP 18123 |
| `ch-http-rowbinary` | ClickHouse RowBinary по HTTP | HTTP, TCP 18123 |
| `ch-http-arrow` | ArrowStream по HTTP | HTTP, TCP 18123 |
| `ch-adbc` | ClickHouse ADBC по HTTP | HTTP, TCP 18123 |
| `ch-flight` | Flight SQL поверх ClickHouse | HTTP/2, TCP 19090 |
| `ch-mysql` | MySQL-совместимый интерфейс ClickHouse | MySQL, TCP 9004 |

## Состав набора

Каждый каталог содержит:

- `capture.pcapng` — исходный захват;
- `details.pdml` и `details.txt` — полная расшифровка Wireshark;
- `packets.txt` — краткий список пакетов;
- `stream-*.txt` — извлеченные TCP-потоки;
- `client-output.txt` и `client.log` — результат и журнал клиента;
- `evidence.json` — наблюдаемые свойства протокола;
- `validation.json` — результат проверки набора.

Некоторые каталоги также содержат извлеченный ответ сервера: например, `copy-response.bin`, `http-response.rowbinary` или `http-response.arrows`.

## Проверка целостности

[`manifest.json`](manifest.json) содержит размер и SHA-256 каждого `capture.pcapng`, число пакетов и параметры `decode-as`. Поле `same_run=true` означает, что захват, вывод клиента и проверка относятся к одному запуску. Во всех 12 записях поле `status` имеет значение `pass`.

Для ручной проверки откройте `capture.pcapng` в Wireshark. Если для записи указано `decode_as`, примените соответствующее правило к TCP-порту. Версии клиентов находятся в [`versions.json`](versions.json), а четыре строки тестовых данных — в [`fixture.json`](fixture.json).

## Учетные данные в открытом трафике

В захватах видны тестовые учетные данные `workshop/workshop` и токены локальных Flight-сессий. Они являются частью примера незашифрованного обмена и относятся только к опубликованному тестовому окружению. Не используйте эти значения в других системах.
