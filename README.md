# gate

Площадка для прототипов инженерных систем. Референсы «как правильно» — в
[`books/`](books/) (сейчас: DDIA 2e, см. `books/library/ddia-2e/INDEX.md`).

## Прототипы

| Пакет | Что это | Референс |
|---|---|---|
| [`outbox/`](outbox/) | transactional outbox для PostgreSQL: LISTEN/NOTIFY, advisory lock, SKIP LOCKED, ретраи, dead-letter | DDIA 2e, гл. 12–13 |

```bash
docker compose up -d --wait   # тестовый postgres
go run ./cmd/outbox-demo      # демо outbox-релея
```
