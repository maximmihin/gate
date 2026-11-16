# outbox — transactional outbox for PostgreSQL

Реализация transactional outbox: события записываются в таблицу **в той же
транзакции, что и бизнес-запись**, а отдельный релей доставляет их наружу
(Kafka, HTTP, индексатор — любой `Publisher`). Референс по дизайну — DDIA 2e,
гл. 12 («Keeping Systems in Sync», врезка про outbox) и гл. 13 (end-to-end
argument, идемпотентность).

## Гарантии

| Свойство | Гарантия | Как достигается |
|---|---|---|
| Атомарность записи | бизнес-запись и события коммитятся вместе или никак | `Append` выполняется в транзакции вызывающего |
| Доставка | **at-least-once** | claim + mark processed; крах между ними → повторная доставка |
| Порядок | тотальный порядок доставки (один писатель); per-aggregate — в happy path | единый `id`, single leader через `pg_advisory_lock` |
| Дубликаты | устраняются **на потребителе** по `event_id` | см. сниппет ниже |
| Ядовитые события | dead-letter после `MaxAttempts`, поток не блокируется | `dead_at` + `OnDead`-хук |
| Отказ релея | standby забирает лидерство за `LockRetry` | session-level advisory lock умирает вместе с коннектом |

Чего **нет** (осознанно): exactly-once до потребителя (не существует
end-to-end, DDIA гл. 13) и строгого порядка при ретраях — событие с
провальной публикацией уходит в backoff, релей едет дальше, поэтому события
одного агрегата могут обменяться местами (DDIA гл. 12, Fig. 12-2). Потребителям,
которым важен порядок, следует носить версию агрегата в payload.

## Схема и контракт

`migrations/0001_outbox.up.sql`. Публичный контракт — только форма событий:
`event_id`, `event_type`, `aggregate_id`, `payload`, `headers`, `created_at`.
Колонки бухгалтии (`processed_at`, `dead_at`, `attempts`, `next_attempt_at`,
`last_error`) — внутренние, меняйте свободно. Outbox-схема — это API для
даунстрима (DDIA гл. 12, врезка «CDC and Database Schemas»): эволюция payload
только с версионированием и обратной совместимостью.

## Запись (в приложении)

```go
tx, _ := conn.Begin(ctx)
// ... бизнес-запись ...
err = outbox.Append(ctx, tx,
    outbox.NewEvent{EventType: "order.placed", AggregateID: orderID,
                    Payload: order, Headers: map[string]string{"schema_version": "1"}})
err = tx.Commit(ctx)
```

## Релей

Один процесс = один `Relay.Run(ctx)` (горутина). Релей открывает **ровно два
единичных коннекта** (без pgxpool — сознательно):

- **work** — advisory lock + все запросы: последовательное использование даёт
  естественный тотальный порядок; session-lock живёт ровно столько, сколько
  коннект → failover без «зависших» локов;
- **notify** — только `LISTEN` и ожидание уведомлений: `WaitForNotification`
  требует эксклюзивного владения коннектом, поэтому он отдельный.

Поток: `NOTIFY` после коммита будит релей (триггер, statement-level) → релей
дрейнит батчи `FOR UPDATE SKIP LOCKED` до пустого → засыпает до следующего
NOTIFY / тикера (`PollInterval` — страховка от потерянных notify и двигатель
ретраев). Реконнекты с backoff, graceful shutdown снимает лок.

Почему SKIP LOCKED, если писатель один? Дешёвая защита от человеческого фактора
(два релея с разными advisory-ключами) и от «зависшего» TX во время отладки.

## Ретраи и реконнект

Вся математика задержек — на примитивах **`k8s.io/apimachinery/pkg/util/wait`**
(`wait.Backoff`, `Step()`, `wait.Jitter`) — ровно тот механизм, на котором
построен `client-go/util/retry`. Сам `client-go` сознательно не тянем: у него
огромное дерево зависимостей, а его `retry.OnError` внутри вызывает ctx-less
`wait.ExponentialBackoff` с `time.Sleep` — прерываемый сон (быстрый graceful
shutdown) через него не получить.

Три точки применения:

- **Реконнект в `Run()`** — `wait.Backoff{Duration: 1s, Factor: 2, Cap: 30s,
  Jitter: RetryJitter}`; задержка берётся `Step()`, сон — ctx-aware (см.
  ниже). Сброс к 1s — пересозданием структуры — только после сессии,
  прожившей ≥ 30s: длительный outage держит кап 30s, а не стартует заново.
- **Standby-режим в `acquireLock()`** — интервал между попытками
  `pg_try_advisory_lock` джиттерится `wait.Jitter(LockRetry, RetryJitter)`:
  толпа standby-релеев не штурмует лок в ногу после смерти лидера.
- **`next_attempt_at` в `claim()`** — экспонента `base·2^(n−1)` с капом
  `RetryMaxDelay` (семантика прежняя: attempt 1 → base), плюс независимый
  джиттер **на каждую строку** — батч событий, уроненный одним сбоем брокера,
  ретраится разреженно, а не синхронной стаей (анти-thundering-herd).

Джиттер по умолчанию 10% (`Config.RetryJitter`, допустимо `(0, 1)`). Подводный
камень: `wait.Jitter(d, 0)` — не «без джиттера», а *полный* джиттер (до ×2),
поэтому неположительное значение конфига молча заменяется дефолтом.

Нюансы актуального API (сверено с исходниками apimachinery v0.37.2): методов
`Backoff.Sleep(ctx, d)`, `Backoff.Reset()` и `DurationForAttempt(n)` там нет —
сон реализован 6-строчным хелпером `sleepCtx` (timer + select на `ctx.Done()`),
сброс — пересозданием `Backoff`, задержка для attempt n — `wait.Jitter`
поверх экспоненты (джиттер в `Step()` не накапливается, поэтому формула
эквивалентна). `DelayWithReset` не подошёл для реконнекта: он сбрасывает
экспоненту по календарю (время с последнего сброса), т.е. при длительном
outage циклически возвращался бы к 1s.

## Потребитель: дедупликация (обязательна)

Прямой аналог Example 13-2 из DDIA (гл. 13): уникальный constrain + вставка,
дубликат отвергается на уровне БД даже при слабой изоляции.

```sql
CREATE TABLE processed_events (
    event_id     UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- в транзакции обработки:
INSERT INTO processed_events (event_id) VALUES ($1)   -- event_id из события
ON CONFLICT (event_id) DO NOTHING;                    -- 0 затронутых строк = дубликат, пропустить
```

## Kafka-паблишер (скетч)

```go
type KafkaPublisher struct{ w *kafka.Writer } // segmentio/kafka-go

func NewKafkaPublisher(brokers []string, topic string) *KafkaPublisher {
	return &KafkaPublisher{w: &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},        // aggregate_id -> партиция = порядок по ключу
		RequiredAcks: kafka.RequireAll,     // "durably accepted" == acks=all
		BatchSize:    1,                    // сохраняем порядок; батчит сам брокер-клиент
		Async:        false,
	}}
}

func (p *KafkaPublisher) Publish(ctx context.Context, evt outbox.Event) error {
	return p.w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(evt.AggregateID),
		Value: evt.Payload,
		Headers: []kafka.Header{{Key: "event_id", Value: []byte(evt.EventID)},
			{Key: "event_type", Value: []byte(evt.EventType)}},
	})
}
```

Fan-out на нескольких потребителей — работа consumer groups Kafka (DDIA гл. 12,
«Multiple consumers»); outbox-таблица знает только «довезти до брокера».

## Мониторинг

```sql
-- backlog: сколько и насколько старых недоставленных событий
SELECT count(*), coalesce(max(now() - created_at), interval '0') AS oldest
FROM outbox WHERE processed_at IS NULL AND dead_at IS NULL;

-- dead letters: любое число > 0 — инцидент (алерт через OnDead + этот запрос)
SELECT id, event_id, event_type, attempts, last_error, dead_at
FROM outbox WHERE dead_at IS NOT NULL;

-- ретраи: устойчивый рост — даунстрим деградирует
SELECT count(*) FROM outbox
WHERE processed_at IS NULL AND dead_at IS NULL AND attempts > 0;
```

Plus `Relay.Stats()` (published/failed/dead/leadership_gains) и лог
`leadership acquired` — смены лидерства чаще, чем деплои заслуживают внимания.

## Отказоустойчивость: что происходит при крахе

| Момент краха | Исход |
|---|---|
| после коммита бизнес-tx, до claim | событие подобрано следующим релеем/тиком |
| после claim, до Publish | повтор после `next_attempt_at` (запланирован в claim) |
| после Publish, до `processed_at` | **повторная доставка** — потребитель дедуплицирует по `event_id` |
| во время лидерства | сессия умирает → лок снят → standby забирает за `LockRetry` |
| зомби-релей (жив, но отрезан) | TCP keepalive убивает сессию → лок снят; новый лидер дедуплицируется потребителем |

## Запуск

```bash
docker compose up -d --wait                      # postgres:17 на :5422
go run ./cmd/outbox-demo                         # демо: failover, ретраи, dead-letter
OUTBOX_TEST_DSN='postgres://outbox:outbox@localhost:5422/outbox?sslmode=disable' \
  go test ./outbox/ -count=1 -v                  # интеграционные тесты
```

## Эволюция (когда упрётесь)

- **Объём таблицы** — партиционирование по `created_at` + джоба чистки
  обработанных партиций (архив — полезный аудит-лог, DDIA гл. 13 «Trust, but
  Verify»).
- **Пропускная способность одного писателя** — шардинг релеев по
  `hash(aggregate_id)` (порядок per-aggregate сохраняется, глобальный теряется
  осознанно — DDIA гл. 13, «The limits of total ordering»).
- **Крупные payload** — claim-check: блоб в объектное хранилище, в событии
  ссылка.
