# Тематическая карта: какой вопрос — какие главы

Кураторская карта для консультаций: найди тему, прочитай указанные секции, при нужде — grep по терминам.
Формат: **Тема** → типовые вопросы → главы (`chapters/NN-*.md`) и названия секций (искать grep'ом по файлу).

---

## Выбор модели данных (реляционная vs документная vs графовая)
«Какую БД выбрать под этот кейс?», «нормализовать или дублировать?», «нужен ли граф?»
- `03-data-models-and-query-languages.md` — вся глава: Object-Relational Mismatch, Normalization/Denormalization trade-offs, When to Use Which Model, Graph-Like Data Models, Property Graphs, Event Sourcing and CQRS
- grep: `normalization`, `denormalization`, `document model`, `property graph`, `Cypher`, `locality`

## Схемы, кодирование и эволюция API (миграции, совместимость)
«Как менять схему без даунтайма?», «protobuf vs Avro vs JSON?», «обратная совместимость сервисов?»
- `05-encoding-and-evolution.md` — Formats for Encoding Data, Protocol Buffers, Avro (writer's/reader's schema, Schema evolution rules), Modes of Dataflow, REST and RPC, Event-Driven Architectures
- grep: `schema evolution`, `backward compatibility`, `field tag`, `writer's schema`, `rolling upgrade`

## Хранилища: движки, индексы, LSM vs B-Tree
«Почему у нас медленные записи/чтения?», «как устроен индекс?», «LSM или B-Tree?», «колоночное хранилище?»
- `04-storage-and-retrieval.md` — Log-Structured Storage, SSTable, Bloom filters, B-Trees, Comparing B-Trees and LSM-Trees (read/write amplification), Column-Oriented Storage, Full-Text Search, Vector Embeddings
- grep: `SSTable`, `compaction`, `bloom filter`, `write amplification`, `column-oriented`, `secondary index`

## Репликация (топологии, лаг, конфликты)
«One leader или multi-leader?», «sync vs async репликация?», «eventual consistency на практике?», «CRDT?»
- `06-replication.md` — вся глава: Single-Leader (Synchronous vs Asynchronous, Failover), Problems with Replication Lag (reading your own writes, monotonic reads), Multi-Leader (topologies, conflict resolution, CRDT), Leaderless (quorums, Detecting Concurrent Writes, version vectors), Sync Engines и local-first
- grep: `failover`, `replication lag`, `quorum`, `last write wins`, `version vector`, `read repair`

## Шардирование и партиционирование
«Как шардиать?», «hot spots / перекос нагрузки?», «rebalancing?», «вторичные индексы в шарде?»
- `07-sharding.md` — вся глава: Sharding by Key Range / Hash of Key, Consistent hashing, Skewed Workloads and Hot Spots, Request Routing, Local vs Global Secondary Indexes
- grep: `consistent hashing`, `hot spot`, `rebalancing`, `request router`, `pre-splitting`

## Транзакции и уровни изоляции
«Какой уровень изоляции нужен?», «что такое write skew / lost update / phantom?», «Serializable vs Snapshot Isolation?», «2PC и XA?»
- `08-transactions.md` — The Meaning of ACID, Weak Isolation Levels (Read Committed, Snapshot Isolation/MVCC), Preventing Lost Updates, Write Skew and Phantoms, Serializability (2PL, SSI), Distributed Transactions (Two-Phase Commit, XA, exactly-once)
- grep: `write skew`, `lost update`, `snapshot isolation`, `MVCC`, `serializable`, `two-phase commit`, `compare-and-set`

## Распределённые системы: сеть, время, частичные отказы
«Почему нельзя полагаться на часы/таймауты?», «fencing tokens?», «детект упавшей ноды?»
- `09-the-trouble-with-distributed-systems.md` — Unreliable Networks (timeouts, unbounded delays), Unreliable Clocks (monotonic vs time-of-day, Clock Synchronization, Relying on Synchronized Clocks), Process Pauses, Knowledge Truth and Lies, Distributed Locks and Leases (Fencing tokens), Byzantine Faults, Safety and Liveness
- grep: `fencing token`, `clock skew`, `monotonic clock`, `lease`, `timeout`, `partial failure`, `safety`, `liveness`

## Консистентность и консенсус (линеаризуемость, CAP, etcd/ZooKeeper)
«Нужна ли нам линеаризуемость?», «CAP на практике?», «как выбрать/использовать coordination service?», «lamport timestamps?»
- `10-consistency-and-consensus.md` — Linearizability (What Makes a System Linearizable, The Cost, CAP theorem), Logical Clocks (Lamport, hybrid), Consensus (single-value, shared logs, total order broadcast), Consensus in Practice, Coordination Services
- grep: `linearizable`, `CAP theorem`, `Lamport timestamp`, `total order broadcast`, `etcd`, `ZooKeeper`, `leader election`

## Пакетная обработка (ETL, Hadoop-стиль, дата-пайплайны)
«Batch vs stream?», «как делать join в пайплайне?», «data locality, shuffle?»
- `11-batch-processing.md` — Batch Processing with Unix Tools, Distributed Filesystems, MapReduce, Dataflow Engines, Shuffling Data, Joins and Grouping, Query Languages/DataFrames, Extract-Transform-Load, Serving Derived Data
- grep: `MapReduce`, `shuffle`, `join`, `dataflow`, `DAG`, `workflow scheduler`

## Потоковая обработка (Kafka-стиль, CDC, event streaming)
«Kafka под нашу задачу?», «CDC из БД?», «event time vs processing time, окна?», «exactly-once в стримах?»
- `12-stream-processing.md` — Messaging Systems, Log-Based Message Brokers (consumer offsets, log compaction, replay), Change Data Capture, Databases and Streams, State Streams and Immutability, Reasoning About Time (windows), Stream Joins, Fault Tolerance (checkpointing, idempotence)
- `13-a-philosophy-of-streaming-systems.md` — Data Integration, Derived data vs distributed transactions, Unbundling Databases, End-to-End Argument, Enforcing Constraints, exactly-once
- grep: `log compaction`, `consumer offset`, `CDC`, `change data capture`, `window`, `watermark`, `idempotence`, `event sourcing`

## Архитектура data-intensive приложения целиком
«Как связать OLTP и аналитику?», «system of record vs derived data?», «микросервисы?», «cloud vs self-hosted?»
- `01-trade-offs-in-data-systems-architecture.md` — Operational Versus Analytical Systems, Data Warehousing (warehouse → lake), Systems of Record and Derived Data, Cloud Versus Self-Hosting, Cloud Native System Architecture, Separation of storage and compute, Microservices and Serverless
- `02-defining-nonfunctional-requirements.md` — Case Study: Social Network Home Timelines (materializing timelines), Describing Performance (percentiles, SLO/SLA), Reliability, Scalability, Maintainability
- grep: `derived data`, `system of record`, `data lake`, `materialized view`, `percentile`, `SLO`, `SLA`, `shared-nothing`

## Корректность end-to-end (дубликаты, exactly-once, ограничения уникальности)
«Как гарантировать exactly-once?», «уникальность без глобального замка?», «где ставить проверки?»
- `13-a-philosophy-of-streaming-systems.md` — The End-to-End Argument for Databases (Duplicate suppression, Uniquely identifying requests), Enforcing Constraints (Uniqueness requires consensus, Coordination-avoiding data systems), Trust but Verify (auditability)
- `12-stream-processing.md` — Fault Tolerance (Idempotence, Atomic commit revisited)
- grep: `exactly-once`, `idempotent`, `duplicate suppression`, `end-to-end`, `coordination-avoiding`

## Выбор хранилища для аналитики / витрины
«Data warehouse / lakehouse?», «колоночные форматы, Parquet?», «materialized views?»
- `04-storage-and-retrieval.md` — Data Storage for Analytics, Cloud Data Warehouses, Column-Oriented Storage (compression, sort order), Materialized Views and Data Cubes, Query Execution: Compilation and Vectorization
- `01-trade-offs-in-data-systems-architecture.md` — Data Warehousing, From data warehouse to data lake
- grep: `column-oriented`, `data cube`, `materialized view`, `vectorized`, `snowflake schema`

## Интеграция и кэширование, состояние клиентов
«Как держать кэш/представления в согласии с источником?», «offline-first клиент?», «push обновлений?»
- `13-a-philosophy-of-streaming-systems.md` — Observing Derived State (Materialized views and caching, Stateful offline-capable clients, Pushing state changes to clients, Reads are events too)
- `06-replication.md` — Sync Engines and Local-First Software
- grep: `read-through`, `cache invalidation`, `offline-first`, `derived state`

## Термины и базовые определения
«Что такое X одним абзацем?»
- `glossary.md` — короткие определения с ссылками на разделы
- `INDEX.md` — карта всех глав и секций

## Этика, приватность, законодательство
«Privacy/PII в данных», «bias в моделях»
- `14-doing-the-right-thing.md` — Predictive Analytics, Bias and Discrimination, Privacy and Tracking, Legislation and Self-Regulation

---

## Как консультироваться (протокол)

1. **Сузь вопрос.** Определи тему выше → глава(ы).
2. **Точечное чтение.** В файле главы используй INDEX-список секций в начале файла или grep по названию секции; читай кусок файла вокруг совпадения (grep возвращает номера строк).
3. **Проверяй смежное.** Ссылки `[Chapter N](...)` внутри текста ведут в другие главы с якорями.
4. **Отвечай с цитатой.** Указывай главу и секцию: «DDIA 2e, гл. 8, Write Skew and Phantoms».
