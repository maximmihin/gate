# Designing Data-Intensive Applications, 2nd Edition

**Authors:** Martin Kleppmann, Chris Riccomini  
**Edition:** 2nd Edition  
**Year:** 2026  
**Publisher:** O'Reilly Media, Inc.  
**Original:** `originals/Kleppmann M., Riccomini C. - Designing Data-Intensive Applications, 2nd Edition - 2026.epub`

Consultation protocol: start here to locate a chapter/section, check `topics.md` for question-to-section mapping, `grep` the phrase you need inside `chapters/`, then read only the matching slice of the chapter file.

## Chapters

### Preface

File: `chapters/00-preface.md` — ~1,951 words

- [Preface](chapters/00-preface.md#preface)
- [Who Should Read This Book?](chapters/00-preface.md#who-should-read-this-book)
- [What’s New in the Second Edition?](chapters/00-preface.md#whats-new-in-the-second-edition)
- [References and Further Reading](chapters/00-preface.md#references-and-further-reading)
- [Conventions Used in This Book](chapters/00-preface.md#conventions-used-in-this-book)
- [O’Reilly Online Learning](chapters/00-preface.md#oreilly-online-learning)
- [How to Contact Us](chapters/00-preface.md#how-to-contact-us)
- [Acknowledgments](chapters/00-preface.md#acknowledgments)

### 1. Trade-Offs in Data Systems Architecture

File: `chapters/01-trade-offs-in-data-systems-architecture.md` — ~10,659 words

- [Operational Versus Analytical Systems](chapters/01-trade-offs-in-data-systems-architecture.md#operational-versus-analytical-systems)
- [Cloud Versus Self-Hosting](chapters/01-trade-offs-in-data-systems-architecture.md#cloud-versus-self-hosting)
- [Distributed Versus Single-Node Systems](chapters/01-trade-offs-in-data-systems-architecture.md#distributed-versus-single-node-systems)
- [Data Systems, Law, and Society](chapters/01-trade-offs-in-data-systems-architecture.md#data-systems-law-and-society)
- [Summary](chapters/01-trade-offs-in-data-systems-architecture.md#summary)

### 2. Defining Nonfunctional Requirements

File: `chapters/02-defining-nonfunctional-requirements.md` — ~9,712 words

- [Case Study: Social Network Home Timelines](chapters/02-defining-nonfunctional-requirements.md#case-study-social-network-home-timelines)
- [Describing Performance](chapters/02-defining-nonfunctional-requirements.md#describing-performance)
- [Reliability and Fault Tolerance](chapters/02-defining-nonfunctional-requirements.md#reliability-and-fault-tolerance)
- [Scalability](chapters/02-defining-nonfunctional-requirements.md#scalability)
- [Maintainability](chapters/02-defining-nonfunctional-requirements.md#maintainability)
- [Summary](chapters/02-defining-nonfunctional-requirements.md#summary)

### 3. Data Models and Query Languages

File: `chapters/03-data-models-and-query-languages.md` — ~15,822 words

- [Relational Versus Document Models](chapters/03-data-models-and-query-languages.md#relational-versus-document-models)
- [Graph-Like Data Models](chapters/03-data-models-and-query-languages.md#graph-like-data-models)
- [Event Sourcing and CQRS](chapters/03-data-models-and-query-languages.md#event-sourcing-and-cqrs)
- [DataFrames, Matrices, and Arrays](chapters/03-data-models-and-query-languages.md#dataframes-matrices-and-arrays)
- [Summary](chapters/03-data-models-and-query-languages.md#summary)

### 4. Storage and Retrieval

File: `chapters/04-storage-and-retrieval.md` — ~14,434 words

- [Storage and Indexing for OLTP](chapters/04-storage-and-retrieval.md#storage-and-indexing-for-oltp)
- [Data Storage for Analytics](chapters/04-storage-and-retrieval.md#data-storage-for-analytics)
- [Multidimensional and Full-Text Indexes](chapters/04-storage-and-retrieval.md#multidimensional-and-full-text-indexes)
- [Summary](chapters/04-storage-and-retrieval.md#summary)

### 5. Encoding and Evolution

File: `chapters/05-encoding-and-evolution.md` — ~11,801 words

- [Formats for Encoding Data](chapters/05-encoding-and-evolution.md#formats-for-encoding-data)
- [Modes of Dataflow](chapters/05-encoding-and-evolution.md#modes-of-dataflow)
- [Summary](chapters/05-encoding-and-evolution.md#summary)

### 6. Replication

File: `chapters/06-replication.md` — ~19,090 words

- [Single-Leader Replication](chapters/06-replication.md#single-leader-replication)
- [Multi-Leader Replication](chapters/06-replication.md#multi-leader-replication)
- [Leaderless Replication](chapters/06-replication.md#leaderless-replication)
- [Summary](chapters/06-replication.md#summary)

### 7. Sharding

File: `chapters/07-sharding.md` — ~8,534 words

- [Pros and Cons of Sharding](chapters/07-sharding.md#pros-and-cons-of-sharding)
- [Sharding for Multitenancy](chapters/07-sharding.md#sharding-for-multitenancy)
- [Sharding of Key-Value Data](chapters/07-sharding.md#sharding-of-key-value-data)
- [Request Routing](chapters/07-sharding.md#request-routing)
- [Sharding and Secondary Indexes](chapters/07-sharding.md#sharding-and-secondary-indexes)
- [Summary](chapters/07-sharding.md#summary)

### 8. Transactions

File: `chapters/08-transactions.md` — ~23,731 words

- [What Exactly Is a Transaction?](chapters/08-transactions.md#what-exactly-is-a-transaction)
- [Weak Isolation Levels](chapters/08-transactions.md#weak-isolation-levels)
- [Serializability](chapters/08-transactions.md#serializability)
- [Distributed Transactions](chapters/08-transactions.md#distributed-transactions)
- [Summary](chapters/08-transactions.md#summary)

### 9. The Trouble with Distributed Systems

File: `chapters/09-the-trouble-with-distributed-systems.md` — ~19,240 words

- [Faults and Partial Failures](chapters/09-the-trouble-with-distributed-systems.md#faults-and-partial-failures)
- [Unreliable Networks](chapters/09-the-trouble-with-distributed-systems.md#unreliable-networks)
- [Unreliable Clocks](chapters/09-the-trouble-with-distributed-systems.md#unreliable-clocks)
- [Knowledge, Truth, and Lies](chapters/09-the-trouble-with-distributed-systems.md#knowledge-truth-and-lies)
- [Summary](chapters/09-the-trouble-with-distributed-systems.md#summary)

### 10. Consistency and Consensus

File: `chapters/10-consistency-and-consensus.md` — ~17,501 words

- [Linearizability](chapters/10-consistency-and-consensus.md#linearizability)
- [ID Generators and Logical Clocks](chapters/10-consistency-and-consensus.md#id-generators-and-logical-clocks)
- [Consensus](chapters/10-consistency-and-consensus.md#consensus)
- [Summary](chapters/10-consistency-and-consensus.md#summary)

### 11. Batch Processing

File: `chapters/11-batch-processing.md` — ~12,635 words

- [Batch Processing with Unix Tools](chapters/11-batch-processing.md#batch-processing-with-unix-tools)
- [Batch Processing in Distributed Systems](chapters/11-batch-processing.md#batch-processing-in-distributed-systems)
- [Batch Processing Models](chapters/11-batch-processing.md#batch-processing-models)
- [Batch Use Cases](chapters/11-batch-processing.md#batch-use-cases)
- [Summary](chapters/11-batch-processing.md#summary)

### 12. Stream Processing

File: `chapters/12-stream-processing.md` — ~18,771 words

- [Transmitting Event Streams](chapters/12-stream-processing.md#transmitting-event-streams)
- [Databases and Streams](chapters/12-stream-processing.md#databases-and-streams)
- [Processing Streams](chapters/12-stream-processing.md#processing-streams)
- [Summary](chapters/12-stream-processing.md#summary)

### 13. A Philosophy of Streaming Systems

File: `chapters/13-a-philosophy-of-streaming-systems.md` — ~17,404 words

- [Data Integration](chapters/13-a-philosophy-of-streaming-systems.md#data-integration)
- [Unbundling Databases](chapters/13-a-philosophy-of-streaming-systems.md#unbundling-databases)
- [Aiming for Correctness](chapters/13-a-philosophy-of-streaming-systems.md#aiming-for-correctness)
- [Summary](chapters/13-a-philosophy-of-streaming-systems.md#summary)

### 14. Doing the Right Thing

File: `chapters/14-doing-the-right-thing.md` — ~6,182 words

- [Predictive Analytics](chapters/14-doing-the-right-thing.md#predictive-analytics)
- [Privacy and Tracking](chapters/14-doing-the-right-thing.md#privacy-and-tracking)
- [Summary](chapters/14-doing-the-right-thing.md#summary)

## Reference

- **Glossary:** [`glossary.md`](glossary.md) — key terms, one-line definitions with links into the text
- **Topic map:** [`topics.md`](topics.md) — curated "which sections answer which engineering question"

> `INDEX.md` and `chapters/` are generated by the book-ingest skill; do not edit them by hand — re-run the ingest script instead. `topics.md` is curated manually.
