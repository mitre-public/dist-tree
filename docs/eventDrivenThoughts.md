## Making an Event-Driven Metric Tree

### Decision: Writing data to a Tree should be isolated from Reading (CQOS)

Reasons:

- Can "batch-up" high-frequency writes to reduce thrashing on the underlying data-store
- Can simplify "Data Consuming code" because it just serves up what ever is in the data-store

### Decision: Eventually use a Kafka-like queue to store incoming data

Reasons:

- Kafka will buffer data the "new data rate" exceeds the "can absorb and index rate"
- A "Kafka-layer" will allow multiple DataWriters to write to the same tree
    - For example, 5 data producing clients can simultaneously write to a single tree because the "tree creation
      process" only indexes data found in the "shared the Kafka topic"
- Kafka will handle
    - **Data Durability:** new data won't be lost assuming we use Kafka correctly
    - **Parallelism:** multiple DataWriters can write to the same tree if we force all new tree data to "start" its
      journey by getting written to a topic
    - **Data Ordering:** If we ever need a definitive order of adds (and maybe deletes) Kafka can provide this
- Achieving durability will be easier if the "tree creating and indexing process" is a _simple bridge_ between mature tech stacks. 
  - New un-indexed data starts in Kafka (a mature, battle-tested, easy-to-use product)
  - We organize the data into a Tree (a brand-new, custom, unproven technology)
  - "Tree-Structured data" is placed in battle-tested durable data store like DynamoDB, DuckDB, etc. 