
## Goals and Non-Goals

### User-facing Goals

1. **Easy to publish high-dimensional data**
2. **Easy to search high-dimensional data**
3. **Easy to "plug-in" a mature database stack**

### Internal Goals

1. **Re-architect `MetricTree`. Take lessons from Databases, B-Trees, and Kafka**
2. **Support measuring the impact of B-Tree design choices**
    - How does the branch factor impact speed?
    - How does leaf node splitting policy impact speed?
    - How many Key-Value pairs should be stored in a leaf?
    - What should we do if the `DistanceMetric` is insanely expensive to compute?
    - What should we do if the `DistanceMetric` is trivial to compute?
    - etc. etc.
3. **Understand how a high dimensional `DistanceTree` differs from a B-Tree**
    - You cannot rebalance in the traditional way.
    - When a DataPage (i.e., leaf node) grows too big it requires computation to determine which Key/Value pairs go to
      which DataPage .

### Non-Goals

1. **Being a database.**
    - We might use a DB, we won't implement our own DB.
2. **Being responsible for data durability.**
    - This project IS NOT taking on data replication and the plethora of issues that requires
    - This feature must be inherited from a component (e.g. we are reliable because Postgres and Kafka are reliable).
3. **Being responsible for ACID transactions.**
    - This project IS NOT taking on properly provided ACID guarantees
    - This project IS responsible for telling us what should be in the transaction -- not HOW to execute the transaction

---
