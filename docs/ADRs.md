## Architecture Decision Records

- Newest ADRs on top.
- Oldest ADSs on bottom.

### Date: 2025-03-31 -- All Plans for a REST-API removed

- **Choice:** What is the "best way" for user's to integrate & adopt the DistanceTree's capabilities into production?
- **Decision:** We will focus on deploying DistanceTree database & search capabilities as a "Java library" that relies on
  local file access (i.e. the DB will be a file on disk and the library will be responsible from reading that file).
- **Context:** We've begun user testing the code-base and refining the public-facing API. We have also gained experience
  with DuckDB.
- **Reasoning:**
    - It is **highly unlikely** standing up a REST-API will be a viable near-term solution for sponsor deployment.
    - Experience with DuckDB showed the power & simplicity of "library & file on disk".
    - Reading persistent data with DuckDB only requires adding a java dependency and a "DB file".
    - The DuckDB dependency has no security issues (it has been scanned).
    - Providing read access to a "beefy DB file" using a persistent volume claim is an easy lift for our FAA deployment
    - We can cleanly separate the "how you bake the DB file" from "how you used the DB file"

### Date: 2025-01-08 -- Should we make RawDataStore's support `containsNodeAt(id)` OR track node creation vs updates

- **OBSOLETE!:** This was a temporary bridge to improving `TreeTransaction`
- **Choice:** We can make RawDataStore's support a `containsNodeAt(id)` method OR We can update the code that computes
  tree mutations so that it tracks which nodes are created vs update.
- **Decision:** Add `containsNodeAt(id)` to the RawDataStore interface
- **Context:** We need to update `TreeTransactions` to distinguish "Create" and "Update" operations. We need
  `TreeTransactions` to be more "CRUD like" so that `RawDataStores` that use a standard SQL interface have an easy time
  choosing between INSERT and UPDATE operations.
- **Reasoning:** This is choice is purely do to time limitations. Ideally, the code that tracks tree mutations and
  builds `TreeTransactions` would keep track of which Nodes are created and which Nodes were updated. We can get an MVP
  off the ground without this capability.

### Date: 2024-10-12 -- Is adding a `TimeId` to Key/Value Entries worthwhile?

- **Choice:** Is generating a `TimeId` for each `Entry<K,V>` necessary and useful?
- **Decision:** Generating `TimeIds` is 100% Useful and Worthwhile. This is a slam dunk decision.
- **Context:** When a `Key/Value` pair is added to the tree. The current implementation generates a 128-bit `TimeId` for
  this tuple.
    - Are these bits "worth it"?
    - Does having these id's support any required behaviors?
- **Reasoning:** Having TimeIds will enable 2 powerful implementation optimizations.
    - Optimization 1: Permits indexing Keys and Values (by the Entry `TimeId`). Without a "sortable key" we can't build
      a fast way to look up a particular Key or Value.
    - Optimization 2: Permits trading "variable-length" Key & Value byte[]s for fixed length values (the TimeId bits)
    - Optimization 3: Permits creating a Map or index that converts "Entry TimeId to DataPage TimeId".
    - Optimization 4: Permits storing Key/Value data ONCE while using multiple `DistanceMetrics` to generate multiple
      DistanceTrees
- **Summary** The current "data storage" implementation isn't using each Key/Value pair's `TimeId`. However, there is a
  HUGE chance a highly optimized (i.e., hard specific) data storage layer will need entries to have sortable "primary
  keys".

### Date: 2024-09-16 -- Should TreeTransactions be "Delta Encoded?"

- **Decision:** Will delay decision on "the best" TreeTransaction implementation until more tooling in place
- **Context:** TreeTransactions are "ACID transactions" that convert a tree from one state to the next (e.g. add new
  data to the tree while repacking the tree to retain balance).
- **Challenges:**
    - A TreeTransaction is likely to move many Entries from one DataPage to another.
    - We could use **Delta Encoding**: e.g., A TreeTransaction could communicate these updates by saying Entry XYZ moves
      from DataPage 123 to DataPage 456.
    - Alternatively we could use **Full Encoding**: e.g. a TreeTransaction could simply say, "Delete the old DataPages
      123 and 456". Here are complete new version of those two DataPages.
    - Delta Encoding is much more efficient when we send the transaction over the wire or write it two disk
    - Full Encoding may be easier to implement and understand.
- **Reasoning:** For now, we are ignoring this tension and this (important!) implementation detail. We will focus on
  building a CORRECT solution, then we will optimize the implementation when correctness can be rigorously tested and
  implementation speed can be rigorously measured.
- **@TODO:** Revisit this question when armed with performance benchmarks and rigorous correctness tests.

### Date: 2024-08-21 -- Building a DistanceTree from the Ground Up, with Side Splitting is Best

- **Decision:** Building a DistanceTree from the ground up is the best.
- **Context:** This project was _"on pause"_ for about a year. Upon return, the solution for "maintaining tree balance"
  and dealing with "overlapping leaf nodes" was clear.
- **Challenges:**
    - The DistanceTree needs to remain balanced or performance will devolve greatly as data load increases.
    - We need to prevent DataPages (i.e. leaf nodes) from overlapping too much otherwise we perform excess IO operations
- **Reasoning:** "Building Up" while using "Side splitting" is awesomely powerful because it delays choosing a "node
  routing key" until all data is available AND the split is needed. Additionally, adding new data _(even data from a
  completely new distribution)_ still gets good performance because additional node splits always occur where they are
  needed most. You just have to build nodes "upward" (after the base of the data pyramid exists) rather than "
  downward" (where you are anticipating future data)
- **Reasoning:** Periodically "repacking" old DataPages (i.e. leaf nodes) will drastically reduce the impact of
  overlapping leaf nodes.  "Repacking" equals deleting an old DataPage (and its entries) and reinserting those entries
  into the tree.

### Data: 2023-09-22 -- PUNT, Node Splitting is a problem

- **Decision:** None really, deciding to revisit later
- **Context:** The initial `DurableMetricTree` is working. This implementation supports both "side splitting" and "down
  splitting".
- **Challenge:** The tension between "side splitting" and "downward splitting" is easier to see
    - Read more [here](sidesplitting.md) and [here](treeBalancing.md)

### Date: 2023-08-30 -- Need three-phase processing "receiving", "indexing", "querying"

- **Decision:** We MUST split the "data ingestion" part from the "data indexing" part
- **Context:** Today, the `TreeProducer` is the only tuple point. The TreeProducer requires a DistanceMetric to create
  AND it has the `put(Entry<K,V>)` method. This is not ideal because it means data producers NEED to have an opinion
  about how the data will be indexed.
- **Challenge:** Need to support two "data creation modes"
    - Mode 1:  Simply "save Key/Value" data. Do so without an opinion on indexing data
    - Mode 2:  Full-pipeline mode that has an opinion on how data will be indexed (e.g. what the DistanceMetric is) and
      where the data will be indexed (e.g. what RawDataStore will be used)
- **Reasoning:** We should not force "data producers" to ALSO know how to index data. This is too much coupling between
  data producers and MetricTree queries

### Date: 2023-08-20 -- What changes are needed to improve durability?

- **Decision:** We need to (1) integrate a Kafka-based "data ingest system" and (2) create a standalone service that
  listens to Kafka and builds the `DurableMetricTree` in a predictable and "eventually consistent manner" from the data
  that was durably stored in a Kafka Topic
- **Context:** The pre-alpha version of `DurableMetricTree` is working. The implementation is based on local JSON files
  that contain data as 64-bit encoded binary. The "batch write op compaction" code is complete. The `TreeTransaction`
  code is complete
- **Challenge:** This project is not _suitable for production_ when we know the "tree writing" process is fragile AND
  provides no data durability guarantees
- **Reasoning:**
    - Kafka helps achieve "parallel Entry publishing"
    - Kafka helps achieve "parallel Tree creation" (i.e. same data, different DistanceMetrics)
    - Kafka helps achieve "data durability"
    - Kafka imposes a strict order on data (i.e. offsets)

### Date: 2023-01-01 -- Question should we cache Node<K,V> data, Node<byte[],byte[]> data, or both?

- **Decision:** We will only cache `Node<byte[],byte[]>` data
- **Reasoning:**
    - We **will not** cache both serialized and deserialized versions of data because it will (very probably) be a bad
      trade off. The purpose of caching is to get a **HUGE** performance gain by eliminating I/O ops. "Double caching"
      will force us to reduce our cache size and increase our cache-miss rate (for a fixed memory footprint). Therefore,
      it is probably best to have a "big cache" that contains one version of the data rather than "two small caches"
    - We **know** I/O will be the most expensive "bottleneck-y" part of this problem. Maximizing the cache for the
      expected bottleneck makes sense
    - The total computational cost of repeatedly deserializing data we store in a cache may not be impactful. But it may
      make sense to measure it.
    - The computational cost of repeatedly serializing data will be "far smaller" then cost of executing "extra" I/O ops

### Date: 2022-12-26 -- Here are the initial `DataStoreFacade` implementations plans

- **Decision:** Begin with: Individual JSON Files (one per node)
    - This is obviously terrible for a final implementation. However, it will simplify building the "Write Operation
      Compaction" back-end.
- **Decision:** Next will be contiguous binary files
    - This `DataStoreFacade` will work for local deployments.
    - The "wrinkle" here is we'll need to be able to read/write arbitrary bytes within a large file.
    - This implementation will also help us figure out "serialized byte counts" (e.g. how much data do we expect to
      store per Sphere)
- **Decision:** Last will be DynamoDB
    - This will be the most durable form of the data.

### Date: 2022-12-23 -- The initial TreeProducers implementation will be as simple as possible

- **Decision:** We will not support parallel TreeProducers writing to the same "DataStoreFacade" at the same time
- **Decision:** We will relay on "external data ordering" (i.e. soft assumption that we are downstream of a Kafka
  partition)

### Date: 2022-12-22 -- Should the "Incoming data capture buffer" provide a durability guarantee?

- **Decision:** For now, we are accepting the risk of data loss if a crash occurs while data is "waiting for
  compaction". Incoming data will **only** be stored in an in-memory queue. (More correctly, we aren't accepting data
  lose, we are forcing the user to retry data upload requests that fail)
- **Context:** `TreeProducer` is being written for the very first time
- **Challenge:** We know achieving efficient IO requires putting incoming data in a buffer so that _"Write Operation
  Compaction"_ can improve write efficiency. But, this feels like a vulnerability because incoming data could get lost
  if the `TreeProducer` process is killed.
- **Reasoning:**
    1. Fact, No matter what, the `TreeProducer` always needs to return an asynchronous `Future` object that say "your
       write succeeded/failed"
    2. Option 1: We provide the "Write Successful Future" when incoming data was accepted but not yet written to the
       final datastore.
        - **Upside:** We give faster feedback.
        - **Downside:** The "upload successful" message provides less information because the data have only made it to
          the "data capture buffer" and NOT the final "data store". In other words, the data may not be readable.
        - **Downside:** Implementing the `TreeProducer` will be harder because when we restart it we'll need to check
          for "data capture buffer orphans" (from prior crashes).
    3. Option 2: We provide the "Write Successful Future" only after the incoming data is compacted and written to the
       final datastore
        - **Upside:** Implementation is easier
        - **Upside:** The "upload successful" message provides precise information about what happened (i.e. the data is
          now viewable on the read side)
        - **Downside:** The "upload successful" message takes longer to receive
