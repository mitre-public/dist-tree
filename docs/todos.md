## Tasks Remaining


- Figure out if Caching is being used yet.
    - Its built, but are we using/testing it


- Perform exhaustive benchmarks
  - Measure tree-build speed and tree-read speed
  - Investigate impact of branchingFactor
  - Investigate impact of repacking
  - Study how cost of DistanceMetric function can impact ideal tree shape


- RawDataStore.applyTransaction(TreeTransaction) should return some sort of future/Exception...it might fail!

---


### Eventual Tasks -- For when MVP is done

- Add support for more dynamic batch size
    - The "batches" of data we send to the DataStore should be capped by both `n` , `bytes`, & `timeSinceRequest`
    - Support `max.batch.size.count` and `max.batch.size.bytes`
    - Support `max.time.btw.batches`

- **IMPORTANT**, Add support for a secondary index that makes it fast to find the `DataPage` containing any `Tuple` we've
  already added to the Tree
    - We can power this feature using the `TimeId` generated when `put(Key, Value)` is called.
    - The `TimeId` is random and unique -- it will work great backing a sorted index
    - NOTE: this index will drastically speed up "kNN" searches for item's we've already added to the tree.
  
- **Add a Kafka Layer to "capture" incoming data**


- **Implement a `DataStore` backed by Postgres**


- **Add a "Search and Insert" method**

- **Add a "Search from known" method**
  - Can skip DistanceMetric computations and tree walk by directly looking up item
  - Problem:  Are we looking up by hash of key bytes or TimeId???  TimeId is less likely to be "on hand"
