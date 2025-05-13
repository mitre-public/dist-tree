# Future Optimizations

This is a list of implementation performance optimization that can be worked on when time permits

### Multi-way Splitting

- **Context:** When a leaf node gets "too big" they get split. Currently, the data in a leaf nodes is split into exactly
  2 leaf nodes. There is a strong likelihood splitting leaf nodes into 3+ nodes will be more performant in the long run.
  We conjecture there will be performance improvements on the "query side" because the resulting tree will have smaller
  leaves on average. There should also be performance improvements during index time because there should be less _"
  DataPage churn"_ during indexing.

### Repacking the oldest Leaf

- **Context:** We want to "repack" as we go to keep (1) amortize the cost of repacking and (2) ensure Search operations
  are always efficient. This means we need to detect "poorly packed" leaf nodes as the tree is growing. One strategy to
  do this (in addition to repacking nodes with large radius values) is to repack the OLDEST leaf node. This node will
  have had the largest opportunity to capture data that would no longer be routed to it. This strategy has the benefit
  of being able to patch areas of the tree that are not in the "sibling set" of a newly created node. The difficulty (or
  maybe simply annoyance) is that we need to "replace" old Node IDs with newer NodeIDs

### Caching

- **Context:** `CachingRawDataStore` exists. It needs to be integrated as part of a performance benchmarking plan. In
  other words, we should count how many times `DistanceMetric.distanceBtw`, `DistanceTree.nodeAt`, and `DistanceTree.pageAt` are
  called before and after integrating the cache.

### Support Multiple Indexes

- **Context:** If I have a large data set I may want to search it with multiple query strategies. For example, If I have
  a dataset with Location & Time and may want to index/search the data by "location and time" as well as "just
  location".
- This is a stretch goal