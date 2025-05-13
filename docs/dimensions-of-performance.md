# Dimensions to Performance

Here is a list of factors that can impact performance of a DistanceTree.

- **Compute cost of executing the `DistanceMetric`**
    - This cost can vary wildly -- i.e. across several orders of magnitude!
    - When this cost is high it will be paramount to configure the tree to minimize `DistanceMetric` calls


- **Size of a `DataPage` in Tuple Count**
    - The DistanceMetric is executed once for each Tuple in the DataPage during a "page scan".
    - Thus, when DataPages contain more Tuples the `DistanceMetric` gets called more often.
    - When the I/O needed to load a `DataPage` is more expensive than executing the `DistanceMetric` this factor is
      irrelevant (Unless you are considering the "I found the right `DataPage` hit-rate")


- **Size of a `DataPage` in bytes**
    - When DataPages are large the cost of loading a `DataPage` grows (pure data transfer problem)


- **Size of `Key` type in bytes**
    - The size of Keys directly impacts the size of `NodeHeader` objects
    - Larger Keys = More expensive (in bytes) to cache NodeHeaders
    - Fewer cached NodeHeader means high chance to make an I/O call to load a `NodeHeader` we need.


- **Branching Factor**
    - **More experimentation needed.**
    - Lower Branching factor seems to reduce the number of DistanceMetric calls
    - Usefulness of a high branching factor is probably heavily influenced by how effective caching `NodeHeaders` is
    - Open Question: Is there a time when High Branching factor is extremely useful??


- **Space Overlap**
    - **More experimentation needed.**
    - Until measurements confirm its importance, "space overlap" should be considered a minor performance detail.
    - Reducing space overlap may be unnecessary because space overlap doesn't impact speed much.
    - Reducing space overlap may be unnecessary because solving it is too expensive.
    - Reducing space overlap may be useful when it is a "one-time polish dataset" operation
    - Need to measure "cost to repack a tree" vs. "Benefit to Search Query Times"


- **Batch Size & Incremental Rebuilding Policy**
    - If each batch "includes" incremental leaf node rebuilds then more batch means more leaf rebuilds
    - Thus, smaller batches (i.e. more `TreeTransactions`) will result in less space overlap and more total computation.


- **Distribution of the Dataset**
    - The `Key` data will be distributed in 1 of 2 possible ways
    - Option 1: `Keys` all come from a bounded volume. Thus, as the number of Keys grows the density of the "Key space"
      increases
    - Option 2: `Keys` come from an unbounded volume. Here, 1 or more dimensions of the Keys is unbounded. For example,
      when _Time_ is a dimension of a Key space then the amount of data with any particular time value will be capped.
      If we get more data to add to this dataset then that data will have a different time value and the overall "Key
      Volume" will increase.     
