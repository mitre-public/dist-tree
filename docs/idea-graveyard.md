# Here are thoughts, problems, and ponderings that are no longer relevant due to design progress

## Important concept users should know

- One core concept will have a **very large impact** on what users can expect from data queries.
    - (This concept may also have an impact on implementation efficiency ... time will tell)
- **Important Question:** "Is the distribution of Keys stagnant or will the distribution of Keys consistently evolve
  over time?"
    - A stagnant key distribution might be "the set of Latitude-Longitudes from aircraft in the USA". This is a mostly
      fixed universe of data.
    - A slowly evolving key distribution might be "the set of LatLongInstants" from aircraft in the USA". This key
      distribution changes over time because the data from today is always a little different from the data from
      yesterday because the time variable keeps increasing ... thus stretching the key space.
- As we get more and more data in a stagnant key distribution the "shape and size" of the key distribution won't change.
  We just have more data packed in the same geometric space. The "key space" simply gets denser and denser (which will
  impact the results from `getNClosest` and `getAllWithinRange`)
- On the other hand, as we get more and more data in an evolving key distribution the "shape" of the key distribution
  continues to change. I.e., the density of the key space may remain roughly the same while the size of the key
  space continues to grow.  (This will also impact the results from `getNClosest` and `getAllWithinRange` but in a much
  different way)
- **So what:** I am not sure how this will be relevant in the future but the efficiency of storing and querying data
  will now doubt be impacted by this difference.

### Thoughts on Binary Tree vs B-Tree

- The original `MetricTree` is a **binary search tree**.
    - This makes sense for an in-memory data structure where walking up or down the tree structure is basically
      costless (i.e. requires no IO).
- **BUT**, We know databases use B-Trees to improve performance. B-Trees reduce the number of disk seeks required to
  find a specific point in the tree (i.e. the sorted data in the database and on disk). The assumption is that
  descending one level in the tree requires an expensive disk seek. Therefore, a tree with many child nodes will be
  shallower and faster because it requires performing fewer disk seeks.
- The question is:  Should a `DurableMetricTree` be based on a Binary Search Tree or B-Tree? The answer probably depends
  on how/where the NodeHeaders are cached. (We **OBVIOUSLY** need to cache NodeHeaders to reduce IO ops). If NodeHeaders
  are cached remotely in a network-accessible cache (i.e. redis) we'll benefit from B-Tree because even cache look -ups
  require network IO. On the other hand, if we are caching NodeHeaders in-memory then the benefit of using a B-Tree
  instead of a simple binary search tree may not be very large. In fact, a B-Tree will be slower than an in-memory
  binary tree if the cost of computing a distance measurement is extremely high.
- The "smart money" is on using a high branching factor (i.e. using a B-Tree) where you can "bulk read" all NodeHeaders
  that are a child of the same parent. This is actually EXACTLY how B-Tree nodes are kept. B-Tree nodes **store multiple
  Keys in them** to correctly route queries. This means a disk-seek operation on a B-Tree returns "pointer" to
  N-different child nodes. This makes me think the `NodeHeader` could be replaced with a `NodeSet`.

### Can Probabilistic Node Promotion Improve "Big-O" behavior?

- **OBSOLETE**: Early editions of this project used an inferior strategy for incorporating new nodes into the tree. The
  old strategy would not keep the tree balanced in all cases.


- **Predicted problem**: IF the data being added to a `DistanceTree` gradually expands the MetricSpace (rather than
  fills the space more and more densely) THEN the `DistanceTree` is likely to become more and more unbalanced as the
  data being added to the tree keeps being "sent down the same path"  (WARNING, This whole argument presupposes nodes
  are often split downward)
- With very high likelihood a good way to combat "gradually worsening imbalance" is to promote nodes to higher layers
  using a probability that is "metered" by the amount of data seen.
- I suggest using random bits of each the `TimeId` assigned to each Entry. These random bits are essentially a uniform
  rand distribution. They can be used to randomly select nodes that will "be promoted" a tier.  (i.e. adopt the
  randomness used in skip lists)

### Should `BatchSize` be a part of the configuration?

- No. Not right now at least.
- Adding this feature is certainly doable. But this feature is not in the critical path.
- If this work occurs it should occur when batches are limited by both tuple count and byte size
- This feature request is 90% dead
