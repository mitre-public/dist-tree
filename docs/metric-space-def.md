
# What is a _"Metric Space"_

- A [Metric Space](https://en.wikipedia.org/wiki/Metric_space) is a _"math-nerdy"_ algebraic construct.
- **In a nutshell:**
    - If _you can measure the distance between two items_, then those items are embedded in a metric space.
- This means you can **directly define** an efficently searchable Metric Space by implementing the `DistanceMetric<K>` [interface](./src/main/java/org/mitre/disttree/DistanceMetric.java).
    - But be careful! A `DistanceMetric` that defines a metric space has a strict algebraic definition **DO NOT** get this
      wrong.
    - A `DistanceMetric` function `d(K key1, K key2)` MUST obey these rules:
        1. `d(x,y) >= 0`
        2. `d(x,y) = d(y,x)`
        3. `d(x,z) <= d(x,y) + d(y,z)`
- A `Metric Space` is essentially the _"next best thing"_ when you want to efficiently search data that has too many
  dimensions to correctly search with a 1-dimensional ordering. For example, `LatLong` data is 2-dimensional. If you
  sort by Latitude you'll sometime wish you had sorted by longitude (and vice versa). However, you can use the distance
  metric defining a Metric Space to sort by "distance between `LatLong` points". Now you can binary search using "closer
  together" and "further apart" in place of "greater than" and "less than".
