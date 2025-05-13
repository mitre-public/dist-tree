# Obsolete Doc Snippets

---

## Project Requirements

### Data is Stored Durably

- The `Key/Value` pairs stored in an `DistanceTree` must be stored safely. This project should be _"suitable for
  production"_ it cannot permit data loss.

### Support Large Capacity

- Reading and writing to a `DistanceTree` cannot slow to a crawl as more and more data is added to the tree.
- A `DistanceTree` should be able to store (i.e., _memorize_) a large amount of data.
- The asymptotic behavior of a `DistanceTree` must be "good". Thus, the `DistanceTree` must interact with its _data
  storage layer_ in a way that allows that component to successfully scale.

### Data Producer has no knowledge of Indexing strategy

- We cannot require the system **supplying** Key-Value pairs to also know how the KV-pairs will be indexed and
  searched. This is too much responsibility and too much coupling.
- We (will soon) use a publish-subscribe pattern in which:
    - The KV-pair **publisher** merely manufactures data
    - A _Tree Indexing Service_ acts as a **consumer** that ingests KV-pairs and produces a searchable DistanceTree.
- This requirement was only realized AFTER the pre-alpha versions of the tree were made. This requirement will be
  supported in the future...
