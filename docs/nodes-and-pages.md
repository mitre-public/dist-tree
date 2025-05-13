# Nodes and Pages
- A `DistanceTree` is a combination of a [B-Tree](https://en.wikipedia.org/wiki/B-tree) and
  an [M-Tree](https://en.wikipedia.org/wiki/M-tree)
- **B-Tree Inspired Properties:**
  - Like B-Trees, inner nodes of an `DistanceTree` usually have many child nodes (i.e. high fan out).
  - Like B-Trees, leaf nodes of an `DistanceTree` contains the data (i.e. `Tuple` records) that are "close together".
  - There is exactly one `NodeHeader` for each **node**
  - There is exactly one `DataPage` for each **leaf node**
- **M-Tree Inspired Properties:**
  - Like M-Trees, all nodes maintain a _"center point"_ and a _"radius"_
- A `DistanceTree` uses a _"data storage layer"_ to store `NodeHeader` and `DataPage` objects.
  - This _"data storage layer"_ is responsible for durability.
- Changing an `DistanceTree` requires executing a `TreeTransaction`. A `TreeTransaction` provides a changeset that must be
  either 100% successful or 100% rejected. Partial changes will leave the tree malformed.
- The _"data storage layer"_ is responsible for ensuring the changes occur in an ACID compliant manner.

---

## How do we store a DistanceTree in an arbitrary Data Storage Layer?

Every `DistanceTree` is built from `NodeHeaders` and `DataPages`. These are the objects we must read and write to the _data
storage layer_. Let's look at them more closely.


---

### The `NodeHeader` record

- Every `NodeHeader` has the following fields:
    - `TimeId id` = A globally unique id value for this NodeHeader
    - `TimeId parent` = The id of this Node's parent
    - `K center` = The center of this Node's hypersphere
    - `double radius` = The radius of this Node's hypersphere
    - `List<TimeId> childNodes` = The id of all child nodes (only applies to inner nodes)
    - `int numTuples` = The number of `Tuple<K,V>` stored in the `DataPage` (only applies to leaf nodes)

`NodeHeader` records are designed to have a small memory footprint, so they can be cached aggressively without
straining memory. The more NodeHeaders we can store in memory the fewer I/O operations we will need to perform

**The `NodeHeader` for an inner node**

```
{
  "id": "ZG5QmJ8nSVgcAMhjny1L-g",
  "parentId": "ZG5QmJ8axZch8pjJmplwTQ",
  "base64Center": "a2V5",
  "radius": 0.5,
  "childNodeIds": [
    "ZG5QmJ800lgCZquH-Ya7NA",
    "ZG5QmJ8tTBeyDJq2mH9lcw"
  ],
  "numTuples": 0
}
```

**The `NodeHeader` for a leaf node**

```
{
  "id": "ZG5QBdfob1vUVEnpwn98xQ",
  "parentId": "ZG5QBdghygMkcsms5sYnJQ",
  "base64Center": "a2V5",
  "radius": 0.5,
  "numTuples": 3
}
```

---

### The `DataPage` record

- Every `DataPage` has the following fields:
    - `TimeId id` = A globally unique id value for this DataPage. This id will match the id of exactly one NodeHeader
    - `Set<Tuples<K, V>> tuples` = The _bucket of data_ found at this leaf node.
- There is exactly one `DataPage` for each leaf node in the tree
- Accessing a `DataPage` typically requires executing an I/O operation (unless a cache is in play).
- Any query that needs data within a `DataPage` will need to load ALL the data contained in the leaf node. This
  all-or-nothing behavior is a side effect of working with multidimensional data.

**An example `DataPage`**

- Notice, all Tuple data is stored as byte[]s. In this JSON example the byte[] are encoded as base64 Strings

```
{
  "id": "ZG5P2VsilmQZh3tLBOVS4w",
  "entries": [
    {
      "id": "ZG5P2VsqU5IRGeWr9j8T4Q",
      "key": "a2V5MQ",
      "value": "dmFsdWUx"
    },
    {
      "id": "ZG5P2Vrrs-zvyuxn76qZDw",
      "key": "a2V5MA",
      "value": "dmFsdWUw"
    },
    {
      "id": "ZG5P2VsYLMom5_xMWjml_w",
      "key": "a2V5Mg",
      "value": "dmFsdWUy"
    }
  ]
}
```

---

## Storing Nodes and Tuples in DB Tables!

**Fact:** Node and Tuple data can be stored in regular database tables!

### The Tuple Table

This example database table contains all the Tuples.

**Tuple Table:**

| TupleId | PageId  | Key Bytes            | Value Bytes          |
|:--------|---------|----------------------|----------------------|
| 128bits | 128bits | blob (or base64 str) | blob (or base64 str) |
| 128bits | 128bits | blob (or base64 str) | blob (or base64 str) |
| 128bits | 128bits | blob (or base64 str) | blob (or base64 str) |
| 128bits | 128bits | blob (or base64 str) | blob (or base64 str) |
| 128bits | 128bits | blob (or base64 str) | blob (or base64 str) |

### The Node Table

This example database table contains all the "Node Links"

**Node Table:**

| NodeId  | ParentId | Center Key Bytes     | Radius | numTuples | child0  | ... | childN  |
|---------|----------|----------------------|--------|-----------|---------|-----|---------|
| 128bits | 128bits  | blob (or base64 str) | double | int       | 128bits | ... | 128bits |
| 128bits | 128bits  | blob (or base64 str) | double | int       | 128bits | ... | 128bits |
| 128bits | 128bits  | blob (or base64 str) | double | int       | 128bits | ... | 128bits |


