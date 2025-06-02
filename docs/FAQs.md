# Frequently Asked Questions

(Or perhaps: Good Questions and Facts worth knowing)

### Is Tree Creation Deterministic?

- **No!**  Tree creation is not deterministic.
- The `Tuples` added to the tree are assigned `TimeId` that contain randomness.
- This randomness mean iterating through the Tuples within a DataPage yields an unpredictable order. The current
  `CenterSelector` exposes this randomness when it chooses which Keys to promote during a Node split operation.
