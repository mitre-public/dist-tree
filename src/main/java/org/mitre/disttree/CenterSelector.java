package org.mitre.disttree;

import java.util.List;

/**
 * A CenterSelector selects keys to use as "center points" when a node (i.e. a multidimensional
 * "spheres of data") gets "split" into multiple nodes.
 *
 * @param <K> The Key class
 */
public interface CenterSelector<K> {

    /**
     * @param keys   The List of Keys that needs to be split into two groups
     * @param metric The distance metric that measures distance between keys
     *
     * @return Keys that will be used as the centerPoints for new Nodes (this will be at least 2)
     */
    List<K> selectCenterPoints(List<K> keys, DistanceMetric<K> metric);
}
