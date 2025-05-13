package org.mitre.disttree;

import static com.google.common.base.Preconditions.checkArgument;

import java.text.MessageFormat;

/**
 * Contains statistics that describe how well-balanced (or not) a tree is.
 *
 * @param numTuples The number of Key-Value pairs in the tree
 * @param numLeafNodes The number of leaf nodes in the tree
 * @param numInnerNodes The number of inner nodes
 * @param meanPageRadius The average radius of all leaf node (i.e. DataPages)
 * @param stdDevPageRadius The standard deviation of the radius of all leaf nodes (i.e. DataPages)
 */
public record TreeStats(
        int numTuples, int numLeafNodes, int numInnerNodes, double meanPageRadius, double stdDevPageRadius) {

    public TreeStats {
        checkArgument(numTuples > 0);
        checkArgument(numLeafNodes > 0);
        checkArgument(numInnerNodes > 0);
        checkArgument(meanPageRadius >= 0);
        checkArgument(stdDevPageRadius >= 0);
    }

    /** The total number of nodes in the tree (leaves + inner nodes. */
    public int numNodes() {
        return numLeafNodes + numInnerNodes;
    }

    /** The fraction of all nodes that are leaf nodes (perfectly balanced trees maximize this). */
    public double leafNodeFraction() {
        return numLeafNodes / (double) numNodes();
    }

    /**
     * The ratio of leaf nodes per inner node (When the tree is well-balanced this will be close to
     * the tree's branching factor).
     */
    public double leafNodesPerInnerNode() {
        return numLeafNodes / (double) numInnerNodes();
    }

    public String toString() {
        int numNodes = numLeafNodes + numInnerNodes;

        return MessageFormat.format(
                "size: {0}\n\nnumber of nodes: {1}\nnumber of leaf nodes: {2}\nnumber of inner nodes: {3}\nleaf node fraction: {4}\nmean of leaf node radius: {5}\nstandard dev of leaf node radius: {6}\n",
                numTuples, numNodes, numLeafNodes, numInnerNodes, leafNodeFraction(), meanPageRadius, stdDevPageRadius);
    }
}
