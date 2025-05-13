package org.mitre.disttree;

import static com.google.common.collect.Lists.newLinkedList;

import java.util.LinkedList;

import org.mitre.caasd.commons.ids.TimeId;

public class Mermaid {

    /**
     * Create the "Markdown String" that can render this Tree as a Mermaid Graphic.  Unfortunately,
     * these graphics are usually too small because the trees are so wide.
     *
     * @param tree A Tree
     *
     * @return A Markdown Mermaid String that represents the nodes in this tree.
     */
    static <K, V> String asMermaidGraphic(InternalTree<K, V> tree) {

        LinkedList<NodeHeader<K>> nodesToExplore = newLinkedList();
        LinkedList<NodeHeader<K>> nextTier = newLinkedList();

        nodesToExplore.add(tree.rootNode());

        StringBuilder mermaidText = new StringBuilder("```mermaid\nflowchart TD\n");

        while (!nodesToExplore.isEmpty()) {
            NodeHeader<K> current = nodesToExplore.removeFirst();

            if (current.isLeafNode()) {
                continue;
            }

            // We have an inner node ...
            String thisNode = asNodeName(current.id());
            for (TimeId id : current.childNodes()) {

                String childNodeName = tree.nodeAt(id).isInnerNode()
                        ? asNodeName(id)
                        // Want something like: "aHBHw --> 5bYXA(5bYXA\nNUM_TUPLES)"
                        : asNodeName(id) + "(" + asNodeName(id) + "\n"
                                + tree.nodeAt(id).numTuples() + ")";

                mermaidText.append("    " + thisNode + " --> " + childNodeName + "\n");
                nextTier.add(tree.nodeAt(id));
            }

            if (nodesToExplore.isEmpty()) {
                nodesToExplore.addAll(nextTier);
                nextTier.clear();
            }
        }
        mermaidText.append("```");

        return mermaidText.toString();
    }

    private static String asNodeName(TimeId id) {

        int numLetters = 5;

        String base64 = id.asBase64();
        int n = base64.length();
        return base64.substring(n - numLetters, n)
                .replace('-', 'f'); // mermaid won't support nodes with '-' in the name
    }
}
