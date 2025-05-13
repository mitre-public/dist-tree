package org.mitre.disttree;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.unmodifiableList;
import static org.mitre.caasd.commons.maps.MapBuilder.newMapBuilder;
import static org.mitre.disttree.Batch.batchify;
import static org.mitre.disttree.MiscTestUtils.randomColor;
import static org.mitre.disttree.MiscTestUtils.randomLatLong;
import static org.mitre.disttree.Serdes.latLongSerde;
import static org.mitre.disttree.Serdes.stringUtf8Serde;
import static org.mitre.disttree.SharedTestUtils.*;
import static org.mitre.disttree.stores.DataStores.inMemoryStore;

import java.awt.Color;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.mitre.caasd.commons.Course;
import org.mitre.caasd.commons.Distance;
import org.mitre.caasd.commons.LatLong;
import org.mitre.caasd.commons.ids.TimeId;
import org.mitre.caasd.commons.maps.MapFeature;
import org.mitre.caasd.commons.maps.MapFeatures;
import org.mitre.caasd.commons.maps.MonochromeTileServer;
import org.mitre.caasd.commons.maps.TileServer;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * These unit tests make maps of the data to help us illustrate what a MetricTree does.
 */
public class MapBasedTest {

    // Memorize the color each TimeId is assigned to, makes graphic evolution easier to see.
    static final Map<TimeId, Color> COLOR_MEMORY = new TreeMap<>();

    static Color colorOf(TimeId id) {
        return COLOR_MEMORY.computeIfAbsent(id, (TimeId meh) -> randomColor());
    }

    @Disabled // We don't need to make these pictures
    @Test
    public void makeSomeMaps() {

        int branchingFactor = 40;
        int maxTuplesPerPage = 500;
        int size = 500_000;
        var tree = makeTree(branchingFactor, maxTuplesPerPage);

        // add some data to the Tree
        List<Tuple<LatLong, String>> testData = createTestData(size);

        addTestDataToTree(tree, testData);

        verifyTree(testData, tree);

        List<DataPage<LatLong, String>> allPages = tree.allDataPages();

        TileServer tileSource = new MonochromeTileServer(Color.BLACK); // For fast Offline use
        //        TileServer tileSource = new MapBoxApi(MapBoxApi.Style.DARK);  //For presentations & graphics

        newMapBuilder()
                .tileSource(tileSource)
                .center(LatLong.of(0.0, 0.0)) // the center of the random distribution we are drawing LatLongs from
                .width(3200, 6)
                .addFeatures(allPages, es -> asMapFeature(es))
                .toFile(new File(
                        "multiColorDots" + branchingFactor + "_ " + maxTuplesPerPage + "_" + (size / 1000) + ".png"));
    }

    @Disabled // We don't need to make these pictures
    @Test
    public void makeBatchByBatchMaps() {

        int branchingFactor = 2;
        int maxTuplesPerPage = 250;

        int SIZE_OF_TEST_SIZE = 100_000;
        int BATCH_SIZE = 500;

        // add some data to the Tree
        List<Tuple<LatLong, String>> testData = createTestData(SIZE_OF_TEST_SIZE);

        List<Batch<LatLong, String>> batches = batchify(testData, BATCH_SIZE);
        List<Tuple<LatLong, String>> dataSoFar = new ArrayList<>(SIZE_OF_TEST_SIZE);

        TreeConfig<LatLong, String> config = TreeConfig.<LatLong, String>builder()
                .maxTuplesPerPage(maxTuplesPerPage)
                .distMetric((a, b) -> a.distanceTo(b).inNauticalMiles())
                .dataStore(inMemoryStore())
                .keySerde(latLongSerde())
                .valueSerde(stringUtf8Serde())
                .build();

        var tree = new InternalTree<>(config);
        DistanceTree<LatLong, String> facade = new DistanceTree<>(tree);

        TileServer tileSource = new MonochromeTileServer(Color.BLACK); // For fast Offline use

        for (Batch<LatLong, String> batch : batches) {
            facade.addBatch(batch);

            dataSoFar.addAll(batch.tuples());

            if (dataSoFar.size() % 10_000 != 0) {
                continue;
            }

            newMapBuilder()
                    .tileSource(tileSource)
                    .center(LatLong.of(0.0, 0.0)) // the center of the random distribution we are drawing LatLongs from
                    .width(3200, 5)
                    .addFeatures(tree.allDataPages(), pages -> asMapFeature(pages))
                    .toFile(new File("tree_bf" + branchingFactor + "_mtpp" + maxTuplesPerPage + "_batch" + BATCH_SIZE
                            + "_" + dataSoFar.size() + ".png"));
        }

        verifyTree(dataSoFar, tree);

        // Work to make one final "nice" image
        facade.repackTree();
        facade.repackTree();
        facade.repackTree();
        facade.repackTree();
        facade.repackTree();
        facade.repackTree();
        facade.repackTree();

        newMapBuilder()
                .tileSource(tileSource)
                .center(LatLong.of(0.0, 0.0)) // the center of the random distribution we are drawing LatLongs from
                .width(3200, 5)
                .addFeatures(tree.allDataPages(), pages -> asMapFeature(pages))
                .toFile(new File("tree_bf" + branchingFactor + "_mtpp" + maxTuplesPerPage + "_batch" + BATCH_SIZE + "_"
                        + dataSoFar.size() + "_repacked.png"));
    }

    @Disabled // We don't need to make these pictures
    @Test
    public void makeBatchByBatchBiModalMaps() {

        int branchingFactor = 2;
        int maxTuplesPerPage = 250;

        int SIZE_OF_TEST_SIZE = 100_000;
        int BATCH_SIZE = 500;

        // add some data to the Tree
        List<Tuple<LatLong, String>> biModalTestData = createBiModalTestData(SIZE_OF_TEST_SIZE);

        List<Batch<LatLong, String>> batches = batchify(biModalTestData, BATCH_SIZE);
        List<Tuple<LatLong, String>> dataSoFar = new ArrayList<>(SIZE_OF_TEST_SIZE);

        TreeConfig<LatLong, String> config = TreeConfig.<LatLong, String>builder()
                .maxTuplesPerPage(maxTuplesPerPage)
                .distMetric((a, b) -> a.distanceTo(b).inNauticalMiles())
                .dataStore(inMemoryStore())
                .keySerde(latLongSerde())
                .valueSerde(stringUtf8Serde())
                .build();

        var tree = new InternalTree<>(config);
        DistanceTree<LatLong, String> facade = new DistanceTree<>(tree);

        TileServer tileSource = new MonochromeTileServer(Color.BLACK); // For fast Offline use

        LatLong center = LatLong.of(0.0, 0.0).project(Course.ofDegrees(45), Distance.ofNauticalMiles(700));

        for (Batch<LatLong, String> batch : batches) {
            facade.addBatch(batch);

            dataSoFar.addAll(batch.tuples());

            if (dataSoFar.size() % 10_000 != 0) {
                continue;
            }

            newMapBuilder()
                    .tileSource(tileSource)
                    .center(center) // the center of the random distribution we are drawing LatLongs from
                    .width(3200, 5)
                    .addFeatures(tree.allDataPages(), pages -> asMapFeature(pages))
                    .toFile(new File("tree_bf" + branchingFactor + "_mtpp" + maxTuplesPerPage + "_batch" + BATCH_SIZE
                            + "_" + dataSoFar.size() + ".png"));
        }

        verifyTree(dataSoFar, tree);

        // Work to make one final "nice" image
        facade.repackTree();
        facade.repackTree();
        facade.repackTree();
        facade.repackTree();
        facade.repackTree();
        facade.repackTree();
        facade.repackTree();

        newMapBuilder()
                .tileSource(tileSource)
                .center(center)
                .width(3200, 5)
                .addFeatures(tree.allDataPages(), pages -> asMapFeature(pages))
                .toFile(new File("tree_bf" + branchingFactor + "_mtpp" + maxTuplesPerPage + "_batch" + BATCH_SIZE + "_"
                        + dataSoFar.size() + "_repacked.png"));
    }

    @Test
    public void showRangeQueryMap() {

        /*
         * This test/demo shows how "easy" it is to spin up a query engine to search a pre-built tree
         */
        InternalTree<LatLong, String> tree = makeTree(64, 50);
        List<Tuple<LatLong, String>> testData = unmodifiableList(createTestData(5_000));
        addTestDataToTree(tree, testData);

        // Code snippet where you query a Tree starts here...=
        TreeSearcher<LatLong, String> searcher = new TreeSearcher<>(tree);

        LatLong queryPoint = randomLatLong();

        SearchResults<LatLong, String> result = searcher.getAllWithinRange(queryPoint, 200);

        TileServer tileSource = new MonochromeTileServer(Color.BLACK); // For fast Offline use

        newMapBuilder()
                .tileSource(tileSource)
                .center(LatLong.of(0.0, 0.0)) // the center of the random distribution we are drawing LatLongs from
                .width(1200, 5)
                .addFeatures(testData, tuple -> MapFeatures.filledCircle(tuple.key(), Color.RED, 10))
                .addFeatures(result.tuples(), tuple -> MapFeatures.filledCircle(tuple.key(), Color.BLUE, 10))
                .addFeatures(MapFeatures.filledCircle(queryPoint, Color.MAGENTA, 14))
                .toImage();
        //            .toFile(new File("rangeQuery.png"));
    }

    @Test
    public void showKnnQueryMap() {

        /*
         * This test/demo shows how "easy" it is to spin up a query engine to search a pre-built tree
         */
        InternalTree<LatLong, String> tree = makeTree(64, 50);
        List<Tuple<LatLong, String>> testData = unmodifiableList(createTestData(1_000));
        addTestDataToTree(tree, testData);

        // Code snippet where you query a Tree starts here...=
        TreeSearcher<LatLong, String> searcher = new TreeSearcher<>(tree);

        LatLong queryPoint = randomLatLong();

        SearchResults<LatLong, String> result = searcher.getNClosest(queryPoint, 10);

        TileServer tileSource = new MonochromeTileServer(Color.BLACK); // For fast Offline use

        newMapBuilder()
                .tileSource(tileSource)
                .center(LatLong.of(0.0, 0.0)) // the center of the random distribution we are drawing LatLongs from
                .width(1200, 5)
                .addFeatures(testData, tuple -> MapFeatures.filledCircle(tuple.key(), Color.RED, 10))
                .addFeatures(result.tuples(), tuple -> MapFeatures.filledCircle(tuple.key(), Color.CYAN, 10))
                .addFeatures(MapFeatures.filledCircle(queryPoint, Color.MAGENTA, 14))
                .toImage();
        //            .toFile(new File("knnQuery.png"));
    }

    private MapFeature asMapFeature(DataPage<LatLong, String> dataPage) {

        Color color = colorOf(dataPage.id());

        return MapFeatures.path(dataPage.keyList(), color, 3.0f);
    }

    // @todo -- Move this code to commons -- blending colors is annoying...but common
    private static Color blend(Color colorAtZero, double fraction, Color colorAtOne) {
        checkArgument(0 <= fraction && fraction <= 1.0);

        int r = (int) (colorAtZero.getRed() * (1.0 - fraction) + colorAtOne.getRed() * fraction);
        int g = (int) (colorAtZero.getGreen() * (1.0 - fraction) + colorAtOne.getGreen() * fraction);
        int b = (int) (colorAtZero.getBlue() * (1.0 - fraction) + colorAtOne.getBlue() * fraction);
        int a = (int) (colorAtZero.getAlpha() * (1.0 - fraction) + colorAtOne.getAlpha() * fraction);

        return new Color(r, g, b, a);
    }

    InternalTree<LatLong, String> makeTree(int branchingFactor, int tuplesPerPage) {

        TreeConfig<LatLong, String> config = TreeConfig.<LatLong, String>builder()
                .branchingFactor(branchingFactor)
                .maxTuplesPerPage(tuplesPerPage)
                .distMetric((a, b) -> a.distanceTo(b).inNauticalMiles())
                .dataStore(inMemoryStore())
                .keySerde(latLongSerde())
                .valueSerde(stringUtf8Serde())
                .build();

        return new InternalTree<>(config);
    }
}
