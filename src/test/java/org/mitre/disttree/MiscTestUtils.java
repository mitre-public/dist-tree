package org.mitre.disttree;

import java.awt.Color;
import java.util.Random;

import org.mitre.caasd.commons.Course;
import org.mitre.caasd.commons.Distance;
import org.mitre.caasd.commons.LatLong;

public class MiscTestUtils {

    public static final Random RNG = new Random(17L);

    public static Tuple<LatLong, byte[]> newRandomDatum() {
        return Tuple.newTuple(randomLatLong(), randomBytes());
    }

    public static LatLong randomLatLong() {
        double c = 5;
        return LatLong.of(c * RNG.nextGaussian(), c * RNG.nextGaussian());
    }

    public static LatLong randomBiModalLatLong() {

        LatLong sample = randomLatLong();
        double DEGREES = 45;
        double MILES_TO_MOVE = 1400;

        if (RNG.nextBoolean()) {
            return sample;
        } else {
            return sample.project(Course.ofDegrees(DEGREES), Distance.ofNauticalMiles(MILES_TO_MOVE));
        }
    }

    public static byte[] randomBytes() {
        byte[] fillMe = new byte[32];
        RNG.nextBytes(fillMe);
        return fillMe;
    }

    public static Color randomColor() {
        //        return new Color(RNG.nextInt(255), RNG.nextInt(255), RNG.nextInt(255), 123);
        return new Color(RNG.nextInt(255), RNG.nextInt(255), RNG.nextInt(255));
    }
}
