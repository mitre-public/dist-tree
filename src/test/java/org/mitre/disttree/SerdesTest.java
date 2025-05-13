package org.mitre.disttree;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.*;
import static org.mitre.disttree.Serdes.voidSerde;

import org.junit.jupiter.api.Test;

class SerdesTest {

    @Test
    void voidSerde_expectsNull() {

        Serde<Void> serde = voidSerde();

        // Null values are supported -- null in = null out
        assertThat(serde.fromBytes(null), nullValue());
        assertThat(serde.toBytes(null), nullValue());

        // Non-null values throw exceptions
        assertThrows(IllegalArgumentException.class, () -> serde.fromBytes(new byte[0]));
        // Cannot test voidSerde().toBytes(instanceOfVoid) because it is impossible to get an instance of void
    }
}
