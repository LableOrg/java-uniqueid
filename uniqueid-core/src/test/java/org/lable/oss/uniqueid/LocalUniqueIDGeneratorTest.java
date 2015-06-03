package org.lable.oss.uniqueid;

import org.junit.Test;

public class LocalUniqueIDGeneratorTest {

    @Test(expected = IllegalArgumentException.class)
    public void outOfBoundsGeneratorIDTest() {
        LocalUniqueIDGenerator.generatorFor(64, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void outOfBoundsClusterIDTest() {
        LocalUniqueIDGenerator.generatorFor(0, 16);
    }

    @Test(expected = IllegalArgumentException.class)
    public void outOfBoundsGeneratorIDNegativeTest() {
        LocalUniqueIDGenerator.generatorFor(-1, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void outOfBoundsClusterIDNegativeTest() {
        LocalUniqueIDGenerator.generatorFor(0, -1);
    }
}