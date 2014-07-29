package org.lable.util.uniqueid;

import org.apache.commons.codec.binary.Hex;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;

public class BaseUniqueIDGeneratorTest {

    /*
     * Byte mangling tests.
     */

    @Test
    public void mangleBytesZero() {
        final byte[] result = BaseUniqueIDGenerator.mangleBytes(new BaseUniqueIDGenerator.Blueprint(0, 0, 0, 0));
        final byte[] zero = new byte[8];

        // Baseline check, if all ID parts are zero so is the result.
        assertThat(result, is(zero));
    }

    @Test
    public void mangleBytesMostlyOnes() {
        final byte[] result = BaseUniqueIDGenerator.mangleBytes(new BaseUniqueIDGenerator.Blueprint(
                BaseUniqueIDGenerator.MAX_TIMESTAMP,
                BaseUniqueIDGenerator.MAX_SEQUENCE_COUNTER,
                BaseUniqueIDGenerator.MAX_GENERATOR_ID,
                BaseUniqueIDGenerator.MAX_CLUSTER_ID
        ));
        // The "03" for the 7th byte is due to the reserved bits that are (for now) always zero.
        final String expected = "ffffffffffff03ff";

        // Baseline check, if all ID parts are all ones so is the result (except for the reserved bytes).
        assertThat(Hex.encodeHexString(result), is(expected));
    }

    @Test
    public void mangleBytesTimestampOnly() {
        final long TEST_TS_A = 143062936275L;
        // This is the above long with its bytes reversed.
        final String TEST_A_REVERSED = "cb54ecf284000000";

        // Timestamp test.
        final byte[] result_a = BaseUniqueIDGenerator.mangleBytes(new BaseUniqueIDGenerator.Blueprint(TEST_TS_A, 0, 0, 0));
        assertThat(Hex.encodeHexString(result_a).toLowerCase(), is(TEST_A_REVERSED));

        final long TEST_TS_B = 0x3FFFFFFFDL;
        // This is the above long with its bytes reversed.
        final String TEST_B_REVERSED = "bfffffffc0000000";

        // Timestamp test.
        final byte[] result_b = BaseUniqueIDGenerator.mangleBytes(new BaseUniqueIDGenerator.Blueprint(TEST_TS_B, 0, 0, 0));
        assertThat(Hex.encodeHexString(result_b).toLowerCase(), is(TEST_B_REVERSED));
    }

    @Test
    public void mangleBytesSequenceCounterOnly() {
        // Sequence counter test.
        final byte[] result = BaseUniqueIDGenerator.mangleBytes(new BaseUniqueIDGenerator.Blueprint(0, 0x22, 0, 0));
        final byte[] sixthByte = new byte[]{result[5]};
        // 0x88 is 0x22 shifted left two bits.
        final String expected = "22";
        assertThat(Hex.encodeHexString(sixthByte), is(expected));
    }

    @Test
    public void mangleBytesGeneratorIdOnly() {
        // Generator ID test.
        final byte[] result = BaseUniqueIDGenerator.mangleBytes(new BaseUniqueIDGenerator.Blueprint(0, 0, 0x27, 0));
        final byte[] lastTwoBytes = new byte[]{result[6], result[7]};
        // 0x0270 is 0x0027 shifted left four bits.
        final String expected = "0270";
        assertThat(Hex.encodeHexString(lastTwoBytes), is(expected));
    }

    @Test
    public void mangleBytesClusterIdOnly() {
        // Cluster ID test.
        final byte[] result = BaseUniqueIDGenerator.mangleBytes(new BaseUniqueIDGenerator.Blueprint(0, 0, 0, 5));
        final byte[] lastTwoBytes = new byte[]{result[6], result[7]};
        final String expected = "0005";
        assertThat(Hex.encodeHexString(lastTwoBytes), is(expected));
    }

    @Test
    public void unmangleBytes() {
        // Create an ID, then un-mangle it, and run the resulting blueprint through the mangler again.
        final long TEST_TS = 143062936275L;
        final byte[] result_one = BaseUniqueIDGenerator.mangleBytes(new BaseUniqueIDGenerator.Blueprint(TEST_TS, 10, 1, 5));
        BaseUniqueIDGenerator.Blueprint blueprint = BaseUniqueIDGenerator.parse(result_one);
        final byte[] result_two = BaseUniqueIDGenerator.mangleBytes(blueprint);
        assertThat(result_one, is(result_two));
    }

    @Test
    public void blueprint() {
        // Round-trip test. First generate the byte[] with mangleBytes, then back to the blueprint with Blueprint.parse.

        final long TEST_TS = 143062936275L;
        final byte[] resultOne = BaseUniqueIDGenerator.mangleBytes(
                new BaseUniqueIDGenerator.Blueprint(TEST_TS, 10, 1, 5));
        final BaseUniqueIDGenerator.Blueprint blueprint_one = BaseUniqueIDGenerator.parse(resultOne);
        assertThat(resultOne, is(blueprint_one.getID()));

        final byte[] resultZeros = BaseUniqueIDGenerator.mangleBytes(
                new BaseUniqueIDGenerator.Blueprint(0, 0, 0, 0));
        final BaseUniqueIDGenerator.Blueprint blueprintZeros = BaseUniqueIDGenerator.parse(resultZeros);
        assertThat(resultZeros, is(blueprintZeros.getID()));

        final byte[] resultMostlyOnes = BaseUniqueIDGenerator.mangleBytes(new BaseUniqueIDGenerator.Blueprint(
                BaseUniqueIDGenerator.MAX_TIMESTAMP,
                BaseUniqueIDGenerator.MAX_SEQUENCE_COUNTER,
                BaseUniqueIDGenerator.MAX_GENERATOR_ID,
                BaseUniqueIDGenerator.MAX_CLUSTER_ID
        ));
        final BaseUniqueIDGenerator.Blueprint blueprintMostlyOnes = BaseUniqueIDGenerator.parse(resultMostlyOnes);
        assertThat(resultMostlyOnes, is(blueprintMostlyOnes.getID()));
    }

    /*
     * Other tests.
     */

    @Test
    public void blueprintToStringTest() {
        BaseUniqueIDGenerator.Blueprint blueprint =
                new BaseUniqueIDGenerator.Blueprint(System.currentTimeMillis(), 0, 0, 0);
        assertThat(blueprint.toString(), is(notNullValue()));
        assertThat(blueprint.toString().length(), is(not(0)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void blueprintParseIllegalArgument() {
        BaseUniqueIDGenerator.parse(new byte[0]);
    }
}