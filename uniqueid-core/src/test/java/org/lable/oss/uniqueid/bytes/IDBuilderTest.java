/*
 * Copyright (C) 2014 Lable (info@lable.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lable.oss.uniqueid.bytes;

import org.apache.commons.codec.binary.Hex;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.lable.oss.uniqueid.BaseUniqueIDGenerator;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.lable.oss.uniqueid.bytes.IDBuilder.parseGeneratorId;

public class IDBuilderTest {
    @Test
    public void buildZero() {
        final byte[] result = IDBuilder.build(new Blueprint(0, 0, 0, 0));
        final byte[] zero = new byte[8];

        // Baseline check, if all ID parts are zero so is the result.
        assertThat(result, CoreMatchers.is(zero));
    }

    @Test
    public void buildMostlyOnes() {
        final byte[] result = IDBuilder.build(new Blueprint(
                Blueprint.MAX_TIMESTAMP,
                Blueprint.MAX_SEQUENCE_COUNTER,
                Blueprint.MAX_GENERATOR_ID,
                Blueprint.MAX_CLUSTER_ID
        ));
        // The "0f" for the 7th byte is due to the reserved bits that are (for now) always zero.
        final String expected = "ffffffffffff0fff";

        // Baseline check, if all ID parts are all ones so is the result (except for the reserved bytes).
        assertThat(Hex.encodeHexString(result), CoreMatchers.is(expected));
    }

    @Test
    public void buildTimestampOnly() {
        final long TEST_TS_A = 143062936275L;
        // This is the above long with its bytes reversed.
        final String TEST_A_REVERSED = "cb54ecf284000000";

        // Timestamp test.
        final byte[] result_a = IDBuilder.build(new Blueprint(TEST_TS_A, 0, 0, 0));
        assertThat(Hex.encodeHexString(result_a).toLowerCase(), CoreMatchers.is(TEST_A_REVERSED));

        final long TEST_TS_B = 0x3FFFFFFFDL;
        // This is the above long with its bytes reversed.
        final String TEST_B_REVERSED = "bfffffffc0000000";

        // Timestamp test.
        final byte[] result_b = IDBuilder.build(new Blueprint(TEST_TS_B, 0, 0, 0));
        assertThat(Hex.encodeHexString(result_b).toLowerCase(), CoreMatchers.is(TEST_B_REVERSED));
    }

    @Test
    public void buildSequenceCounterOnly() {
        // Sequence counter test.
        final byte[] result = IDBuilder.build(new Blueprint(0, 0x22, 0, 0));
        final byte[] sixthByte = new byte[]{result[5]};
        // 0x88 is 0x22 shifted left two bits.
        final String expected = "22";
        assertThat(Hex.encodeHexString(sixthByte), CoreMatchers.is(expected));
    }

    @Test
    public void buildGeneratorIdOnly() {
        // Generator ID test.
        final byte[] result = IDBuilder.build(new Blueprint(0, 0, 0x27, 0));
        final byte[] lastTwoBytes = new byte[]{result[6], result[7]};
        // 0x0270 is 0x0027 shifted left four bits.
        final String expected = "0270";
        assertThat(Hex.encodeHexString(lastTwoBytes), CoreMatchers.is(expected));
    }

    @Test
    public void buildClusterIdOnly() {
        // Cluster ID test.
        final byte[] result = IDBuilder.build(new Blueprint(0, 0, 0, 5));
        final byte[] lastTwoBytes = new byte[]{result[6], result[7]};
        final String expected = "0005";
        assertThat(Hex.encodeHexString(lastTwoBytes), CoreMatchers.is(expected));
    }

    @Test
    public void parseBytes() {
        // Create an ID, then un-mangle it, and run the resulting blueprint through the mangler again.
        final long TEST_TS = 143062936275L;
        final byte[] resultOne = IDBuilder.build(new Blueprint(TEST_TS, 10, 1, 5));

        assertThat(IDBuilder.parseGeneratorId(resultOne), is(1));
        assertThat(IDBuilder.parseClusterId(resultOne), is(5));
        assertThat(IDBuilder.parseSequenceId(resultOne), is(10));
        assertThat(IDBuilder.parseTimestamp(resultOne), is(TEST_TS));

        Blueprint blueprint = IDBuilder.parse(resultOne);

        final byte[] result_two = IDBuilder.build(blueprint);
        assertThat(resultOne, CoreMatchers.is(result_two));
    }

    @Test
    public void blueprint() {
        // Round-trip test. First generate the byte[] with mangleBytes, then back to the blueprint with Blueprint.parse.

        final long TEST_TS = 143062936275L;
        final byte[] resultOne = IDBuilder.build(new Blueprint(TEST_TS, 10, 1, 5));
        final Blueprint blueprintOne = IDBuilder.parse(resultOne);
        final byte[] resultOneAgain = IDBuilder.build(blueprintOne);
        assertThat(resultOne, CoreMatchers.is(resultOneAgain));

        final byte[] resultZeros = IDBuilder.build(new Blueprint(0, 0, 0, 0));
        final Blueprint blueprintZeros = IDBuilder.parse(resultZeros);
        final byte[] resultZerosAgain = IDBuilder.build(blueprintZeros);
        assertThat(resultZeros, CoreMatchers.is(resultZerosAgain));

        final byte[] resultMostlyOnes = IDBuilder.build(new Blueprint(
                Blueprint.MAX_TIMESTAMP,
                Blueprint.MAX_SEQUENCE_COUNTER,
                Blueprint.MAX_GENERATOR_ID,
                Blueprint.MAX_CLUSTER_ID
        ));
        final Blueprint blueprintMostlyOnes = IDBuilder.parse(resultMostlyOnes);
        final byte[] resultMostlyOnesAgain = IDBuilder.build(blueprintMostlyOnes);
        assertThat(resultMostlyOnes, CoreMatchers.is(resultMostlyOnesAgain));
    }

    @Test
    public void parseGeneratorIdTest() {
        byte[] id = new byte[8];
        id[6] = 0x0f;
        id[7] = (byte) (0x0f << 4);

        byte[] clone = id.clone();

        assertThat(parseGeneratorId(id), is(255));

        assertThat(id, is(clone));
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseIllegalArgument() {
        IDBuilder.parse(new byte[0]);
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseIllegalArgumentNull() {
        IDBuilder.parse(null);
    }
}