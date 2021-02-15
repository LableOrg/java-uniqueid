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

import java.nio.ByteBuffer;

import static org.lable.oss.uniqueid.ParameterUtil.assertNotNullEightBytes;

/**
 * Composes and deconstructs the special eight byte identifiers generated by this library.
 * <p>
 * The eight byte ID is composed as follows:
 *
 * <pre>TTTTTTTT TTTTTTTT TTTTTTTT TTTTTTTT TTTTTTTT TTSSSSSS GGGMGGGG GGGGCCCC</pre>
 *
 * <ul>
 * <li><code>T</code>: Timestamp (in milliseconds, bit order depends on mode)
 * <li><code>S</code>: Sequence counter
 * <li><code>.</code>: Reserved for future use
 * <li><code>M</code>: Mode
 * <li><code>G</code>: Generator ID
 * <li><code>C</code>: Cluster ID
 * </ul>
 *
 * Because only 42 bits are assigned to represent the timestamp in the generated ID, the timestamp used must take place
 * between the Unix epoch (1970-01-01T00:00:00.000 UTC) and 2109.
 */
public class IDBuilder {
    /**
     * Perform all the byte mangling needed to create the eight byte ID.
     *
     * @param blueprint Blueprint containing all needed data to work with.
     * @return The 8-byte ID.
     */
    public static byte[] build(Blueprint blueprint) {
        // First 42 bits are the timestamp.
        // [0] TTTTTTTT [1] TTTTTTTT [2] TTTTTTTT [3] TTTTTTTT [4] TTTTTTTT [5] TT......
        ByteBuffer bb = ByteBuffer.allocate(8);
        switch (blueprint.getMode()) {
            case SPREAD:
                long reverseTimestamp = Long.reverse(blueprint.getTimestamp());
                bb.putLong(reverseTimestamp);
                break;
            case TIME_SEQUENTIAL:
                long timestamp = blueprint.getTimestamp();
                bb.putLong(timestamp << 22);
                break;
        }
        byte[] tsBytes = bb.array();

        // Last 6 bits of byte 6 are for the sequence counter. The first two bits are from the timestamp.
        // [5] TTSSSSSS
        int or = tsBytes[5] | (byte) blueprint.getSequence();
        tsBytes[5] = (byte) or;

        // Last two bytes. The mode flag, generator ID, and cluster ID.
        // [6] GGGMGGGG  [7] GGGGCCCC
        int flagGeneratorCluster = (blueprint.getGeneratorId() << 5) & 0xE000;
        flagGeneratorCluster += (blueprint.getGeneratorId() & 0x00FF) << 4;
        flagGeneratorCluster += blueprint.getClusterId();
        flagGeneratorCluster += blueprint.getMode().getModeMask() << 12;

        tsBytes[7] = (byte) flagGeneratorCluster;
        flagGeneratorCluster >>>= 8;
        tsBytes[6] = (byte) flagGeneratorCluster;

        return tsBytes;
    }

    /**
     * Decompose a generated ID into its {@link Blueprint}.
     *
     * @param id Eight byte ID to parse.
     * @return A blueprint containing the four ID components.
     */
    public static Blueprint parse(byte[] id) {
        assertNotNullEightBytes(id);

        int sequence = parseSequenceIdNoChecks(id);
        int generatorId = parseGeneratorIdNoChecks(id);
        int clusterId = parseClusterIdNoChecks(id);
        long timestamp = parseTimestampNoChecks(id);
        Mode mode = parseModeNoChecks(id);

        return new Blueprint(timestamp, sequence, generatorId, clusterId, mode);
    }

    /**
     * Find the sequence number in an identifier.
     *
     * @param id Identifier.
     * @return The sequence number, if {@code id} is a byte array with length eight.
     */
    public static int parseSequenceId(byte[] id) {
        assertNotNullEightBytes(id);
        return parseSequenceIdNoChecks(id);
    }

    /**
     * Find the generator id in an identifier.
     *
     * @param id Identifier.
     * @return The generator id, if {@code id} is a byte array with length eight.
     */
    public static int parseGeneratorId(byte[] id) {
        assertNotNullEightBytes(id);
        return parseGeneratorIdNoChecks(id);
    }

    /**
     * Find the cluster id in an identifier.
     *
     * @param id Identifier.
     * @return The cluster id, if {@code id} is a byte array with length eight.
     */
    public static int parseClusterId(byte[] id) {
        assertNotNullEightBytes(id);
        return parseClusterIdNoChecks(id);
    }

    /**
     * Find the timestamp in an identifier.
     *
     * @param id Identifier.
     * @return The timestamp, if {@code id} is a byte array with length eight.
     */
    public static long parseTimestamp(byte[] id) {
        assertNotNullEightBytes(id);
        return parseTimestampNoChecks(id);
    }

    /**
     * Find the ID mode used to construct the identifier.
     *
     * @param id Identifier.
     * @return The {@link Mode}, if {@code id} is a byte array with length eight.
     */
    public static Mode parseMode(byte[] id) {
        assertNotNullEightBytes(id);
        return parseModeNoChecks(id);
    }

    // The private methods skip the null and length check on the id, because the method calling them took care of that.

    private static int parseSequenceIdNoChecks(byte[] id) {
        // [5] ..SSSSSS
        return id[5] & 0x3F;
    }

    private static int parseGeneratorIdNoChecks(byte[] id) {
        // [6] GGG.GGGG  [7] GGGG....
        return (id[7] >> 4 & 0x0F) | (id[6] << 3 & 0x0700) | (id[6] << 4 & 0xF0);
    }

    private static int parseClusterIdNoChecks(byte[] id) {
        // [7] ....CCCC
        return id[7] & 0x0F;
    }

    private static long parseTimestampNoChecks(byte[] id) {
        Mode mode = parseModeNoChecks(id);
        switch (mode) {
            case TIME_SEQUENTIAL:
                return parseTimestampNoChecksTime(id);
            case SPREAD:
            default:
                return parseTimestampNoChecksSpread(id);
        }
    }

    private static long parseTimestampNoChecksSpread(byte[] id) {
        byte[] copy = id.clone();

        // Clear everything but the first 42 bits for the timestamp.
        // [0] TTTTTTTT [1] TTTTTTTT [2] TTTTTTTT [3] TTTTTTTT [4] TTTTTTTT [5] TT......
        copy[5] = (byte) (copy[5] & 0xC0);
        copy[6] = 0;
        copy[7] = 0;

        ByteBuffer bb = ByteBuffer.wrap(copy);
        return Long.reverse(bb.getLong());
    }

    private static long parseTimestampNoChecksTime(byte[] id) {
        byte[] copy = id.clone();

        ByteBuffer bb = ByteBuffer.wrap(copy);
        long ts = bb.getLong();
        ts >>>= 22;
        return ts;
    }

    private static Mode parseModeNoChecks(byte[] id) {
        // [6] ...M....
        int modeMask = id[6] >> 4 & 0x01;
        return Mode.fromModeMask(modeMask);
    }
}
