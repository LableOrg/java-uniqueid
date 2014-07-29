package org.lable.util.uniqueid;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;

/**
 * Generate short, possibly unique ID's based on the current timestamp. Whether the ID's are truly unique or not
 * depends on the scope of its use. If the combination of generator-ID and cluster-ID passed to this class is unique —
 * i.e., there is only one ID-generator using that specific combination of generator-ID and cluster-ID within the
 * confines of your computing environment at the moment you generate an ID — then the ID's returned are unique.
 */
public abstract class BaseUniqueIDGenerator implements IDGenerator {
    /*
      The eight byte ID is composed as follows:
      TTTTTTTT TTTTTTTT TTTTTTTT TTTTTTTT TTTTTTTT TTSSSSSS ......GG GGGGCCCC

      T: Timestamp (bit order reversed)
      S: Sequence counter
      .: Reserved for future use
      G: Generator ID
      C: Cluster ID

      Because only 42 bits are assigned to represent the timestamp in the generated ID,
      the timestamp used must take place between the Unix epoch (1970-01-01T00:00:00.000) and 2109.
    */

    /**
     * Maximum timestamp, this represents a date somewhen in 2109.
     */
    public final static long MAX_TIMESTAMP = 0x3FFFFFFFFFFL;

    /**
     * ID's using the same timestamp are limited to 64 variations.
     */
    public final static int MAX_SEQUENCE_COUNTER = 63;

    /**
     * Upper bound (inclusive) of the generator-ID.
     */
    public final static int MAX_GENERATOR_ID = 63;

    /**
     * Upper bound (inclusive) of the cluster-ID.
     */
    public final static int MAX_CLUSTER_ID = 15;

    protected int generatorId;
    protected int clusterId;

    long previousTimestamp = 0;
    int sequence = 0;

    /**
     * Create a new UniqueIDGenerator instance.
     *
     * @param generatorId Generator ID to use (0 <= n < 64).
     * @param clusterId   Cluster ID to use (0 <= n < 16).
     */
    protected BaseUniqueIDGenerator(int generatorId, int clusterId) {
        this.generatorId = generatorId;
        this.clusterId = clusterId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized byte[] generate() throws GeneratorException {

        long now = System.currentTimeMillis();
        if (now == previousTimestamp) {
            sequence++;
        } else {
            sequence = 0;
        }
        if (sequence > MAX_SEQUENCE_COUNTER) {
            try {
                TimeUnit.MILLISECONDS.sleep(1);
                return generate();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        previousTimestamp = now;

        Blueprint blueprint = new Blueprint(now, sequence, generatorId, clusterId);

        return mangleBytes(blueprint);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Deque<byte[]> batch(int size) throws GeneratorException {
        Deque<byte[]> stack = new ArrayDeque<byte[]>();
        for (int i = 0; i < size; i++) {
            stack.add(generate());
        }
        return stack;
    }

    /**
     * Helper method that throws an {@link java.lang.IllegalArgumentException} when a number is not within the
     * supplied range.
     *
     * @param name      Name of the parameter to use in the Exception message.
     * @param lower     Lower bound (inclusive).
     * @param upper     Upper bound (inclusive).
     * @param parameter The parameter to test.
     * @throws java.lang.IllegalArgumentException Thrown when the parameter is out of bounds.
     */
    protected static void assertParameterWithinBounds(String name, long lower, long upper, long parameter) {
        if (parameter < lower || parameter > upper) {
            throw new IllegalArgumentException(String.format("Invalid %s: %d (expected: %d <= n < %d)",
                    name, parameter, lower, upper + 1));
        }
    }

    /**
     * Perform all the byte mangling needed to create the eight byte ID.
     *
     * @param blueprint Blueprint containing all needed data to work with.
     * @return The 8-byte ID.
     */
    static byte[] mangleBytes(Blueprint blueprint) {
        long reverseTimestamp = Long.reverse(blueprint.getTimestamp());
        // First 42 bits are the reversed timestamp.
        // [0] TTTTTTTT [1] TTTTTTTT [2] TTTTTTTT [3] TTTTTTTT [4] TTTTTTTT [5] TTTTTTTT [6] TT......
        ByteBuffer bb = ByteBuffer.allocate(8);
        byte[] tsBytes = bb.putLong(reverseTimestamp).array();

        // Last 6 bits of byte 6 are for the sequence counter. The first two bits are from the timestamp.
        // [5] TTSSSSSS
        int or = tsBytes[5] | (byte) blueprint.getSequence();
        tsBytes[5] = (byte) or;

        // Last two bytes.
        // [6] ......GG  [7] GGGGCCCC
        int generatorAndCluster = blueprint.getGeneratorId() << 4;
        generatorAndCluster += blueprint.getClusterId();

        tsBytes[7] = (byte) generatorAndCluster;
        generatorAndCluster >>>= 8;
        tsBytes[6] = (byte) generatorAndCluster;

        return tsBytes;
    }

    /**
     * Decompose a generated ID into its {@link BaseUniqueIDGenerator.Blueprint}.
     *
     * @param id Eight byte ID to parse.
     * @return A blueprint containing the four ID components.
     */
    public static Blueprint parse(byte[] id) {
        if (id.length != 8) {
            throw new IllegalArgumentException(String.format("Expected an 8-byte ID, but got: %d bytes.", id.length));
        }

        byte[] copy = id.clone();

        // [5] ..SSSSSS
        int sequence = copy[5] & 0x3F;

        // [6] ......GG  [7] GGGG....
        int generatorId = (copy[7] >> 4 & 0x0F) | (copy[6] << 4);

        // [7] ....CCCC
        int clusterId = copy[7] & 0x0F;

        // Clear everything but the first 42 bits for the timestamp.
        // [0] TTTTTTTT [1] TTTTTTTT [2] TTTTTTTT [3] TTTTTTTT [4] TTTTTTTT [5] TTTTTTTT [6] TT......
        copy[5] = (byte) (copy[5] & 0xC0);
        copy[6] = 0;
        copy[7] = 0;

        ByteBuffer bb = ByteBuffer.wrap(copy);
        long timestamp = Long.reverse(bb.getLong());

        return new Blueprint(timestamp, sequence, generatorId, clusterId);
    }

    /**
     * Struct containing all data required to build the ID.
     */
    public static class Blueprint {
        final long timestamp;
        final int sequence;
        final int generatorId;
        final int clusterId;

        /**
         * Create a blueprint for a unique ID.
         *
         * @param timestamp   Milliseconds since the Unix epoch.
         * @param sequence    Sequence counter.
         * @param generatorId Generator ID.
         * @param clusterId   Cluster ID.
         * @see BaseUniqueIDGenerator#MAX_CLUSTER_ID
         * @see BaseUniqueIDGenerator#MAX_GENERATOR_ID
         */
        public Blueprint(long timestamp, int sequence, int generatorId, int clusterId) {
            assertParameterWithinBounds("timestamp", 0, MAX_TIMESTAMP, timestamp);
            assertParameterWithinBounds("sequence counter", 0, MAX_SEQUENCE_COUNTER, sequence);
            assertParameterWithinBounds("generator-ID", 0, MAX_GENERATOR_ID, generatorId);
            assertParameterWithinBounds("cluster-ID", 0, MAX_CLUSTER_ID, clusterId);


            this.timestamp = timestamp;
            this.sequence = sequence;
            this.generatorId = generatorId;
            this.clusterId = clusterId;
        }

        /**
         * @return The ID that corresponds to this Blueprint.
         */
        public byte[] getID() {
            return BaseUniqueIDGenerator.mangleBytes(this);
        }

        /**
         * @return The timestamp.
         */
        public long getTimestamp() {
            return timestamp;
        }

        /**
         * @return The sequence counter, incremented in case more than one ID was generated in the same millisecond.
         */
        public int getSequence() {
            return sequence;
        }

        /**
         * @return ID of the generating instance.
         */
        public int getGeneratorId() {
            return generatorId;
        }

        /**
         * @return ID of the cluster this ID was generated on.
         */
        public int getClusterId() {
            return clusterId;
        }

        @Override
        public String toString() {
            return String.format(
                    "{\n  timestamp: %d,\n  sequence: %d,\n  generator: %d,\n  cluster: %d\n}",
                    timestamp, sequence, generatorId, clusterId
            );
        }
    }
}
