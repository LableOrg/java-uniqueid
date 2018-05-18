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

import static org.lable.oss.uniqueid.ParameterUtil.assertParameterWithinBounds;

/**
 * Contains all parameters required to build the ID.
 */
public class Blueprint {
    /**
     * Maximum timestamp, this represents a date somewhen in 2109.
     */
    public final static long MAX_TIMESTAMP = 0x3FFFFFFFFFFL;

    /**
     * IDs using the same timestamp are limited to 64 variations.
     */
    public final static int MAX_SEQUENCE_COUNTER = 63;

    /**
     * Upper bound (inclusive) of the generator-ID.
     */
    public final static int MAX_GENERATOR_ID = 255;

    /**
     * Upper bound (inclusive) of the cluster-ID.
     */
    public final static int MAX_CLUSTER_ID = 15;

    final long timestamp;
    final int sequence;
    final int generatorId;
    final int clusterId;
    final Mode mode;

    /**
     * Create a blueprint for a unique ID with the default mode of {@link Mode#SPREAD}.
     *
     * @param timestamp   Milliseconds since the Unix epoch.
     * @param sequence    Sequence counter.
     * @param generatorId Generator ID.
     * @param clusterId   Cluster ID.
     * @see #MAX_CLUSTER_ID
     * @see #MAX_GENERATOR_ID
     */
    public Blueprint(long timestamp, int sequence, int generatorId, int clusterId) {
        this(timestamp, sequence, generatorId, clusterId, Mode.SPREAD);
    }

    /**
     * Create a blueprint for a unique ID.
     *
     * @param timestamp   Milliseconds since the Unix epoch.
     * @param sequence    Sequence counter.
     * @param generatorId Generator ID.
     * @param clusterId   Cluster ID.
     * @param mode        Mode to use.
     * @see #MAX_CLUSTER_ID
     * @see #MAX_GENERATOR_ID
     * @see Mode
     */
    public Blueprint(long timestamp, int sequence, int generatorId, int clusterId, Mode mode) {
        assertParameterWithinBounds("timestamp", 0, MAX_TIMESTAMP, timestamp);
        assertParameterWithinBounds("sequence counter", 0, MAX_SEQUENCE_COUNTER, sequence);
        assertParameterWithinBounds("generator-ID", 0, MAX_GENERATOR_ID, generatorId);
        assertParameterWithinBounds("cluster-ID", 0, MAX_CLUSTER_ID, clusterId);

        this.timestamp = timestamp;
        this.sequence = sequence;
        this.generatorId = generatorId;
        this.clusterId = clusterId;
        this.mode = mode == null ? Mode.SPREAD : mode;
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

    /**
     * @return The ID mode chosen.
     */
    public Mode getMode() {
        return mode;
    }

    @Override
    public String toString() {
        return String.format(
                "{\n  mode: %s,\n  timestamp: %d,\n  sequence: %d,\n  generator: %d,\n  cluster: %d\n}",
                mode, timestamp, sequence, generatorId, clusterId
        );
    }
}
