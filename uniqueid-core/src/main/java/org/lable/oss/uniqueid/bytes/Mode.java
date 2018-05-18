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

/**
 * ID generation mode.
 */
public enum Mode {
    /**
     * Generated IDs start with a timestamp with the bytes in reverse order to prevent hot-spotting in key-value
     * stores that order their records based on the key. This mode is ideally suited for generating opaque
     * identifiers without a predictable order.
     * <p>
     * Generators are encouraged to cache a stack of pre-generated IDs, to reduce I/O, as there is no need to
     * maintain a claimed generator-ID for longer than it takes to top up the stack of IDs.
     */
    SPREAD,
    /**
     * Generated IDs start with a timestamp in natural sorting order. The timestamps are intended to be
     * used actively, so generators should not cache pre-generated IDs for long periods of time (how long
     * depends on the application) or not at all. This may result in more I/O to maintain an active claim on a
     * generator-ID (if a coordination service such as ZooKeeper is used).
     */
    TIME_SEQUENTIAL;

    public int getModeMask() {
        return ordinal();
    }

    public static Mode defaultMode() {
        return SPREAD;
    }

    public static Mode fromModeMask(int modeMask) {
        switch (modeMask) {
            case 1:
                return Mode.TIME_SEQUENTIAL;
            case 0:
            default:
                return Mode.SPREAD;
        }
    }
}
