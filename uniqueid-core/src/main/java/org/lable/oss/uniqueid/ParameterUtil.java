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
package org.lable.oss.uniqueid;

/**
 * Parameter validation helpers.
 */
public class ParameterUtil {
    /**
     * Throw an {@link IllegalArgumentException} when a number is not within the supplied range.
     *
     * @param name      Name of the parameter to use in the Exception message.
     * @param lower     Lower bound (inclusive).
     * @param upper     Upper bound (inclusive).
     * @param parameter The parameter to test.
     * @throws IllegalArgumentException Thrown when the parameter is out of bounds.
     */
    public static void assertParameterWithinBounds(String name, long lower, long upper, long parameter) {
        if (parameter < lower || parameter > upper) {
            throw new IllegalArgumentException(String.format("Invalid %s: %d (expected: %d <= n < %d)",
                    name, parameter, lower, upper + 1));
        }
    }

    /**
     * Thrown an {@link IllegalArgumentException} when the byte array does not contain exactly eight bytes.
     *
     * @param bytes Byte array.
     */
    public static void assertNotNullEightBytes(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("Expected 8 bytes, but got null.");
        }
        if (bytes.length != 8) {
            throw new IllegalArgumentException(String.format("Expected 8 bytes, but got: %d bytes.", bytes.length));
        }
    }
}
