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