/**
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
package org.lable.oss.uniqueid.zookeeper;

import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.lable.oss.uniqueid.zookeeper.ZooKeeperHelper.pathParts;

public class ZooKeeperHelperTest {
    @Test
    public void pathPartsTest() {
        final String INPUT_A = "/znode";
        final List<String> RESULT_A = pathParts(INPUT_A);
        assertThat(RESULT_A.size(), is(1));
        assertThat(RESULT_A.get(0), is("/znode"));

        final String INPUT_B = "/znode/nested";
        List<String> RESULT_B = pathParts(INPUT_B);
        assertThat(RESULT_B.size(), is(2));
        assertThat(RESULT_B.get(0), is("/znode"));
        assertThat(RESULT_B.get(1), is("/znode/nested"));
    }
}