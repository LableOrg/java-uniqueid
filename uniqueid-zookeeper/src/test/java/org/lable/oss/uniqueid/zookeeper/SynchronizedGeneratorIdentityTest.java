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
package org.lable.oss.uniqueid.zookeeper;

import org.junit.Test;

import java.time.Duration;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.*;
import static org.lable.oss.uniqueid.zookeeper.SynchronizedGeneratorIdentity.getDurationInMillis;

public class SynchronizedGeneratorIdentityTest {
    @Test
    public void getDurationInMillisNullTest() {
        assertThat(getDurationInMillis(null), is(nullValue()));
        assertThat(getDurationInMillis(new DurationSupplier() {
            @Override
            public Long get() {
                return null;
            }
        }), is(nullValue()));
    }

    @Test
    public void getDurationInMillisTest() {
        assertThat(getDurationInMillis(new DurationSupplier() {
            @Override
            public Long get() {
                return 3_600_000L;
            }
        }), is(3_600_000L));
    }
}