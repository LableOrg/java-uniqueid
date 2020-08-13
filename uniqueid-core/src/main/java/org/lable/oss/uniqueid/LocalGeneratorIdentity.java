/*
 * Copyright © 2014 Lable (info@lable.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lable.oss.uniqueid;

import org.lable.oss.uniqueid.bytes.Blueprint;

import java.io.IOException;

import static org.lable.oss.uniqueid.ParameterUtil.assertParameterWithinBounds;

public class LocalGeneratorIdentity implements GeneratorIdentityHolder {
    private final int clusterId;
    private final int generatorId;
    private boolean closed = false;

    LocalGeneratorIdentity(int clusterId, int generatorId) {
        this.clusterId = clusterId;
        this.generatorId = generatorId;
    }

    public static LocalGeneratorIdentity with(int clusterId, int generatorId) {
        assertParameterWithinBounds("generatorId", 0, Blueprint.MAX_GENERATOR_ID, generatorId);
        assertParameterWithinBounds("clusterId", 0, Blueprint.MAX_CLUSTER_ID, clusterId);
        return new LocalGeneratorIdentity(clusterId, generatorId);
    }

    @Override
    public int getClusterId() {
        if (closed) throw new IllegalStateException("Resource was closed.");
        return clusterId;
    }

    @Override
    public int getGeneratorId() {
        if (closed) throw new IllegalStateException("Resource was closed.");
        return generatorId;
    }

    @Override
    public void close() throws IOException {
        closed = true;
    }
}
