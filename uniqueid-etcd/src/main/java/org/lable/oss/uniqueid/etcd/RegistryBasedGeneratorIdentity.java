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
package org.lable.oss.uniqueid.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import org.lable.oss.uniqueid.GeneratorException;
import org.lable.oss.uniqueid.GeneratorIdentityHolder;
import org.lable.oss.uniqueid.bytes.Blueprint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Holder for a claimed cluster-id and generator-id that once claimed remains claimed without an active connection to
 * an Etcd cluster. The claim is relinquished upon calling {@link #close()} (where a new connection to Etcd will be
 * set up briefly).
 */
public class RegistryBasedGeneratorIdentity implements GeneratorIdentityHolder {
    private static final Logger logger = LoggerFactory.getLogger(RegistryBasedGeneratorIdentity.class);

    private final String endpoints;
    private final String namespace;
    private final RegistryBasedResourceClaim resourceClaim;

    public RegistryBasedGeneratorIdentity(String endpoints, String namespace, String registryEntry) {
        this.endpoints = endpoints;
        this.namespace = namespace;

        try {
            resourceClaim = acquireResourceClaim(registryEntry, 0);
        } catch (GeneratorException e) {
            throw new RuntimeException(e);
        }
    }

    public static RegistryBasedGeneratorIdentity basedOn(String endpoints, String namespace, String registryEntry)
            throws IOException {
        return new RegistryBasedGeneratorIdentity(
                endpoints, namespace, registryEntry
        );
    }

    @Override
    public int getClusterId() throws GeneratorException {
        return resourceClaim.getClusterId();
    }

    @Override
    public int getGeneratorId() throws GeneratorException {
        return resourceClaim.getGeneratorId();
    }

    public String getRegistryEntry() {
        return resourceClaim.getRegistryEntry();
    }

    private RegistryBasedResourceClaim acquireResourceClaim(String registryEntry, int retries)
            throws GeneratorException {
        try {
            return RegistryBasedResourceClaim.claim(
                    this::getEtcdConnection,
                    Blueprint.MAX_GENERATOR_ID + 1,
                    registryEntry
            );
        } catch (IOException e) {
            logger.warn(
                    "Connection to Etcd failed, retrying resource claim acquisition, attempt {}.",
                    retries + 1
            );
            if (retries < 3) {
                return acquireResourceClaim(registryEntry, retries + 1);
            } else {
                throw new GeneratorException(e);
            }
        }
    }

    Client getEtcdConnection() {
        return Client.builder()
                .endpoints(endpoints.split(","))
                .namespace(ByteSequence.from(namespace, StandardCharsets.UTF_8))
                .build();
    }

    @Override
    public void close() throws IOException {
        if (resourceClaim != null) {
            resourceClaim.close();
        }
    }
}
