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
package org.lable.oss.uniqueid.etcd;

import io.etcd.jetcd.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Timer;
import java.util.TimerTask;

/**
 * {@link ResourceClaim} that automatically relinquishes its hold on a resource
 * after a set amount of time.
 */
public class ExpiringResourceClaim extends ResourceClaim {
    private static final Logger logger = LoggerFactory.getLogger(ExpiringResourceClaim.class);

    public final static Duration DEFAULT_CLAIM_HOLD = Duration.ofSeconds(30);
    public final static Duration DEFAULT_ACQUISITION_TIMEOUT = Duration.ofMinutes(10);

    ExpiringResourceClaim(Client etcd,
                          int poolSize,
                          Duration claimHold,
                          Duration acquisitionTimeout) throws IOException {
        super(etcd, poolSize, acquisitionTimeout);
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                close();
            }
        }, claimHold.toMillis());
    }

    /**
     * Claim a resource.
     *
     * @param etcd     Etcd connection to use.
     * @param poolSize Size of the resource pool.
     * @return A resource claim.
     */
    public static ResourceClaim claimExpiring(Client etcd, int poolSize)
            throws IOException {
        return claimExpiring(etcd, poolSize, DEFAULT_CLAIM_HOLD, DEFAULT_ACQUISITION_TIMEOUT);
    }

    /**
     * Claim a resource.
     *
     * @param etcd               Etcd connection to use.
     * @param poolSize           Size of the resource pool.
     * @param claimHold          How long the claim should be held. May be {@code null} for the default value of
     *                           {@link #DEFAULT_CLAIM_HOLD}.
     * @param acquisitionTimeout How long to keep trying to acquire a claim. May be {@code null} to keep trying
     *                           indefinitely.
     * @return A resource claim.
     */
    public static ResourceClaim claimExpiring(Client etcd,
                                              int poolSize,
                                              Duration claimHold,
                                              Duration acquisitionTimeout)
            throws IOException {

        claimHold = claimHold == null ? DEFAULT_CLAIM_HOLD : claimHold;
        if (logger.isDebugEnabled()) {
            logger.debug("Preparing expiring resource-claim; will release it in {}ms.", claimHold.toMillis());
        }

        return new ExpiringResourceClaim(etcd, poolSize, claimHold, acquisitionTimeout);
    }
}
