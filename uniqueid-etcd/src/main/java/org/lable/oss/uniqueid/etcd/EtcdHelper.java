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
import io.etcd.jetcd.CloseableClient;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.grpc.stub.StreamObserver;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 * Utility methods that make working with jetcd a little less verbose.
 */
public class EtcdHelper {
    final static ByteSequence UNIQUE_ID_NAMESPACE = ByteSequence.from("unique-id/", StandardCharsets.UTF_8);

    public static Optional<Integer> getInt(Client etcd, String key) throws ExecutionException, InterruptedException {
        GetResponse getResponse = etcd.getKVClient().get(asByteSequence(key)).get();

        if (getResponse.getCount() == 0) return Optional.empty();

        String value = getResponse.getKvs().get(0).getValue().toString(StandardCharsets.UTF_8);
        try {
            return Optional.of(Integer.parseInt(value));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    public static Optional<String> get(Client etcd, String key) throws ExecutionException, InterruptedException {
        GetResponse getResponse = etcd.getKVClient().get(asByteSequence(key)).get();

        if (getResponse.getCount() == 0) return Optional.empty();

        return Optional.of(getResponse.getKvs().get(0).getValue().toString(StandardCharsets.UTF_8));
    }

    public static void put(Client etcd, String key, int value) throws ExecutionException, InterruptedException {
        etcd.getKVClient().put(asByteSequence(key), asByteSequence(value)).get();
    }

    public static void put(Client etcd, String key) throws ExecutionException, InterruptedException {
        etcd.getKVClient().put(asByteSequence(key),ByteSequence.EMPTY).get();
    }

    public static void delete(Client etcd, String key) throws ExecutionException, InterruptedException {
        etcd.getKVClient().delete(asByteSequence(key)).get();
    }

    static ByteSequence asByteSequence(String value) {
        return ByteSequence.from(value, StandardCharsets.UTF_8);
    }

    static ByteSequence asByteSequence(int value) {
        return asByteSequence(String.valueOf(value));
    }

    public static CloseableClient keepLeaseAlive(Client etcd, Long leaseId, OnRelease onRelease) {
        final OnRelease onReleaseCallback = onRelease == null
                ? () -> {}
                : onRelease;

        return etcd.getLeaseClient().keepAlive(
                leaseId,
                new StreamObserver<LeaseKeepAliveResponse>() {
                    @Override
                    public void onNext(LeaseKeepAliveResponse value) {
                        // Great! No-op.
                    }

                    @Override
                    public void onError(Throwable t) {
                        onReleaseCallback.cleanUp();
                    }

                    @Override
                    public void onCompleted() {
                        onReleaseCallback.cleanUp();
                    }
                }
        );
    }

    @FunctionalInterface
    interface OnRelease {
        void cleanUp();
    }
}
