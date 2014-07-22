package org.lable.util.uniqueid.zookeeper;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

public class ZooKeeperConnectionTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void notConfiguredTest() throws IOException {
        thrown.expect(RuntimeException.class);
        thrown.expectMessage("ZooKeeper quorum addresses were never configured.");
        ZooKeeperConnection.get();
    }
}