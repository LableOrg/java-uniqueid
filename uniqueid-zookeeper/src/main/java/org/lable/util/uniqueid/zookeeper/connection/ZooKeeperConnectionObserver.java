package org.lable.util.uniqueid.zookeeper.connection;

/**
 * ?
 */
public interface ZooKeeperConnectionObserver {
    public void disconnected();
    public void connected();
}
