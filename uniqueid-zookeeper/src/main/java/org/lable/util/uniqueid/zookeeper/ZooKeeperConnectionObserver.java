package org.lable.util.uniqueid.zookeeper;

/**
 * ?
 */
public interface ZooKeeperConnectionObserver {
    public void disconnected();
    public void connected();
}
