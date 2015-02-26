package org.lable.util.uniqueid.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.List;

/**
 * A collection of short hand static methods for common ZooKeeper operations.
 */
public class ZooKeeperHelper {

    /**
     * Recursively create empty znodes (if missing) analogous to {@code mkdir -p}.
     *
     * @param zookeeper ZooKeeper instance to work with.
     * @param znode     Path to create.
     * @throws org.apache.zookeeper.KeeperException
     * @throws InterruptedException
     */
    static void mkdirp(ZooKeeper zookeeper, String znode) throws KeeperException, InterruptedException {
        boolean createPath = false;
        for (String path : pathParts(znode)) {
            if (!createPath) {
                Stat stat = zookeeper.exists(path, false);
                if (stat == null) {
                    createPath = true;
                }
            }
            if (createPath) {
                create(zookeeper, path);
            }
        }
    }

    /**
     * Create an empty normal (persistent) Znode.
     *
     * @param zookeeper ZooKeeper instance to work with.
     * @param znode     Znode to create.
     * @throws KeeperException
     * @throws InterruptedException
     */
    static void create(ZooKeeper zookeeper, String znode) throws KeeperException, InterruptedException {
        zookeeper.create(znode, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    /**
     * Create a normal (persistent) Znode.
     *
     * @param zookeeper ZooKeeper instance to work with.
     * @param znode     Znode to create.
     * @param value     Znode contents.
     * @throws KeeperException
     * @throws InterruptedException
     */
    static void create(ZooKeeper zookeeper, String znode, byte[] value) throws KeeperException, InterruptedException {
        zookeeper.create(znode, value, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    /**
     * Create an empty normal (persistent) Znode. If the znode already exists, do nothing.
     *
     * @param zookeeper ZooKeeper instance to work with.
     * @param znode     Znode to create.
     * @throws KeeperException
     * @throws InterruptedException
     */
    static void createIfNotThere(ZooKeeper zookeeper, String znode) throws KeeperException, InterruptedException {
        try {
            create(zookeeper, znode);
        } catch (KeeperException e) {
            if (e.code() != KeeperException.Code.NODEEXISTS) {
                // Rethrow all exceptions, except "node exists",
                // because if the node exists, this method reached its goal.
                throw e;
            }
        }
    }

    /**
     * Parse a znode path, and return a list containing the full paths to its constituent directories.
     *
     * @param path Path to parse.
     * @return List of paths.
     */
    static List<String> pathParts(String path) {
        String[] pathParts = path.split("/");
        List<String> parts = new ArrayList<>(pathParts.length);
        String pathSoFar = "";
        for (String pathPart : pathParts) {
            if (!pathPart.equals("")) {
                pathSoFar += "/" + pathPart;
                parts.add(pathSoFar);
            }
        }
        return parts;
    }
}
