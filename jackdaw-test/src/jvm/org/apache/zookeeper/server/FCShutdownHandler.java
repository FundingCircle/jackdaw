package org.apache.zookeeper.server;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.server.ZooKeeperServer.State;
import org.apache.zookeeper.server.ZooKeeperServerShutdownHandler;

/**
 * This class exists as a hack around the package-local defaults
 * on the ZooKeeperServerShutdownHandler constructor, which prevent
 * any FC code from providing a handler and thus silencing the
 *
 *  > ERROR o.a.zookeeper.server.ZooKeeperServer - ZKShutdownHandler is not registered, so ZooKeeper server won't take any action on ERROR or SHUTDOWN server state changes
 *
 *  log errors.
 */
public class FCShutdownHandler extends ZooKeeperServerShutdownHandler {
    public FCShutdownHandler(CountDownLatch shutdownLatch) {
        super(shutdownLatch);
    }
}
