package org.apache.zookeeper.server;

import java.io.IOException;
import java.io.File;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooKeeperServerShutdownHandler;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

/**
 * This class exists as a hack around the package-local defaults on
 * the ZooKeeperServer registerServerShutdownHandler method, which
 * prevent any FC code from providing a handler and thus silencing the
 *
 *  > ERROR o.a.zookeeper.server.ZooKeeperServer - ZKShutdownHandler is not registered, so ZooKeeper server won't take any action on ERROR or SHUTDOWN server state changes
 *
 *  log errors.
 */
public class FCZooKeeperServer extends ZooKeeperServer {

    public FCZooKeeperServer(File snapDir, File logDir, int tickTime)
        throws IOException {
        super(new FileTxnSnapLog(snapDir, logDir),
              tickTime, new ZooKeeperServer.BasicDataTreeBuilder());
    }

    public FCZooKeeperServer(File snapDir, File logDir, int tickTime,
                             FCShutdownHandler zkShutdownHandler)
        throws IOException {
            super(new FileTxnSnapLog(snapDir, logDir),
                  tickTime, new ZooKeeperServer.BasicDataTreeBuilder());
            super.registerServerShutdownHandler((ZooKeeperServerShutdownHandler) zkShutdownHandler);
    }
}
