package com.hfy;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ConnectionWatcher implements Watcher {

    private static final int SESSION_TIMEOUT = 5000;
    private CountDownLatch signal = new CountDownLatch(1);
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * @throws IOException
     * @throws InterruptedException
     */
    public ZooKeeper connection(String servers) {
        ZooKeeper zk;
        try {
            zk = new ZooKeeper(servers, SESSION_TIMEOUT, this);
            signal.await();
            return zk;
        } catch (IOException | InterruptedException e) {
            logger.error("{}", e);
        }
        return null;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
     */
    public void process(WatchedEvent event) {
        Event.KeeperState state = event.getState();
        if (state == Event.KeeperState.SyncConnected) {
//            signal.countDown();
            if (Event.EventType.None == event.getType() && event.getPath() == null){
                signal.countDown();
                logger.info("链接状态：{}", event.getState());
            }
        }
    }
}