package com.hfy.zklock;

import com.hfy.ConnectionWatcher;
import org.apache.zookeeper.*;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * 利用节点名称的唯一性来实现共享锁
 * @Author:jianghuimin
 * @Date: 2017/5/25
 * @Time:20:49
 */
public class SDistributeLock {

    private  String lockZnode = null;

    private String lockNameSpace = "/mylock";

    private String nodeString = lockNameSpace + "/test1";

    private static ZooKeeper zk;

    private static String zooKeeperUrl="localhost:2181,localhost:2182,localhost:2183";

    static {
        ConnectionWatcher cw = new ConnectionWatcher();
        zk = cw.connection(zooKeeperUrl);
    }

    private void ensureRootPath() throws InterruptedException {
        try {
            if (zk.exists(lockNameSpace,true)==null){
                zk.create(lockNameSpace,"".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    private void watchNode(final String nodeString, final Thread thread) throws InterruptedException {
        try {
            zk.exists(nodeString, new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {    //3类事件触发wather后就不再作用，也就是所谓的（一次作用）
                    System.out.println( "==" + watchedEvent.toString());
                    if(watchedEvent.getType() == Event.EventType.NodeDeleted){
                        System.out.println("Threre is a Thread released Lock==============");
                        thread.interrupt();
                    }
                    try {
                        zk.exists(nodeString,new Watcher() {
                            @Override
                            public void process(WatchedEvent watchedEvent) {
                                System.out.println( "==" + watchedEvent.toString());
                                if(watchedEvent.getType() == Event.EventType.NodeDeleted){
                                    System.out.println("is a Thread released Lock==============");
                                    thread.interrupt();
                                }
                                try {
                                    zk.exists(nodeString,true);
                                } catch (KeeperException | InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }

                        });
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            });
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取锁
     * @return
     * @throws InterruptedException
     */
    public boolean lock() throws InterruptedException {
        String path = null;
        ensureRootPath();
        watchNode(nodeString,Thread.currentThread());
        while (true) {
            try {
                path = zk.create(nodeString, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } catch (KeeperException e) {
                System.out.println(Thread.currentThread().getName() + "  getting Lock but can not get");
                try {
                    Thread.sleep(5000);
                }catch (InterruptedException ex){
                    System.out.println("thread is notify");
                }
            }
            if (!StringUtils.isEmpty(path)) {
                System.out.println(Thread.currentThread().getName() + "  get Lock...");
                return true;
            }
        }
    }

    /**
     * 释放锁
     */
    public void unlock(){
        try {
            zk.delete(nodeString,-1);
            System.out.println(Thread.currentThread().getName() + " release Lock...");
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

}
