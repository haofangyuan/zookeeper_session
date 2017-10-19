package com.hfy.zklock;

import com.hfy.ConnectionWatcher;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.springframework.util.StringUtils;

import java.util.Collections;
import java.util.List;

/**
 * 利用临时顺序节点实现共享锁的改进实现
 * <p>
 * 改进后的分布式锁实现，和之前的实现方式唯一不同之处在于，这里设计成每个锁竞争者，
 * 只需要关注”locknode”节点下序号比自己小的那个节点是否存在即可。
 * <p>
 * 算法思路：
 * 对于加锁操作，可以让所有客户端都去/lock目录下创建临时顺序节点，
 * 如果创建的客户端发现自身创建节点序列号是/lock/目录下最小的节点，则获得锁。
 * 否则，监视比自己创建节点的序列号小的节点（比自己创建的节点小的最大节点），进入等待。
 *
 * @Author:jianghuimin
 * @Date: 2017/5/24
 * @Time:20:40
 */
public class DistributeLock {

    private String lockZnode = null;

    private static ZooKeeper zk;

    private static String zooKeeperUrl = "localhost:2181,localhost:2182,localhost:2183";

    static {
        ConnectionWatcher cw = new ConnectionWatcher();
        zk = cw.connection(zooKeeperUrl);
    }

    /**
     * 获取锁
     *
     * @return
     * @throws InterruptedException
     */
    public void lock() {
        try {

            Stat stat = zk.exists("/locknode", false);//此去不执行 Watcher
            if (stat == null) {
                //创建根节点，永久存在
                zk.create("/locknode", "lock".getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            String path = zk.create("/locknode/guid-lock", "lock".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            lockZnode = path;
            System.out.println("=============  " + path);
            List<String> children = zk.getChildren("/locknode", true);
            Collections.sort(children);
            if (!StringUtils.isEmpty(path)
                    && !StringUtils.isEmpty(children.get(0))
                    && path.equals("/locknode/" + children.get(0))) {
                System.out.println(Thread.currentThread().getName() + "  get Lock...");
                return;
            }
            String watchNode = null;
            for (int i = children.size() - 1; i >= 0; i--) {
                if (children.get(i).compareTo(path.substring(path.lastIndexOf("/") + 1)) < 0) {
                    watchNode = children.get(i);
                    break;
                }
            }
            System.out.println(">>>>>>>>>>>>>>>>>>>>  " + watchNode);
            if (watchNode != null) {
                final String watchNodeTmp = watchNode;
                //给当前线程的创建的znode小的znode添加监听，当发生删除事件的时候只叫醒当前的这个线程
                final Thread thread = Thread.currentThread();
                Stat stat1 = zk.exists("/locknode/" + watchNodeTmp, new Watcher() {
                    @Override
                    public void process(WatchedEvent watchedEvent) {
                        if (watchedEvent.getType() == Event.EventType.NodeDeleted) {
                            thread.interrupt();
                        }
                        try {
                            zk.exists("/locknode/" + watchNodeTmp, true);
                        } catch (KeeperException | InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                });
                if (stat1 != null) {
//                    System.out.println("Thread " + Thread.currentThread().getId() + " waiting for " + "/locknode/" + watchNode);
                }
            }
            try {
                //等待直到被唤醒
                Thread.sleep(1000000000);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
                System.out.println(Thread.currentThread().getName() + " notify");
                System.out.println(Thread.currentThread().getName() + " get Lock...");
                return;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 释放锁
     */
    public void unlock() {
        try {
            System.out.println(Thread.currentThread().getName() + " release Lock...");
            zk.delete(lockZnode, -1);
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

}
