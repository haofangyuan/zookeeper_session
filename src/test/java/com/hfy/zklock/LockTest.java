package com.hfy.zklock;

import com.hfy.ZooKeeperHelper;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.StringUtils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Author:jianghuimin
 * @Date: 2017/5/24
 * @Time:21:24
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class LockTest {

    @Test
    public void testDistributeLock() throws InterruptedException {
        ZooKeeperHelper.initialize("localhost:2181");
        ZooKeeperHelper zooKeeper = new ZooKeeperHelper();
        ExecutorService executor = Executors.newCachedThreadPool();
        final int count = 20;
        //在完成一组正在其他线程中执行的操作之前，它允许一个或多个线程一直等待。
        final CountDownLatch latch = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {

            executor.submit(new Runnable() {
                public void run() {
                    String path = "";
                    try {
                        //如果不加CountDownLatch，睡1s会直接让主线程跑完shutdown而没有加锁，
                        // 这里睡1000秒可以让多个线程同时执行
                        Thread.sleep(1000);
                        path = zooKeeper.lock();
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                        if (!StringUtils.isEmpty(path)) {
                            zooKeeper.unlock(path);
                        }
                    }
                }
            });
        }

       try {
            //await方法调用此方法会一直阻塞当前线程，直到计时器的值为0
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        executor.shutdown();
        zooKeeper.close();
    }
}
