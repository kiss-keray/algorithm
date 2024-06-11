package com.jingxun;

import cn.hutool.core.util.ByteUtil;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 给定一个排列有n个孔洞的模具A 和排列有n个补胶的模具B，模具B中只有m(0 < m <= n)个有效的补胶。孔洞可以重复补
 * 计算出使用X个模具B填补上模具A的所有孔洞，重复补的空洞的次数为Y，找出（X+Y/100）最小的模具B组合
 * todo
 */
public class Keray {
    public static void main(String[] args) throws Exception {
        var latch = new AddDownLatch();
        ThreadPoolExecutor pool = new ThreadPoolExecutor(200, 200, 100, TimeUnit.DAYS, new LinkedBlockingDeque<>());
        var cnt = new AtomicInteger();
        for (var i = 0; i < 10000; i++) {
            latch.add();
            pool.execute(() -> {
                try {
                    Thread.sleep((long) (10 + Math.random() * 100));
                    cnt.getAndIncrement();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        System.out.println(cnt.get());
        pool.shutdownNow();
    }
}
