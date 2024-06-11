package com.jingxun;

import cn.hutool.core.util.ByteUtil;

import java.io.FileInputStream;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 计算
 */
public class SearchComputed3 {

    private final static Object clock = new Object();


    public static void main(String[] args) throws InterruptedException {
        for (var i = 0; i < 1; i++) {
            var queue = new LinkedBlockingQueue<Data>(100);
            var flag = new AtomicBoolean(false);
            startProducer(queue, flag);
            long time = 0;
            var cnt = 0;
            while (true) {
                var data = execNextData(queue, flag);
                if (data == null) break;
//                System.out.println("开始--：" + data.word);
                var now = System.currentTimeMillis();
                Thread.sleep(10);
                time += (System.currentTimeMillis() - now);
                // 计算过程
                data.clean();
                cnt++;
//                System.out.println("结束##：" + data.word);
            }
            checkMemory(0, true);
            System.out.println(cnt + " " + i + " => " + time);
        }
    }


    private static Data execNextData(LinkedBlockingQueue<Data> queue, AtomicBoolean flag) {
        try {
            var r = queue.poll(5, TimeUnit.SECONDS);
            if (r == null) {
                return flag.get() ? null : execNextData(queue, flag);
            }
            return r;
        } catch (InterruptedException e) {
            return null;
        }
    }

    private static void startProducer(LinkedBlockingQueue<Data> queue, AtomicBoolean flag) throws InterruptedException {
        var producerSize = 10;
        ThreadPoolExecutor pool = new ThreadPoolExecutor(producerSize, producerSize, 1, TimeUnit.SECONDS, new LinkedBlockingDeque<>(1));
        var words = List.of("数据", "实用", "大数", "石墨", "新型", "用例", "美颜", "石化", "能源", "生命", "编程");
        var wq = new ConcurrentLinkedQueue<>(words);
        var cnt = new AtomicInteger(producerSize);
        for (var i = 0; i < producerSize; i++) {
            pool.execute(() -> {
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        String word = wq.poll();
                        if (word == null) return;
                        try {
                            queue.put(loadOneData(word));
                        } catch (OutOfMemoryError ignore) {
                            try {
                                Thread.sleep(1);
                                wq.add(word);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                } finally {
                    if (cnt.getAndIncrement() == producerSize) {
                        pool.shutdown();
                        flag.set(true);
                    }
                }
            });
        }
    }

    private static Data loadOneData(String word) throws Exception {
        var data = new Data();
        data.word = word;
        var fw = word.substring(0, 1);
        var ew = word.substring(1);
        try (var fi = new FileInputStream("/Users/keray/Downloads/xxx/" + fw);
             var ei = new FileInputStream("/Users/keray/Downloads/xxx/" + ew);
        ) {
            var fm = fi.available();
            var em = ei.available();
            int[] fa;
            int[] ea;
            var buffLen = 65536;
            byte[] buff;
            synchronized (clock) {
                checkMemory(fm + em + buffLen, false);
                fa = new int[fm / 4];
                ea = new int[em / 4];
                buff = new byte[buffLen];
                Thread.sleep(20);
            }
            data.arr1 = readFileIntArray(fi, fa, buff);
            data.arr2 = readFileIntArray(ei, ea, buff);
        }
        return data;
    }

    private static int[] readFileIntArray(FileInputStream input, int[] array, byte[] bs) throws Exception {
        int len;
        var index = 0;
        while ((len = input.read(bs)) > 0) {
            for (var i = 0; i < len; i += 4) {
                if (Thread.currentThread().isInterrupted()) return null;
                array[index++] = ByteUtil.bytesToInt(bs, i, ByteOrder.LITTLE_ENDIAN);
            }
        }
        return array;
    }

    private static void checkMemory(int byteLen, boolean out) throws InterruptedException {
        long freeMemory = 0;
        for (var len = 0; len < 1; len++) {
            for (var i = 0; freeMemory < byteLen + 200 * 1024 * 1024; i++) {
                if (i > 10000) throw new RuntimeException();
                freeMemory = Runtime.getRuntime().freeMemory();
                Thread.sleep(10);
            }
            if (out)
                System.out.println(Thread.currentThread() + " 需要:" + (int) (byteLen / 1024.0 / 1024.0) + " 剩余:" + (int) (freeMemory / 1024.0 / 1024.0) + " 后剩余:" + (int) ((freeMemory - byteLen) / 1024.0 / 1024.0));
        }
    }

    static class Data {
        private String word;
        private int[] arr1;
        private int[] arr2;
        private int[] arr3;

        void clean() {
            arr1 = null;
            arr2 = null;
            arr3 = null;
            System.gc();
        }
    }
}
