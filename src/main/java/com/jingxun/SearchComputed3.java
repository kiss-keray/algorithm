package com.jingxun;

import cn.hutool.core.util.ByteUtil;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 计算
 */
public class SearchComputed3 {

    private final static Object clock = new Object();
    private static final Map<String, Integer> wordCntMap = new HashMap<>();
    private static final ConcurrentLinkedQueue<String> wq = new ConcurrentLinkedQueue<>();

    private static String dir1;
    private static String dir2;

    public static void main(String[] args) throws Exception {
        dir1 = args[0];
        dir2 = args[1];
        cntInit();
        var queue = new LinkedBlockingQueue<Data>(100);
        var flag = new AtomicBoolean(false);
        startProducer(queue, flag);
        long time = 0;
        var cnt = 0;
        while (true) {
            var x = System.currentTimeMillis();
            var data = execNextData(queue, flag);
            System.out.println("加载耗时:" + (System.currentTimeMillis() - x));
            if (data == null) break;
            var now = System.currentTimeMillis();
            computed(data);
            time += (System.currentTimeMillis() - now);
            data.clean();
            cnt++;
            System.out.printf("完成:%s %d %n", data.word, cnt);
        }
        checkMemory(0, true);
        System.out.println("耗时:" + time);
    }

    private static void computed(Data data) {
        if (data.arr1 == null && data.arr2 == null) return;
        var index1 = 0;
        var index2 = 0;
        if (data.arr3 == null) {
            if (data.arr1 != null) index1 = data.arr1.length;
            if (data.arr2 != null) index2 = data.arr2.length;
        }

    }

    private static void cntInit() throws Exception {
        var read = new BufferedReader(new FileReader(dir2 + "/words"));
        String line = null;
        while ((line = read.readLine()) != null) {
            var ws = line.split(" ");
            wq.add(ws[0]);
            wordCntMap.put(ws[0], Integer.parseInt(ws[1]));
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
        ThreadPoolExecutor pool = new ThreadPoolExecutor(producerSize, producerSize, 1, TimeUnit.SECONDS, new ArrayBlockingQueue<>(2));
        var cnt = new AtomicInteger(producerSize);
        for (var i = 0; i < producerSize; i++) {
            pool.execute(() -> {
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        String word = wq.poll();
                        if (word == null) return;
                        if ((word.hashCode() & 1023) != 1022) continue;
                        try {
                            var data = loadOneData(word);
                            if (data == null) continue;
                            queue.put(data);
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
        var wl = wordCntMap.get(word);
        var fe = fw.equals(ew);
        try (var fi = new FileInputStream(dir1 + "/" + fw);
             var ei = new FileInputStream(dir1 + "/" + ew);
             var wi = wl < 100 ? null : new FileInputStream(dir2 + "/" + (word.hashCode() & 1023) + "/" + word)
        ) {
            var fm = fi.available();
            var em = fe ? 0 : ei.available();
            var wm = wi == null ? 0 : wi.available();
            int[] fa;
            int[] ea = null;
            int[] wa = null;
            var buffLen = 65536;
            byte[] buff;
            if ((wm & 3) != 0) {
                throw new RuntimeException(word);
            }
            synchronized (clock) {
                checkMemory(fm + em + wm + buffLen, false);
                fa = new int[fm >>> 2];
                if (!fe) ea = new int[em >>> 2];
                if (wi != null) wa = new int[wm >>> 2];
                buff = new byte[buffLen];
            }
            data.arr1 = readFileIntArray(fi, fa, buff);
            if (!fe) data.arr2 = readFileIntArray(ei, ea, buff);
            if (wi != null) data.arr3 = readFileIntArray(wi, wa, buff);
            if (wi == null) return null;
        } catch (FileNotFoundException e) {
            return data;
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
