package com.jingxun;

import cn.hutool.core.util.ByteUtil;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 计算
 */
public class SearchComputed3 {

    private final static Object clock = new Object();
    private static final Map<String, Integer> wordCntMap = new HashMap<>();
    private static final ConcurrentLinkedQueue<String> wq = new ConcurrentLinkedQueue<>();

    private static String dir1;
    private static String dir2;

    private final static AtomicLong queueMemory = new AtomicLong(0);

    private static long maxMemory = 3000L * 1024 * 1024;

    public static void main(String[] args) throws Exception {
        dir1 = args[0];
        dir2 = args[1];
        queueMemory.set(maxMemory);
        cntInit();
        var queue = new LinkedBlockingQueue<Data>(100);
        var flag = new AtomicBoolean(false);
        startProducer(queue, flag);
        long time = 0;
        var cnt = 0;
        while (true) {
            var data = execNextData(queue, flag);
            if (data == null) break;
            var useTime = computed(data);
            data.clean();
            if (useTime == 0) continue;
            time += useTime;
            cnt++;
//            if (time > 0) System.out.printf("%.0f/s%n", cnt / (float) time * 1000);
        }
        checkMemory(0, true, queueMemory);
        System.out.println("耗时:" + time);
    }

    private static long computed(Data data) {
        var _now = System.currentTimeMillis();
        if (data.arr1 == null || data.arr2 == null) return 0;
        var _x = data.arr1.length < data.arr2.length;
        var shortArr = _x ? data.arr1 : data.arr2;
        var longArr = _x ? data.arr2 : data.arr1;
        var ids = getIntegers(data, shortArr, longArr);
        var t1 = System.currentTimeMillis() - _now;
        System.out.printf("%s sl=%-15s ll=%-15s  r=%-10s %10s %n", data.word, shortArr.length, longArr.length, t1, ids.size());
        return System.currentTimeMillis() - _now;
    }

    private static ArrayList<Integer> getIntegers(Data data, int[] shortArr, int[] longArr) {
        var ids = new ArrayList<Integer>();
        var left = 0;
        var left1 = 0;
        for (var val : shortArr) {
            var r = binarySearch(longArr, val, left);
            left = r[1];
            if (r[0] == -1) continue;
            if (data.arr3 == null) {
                ids.add(val);
            } else {
                var r1 = binarySearch(data.arr3, val, left1);
                left1 = r1[1];
                if (r1[0] < 0) {
                    ids.add(val);
                }
            }
        }
        return ids;
    }

    private static int[] binarySearch(int[] array, int val, int left) {
        var start = left;
        var end = array.length - 1;
        int middle = 0;
        while (start <= end) {
            middle = (start + end) >>> 1;
            if (val < array[middle]) end = middle - 1;
            else if (val > array[middle]) start = middle + 1;
            else {
                return new int[]{middle, middle};
            }
        }
        return new int[]{-1, middle};
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
        var pool = new ThreadPoolExecutor(producerSize, producerSize, 10, TimeUnit.SECONDS, new LinkedBlockingDeque<>(1));
        var cnt = new AtomicInteger(0);
        for (var i = 0; i < producerSize; i++) {
            pool.execute(() -> {
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        String word = wq.poll();
                        if (word == null) return;
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
                    if (cnt.incrementAndGet() == producerSize) {
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
        FileInputStream fi = null;
        FileInputStream ei = null;
        FileInputStream wi = null;
        try {
            fi = new FileInputStream(dir1 + "/" + fw);
        } catch (FileNotFoundException ignore) {
        }
        try {
            ei = new FileInputStream(dir1 + "/" + ew);
        } catch (FileNotFoundException ignore) {
        }

        try {
            wi = wl < 100 ? null : new FileInputStream(dir2 + "/" + (word.hashCode() & 1023) + "/" + word);
        } catch (FileNotFoundException ignore) {
        }
        try {
            var fm = fi == null ? 0 : fi.available();
            var em = fe || ei == null ? 0 : ei.available();
            var wm = wi == null ? 0 : wi.available();
            if (fm + em + wm == 0) return null;
            int[] fa = null;
            int[] ea = null;
            int[] wa = null;
            var buffLen = 65536;
            byte[] buff;
            if ((wm & 3) != 0) {
                throw new RuntimeException(word);
            }
            var memoryLen = fm + em + wm + buffLen;
            synchronized (clock) {
                checkMemory(memoryLen, false, queueMemory);
                if (fm > 0) fa = new int[fm >>> 2];
                if (em > 0) ea = new int[em >>> 2];
                if (wm > 0) wa = new int[wm >>> 2];
                buff = new byte[buffLen];
                data.memory = memoryLen;
            }
            if (fm > 0) data.arr1 = readFileIntArray(fi, fa, buff);
            if (em > 0) data.arr2 = readFileIntArray(ei, ea, buff);
            if (wm > 0) data.arr3 = readFileIntArray(wi, wa, buff);
            return data;
        } finally {
            if (fi != null) fi.close();
            if (ei != null) ei.close();
            if (wi != null) wi.close();
        }
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

    private static void checkMemory(int byteLen, boolean out, AtomicLong memoryCount) throws InterruptedException {
        long nextFree = 0;
        for (var i = 0; ; i++) {
            nextFree = memoryCount.addAndGet(-byteLen);
            if (nextFree > 0) break;
            memoryCount.addAndGet(byteLen);
            Thread.sleep(10);
        }
        if (out) {
            var use = (int) ((maxMemory - nextFree) / 1024.0 / 1024.0);
            var free = (int) (Runtime.getRuntime().freeMemory() / 1024.0 / 1024.0);
            System.out.printf("需要:%-10s 已使用:%-10s 剩余:%-10s 实剩:%-10s %n",
                    (int) (byteLen / 1024.0 / 1024.0),
                    use,
                    (int) (nextFree / 1024.0 / 1024.0),
                    free
            );
        }
    }

    static class Data {
        private String word;
        private int[] arr1;
        private int[] arr2;
        private int[] arr3;
        private int memory;

        void clean() {
            SearchComputed3.queueMemory.getAndAdd(memory);
            arr1 = null;
            arr2 = null;
            arr3 = null;
            System.gc();
        }
    }
}
