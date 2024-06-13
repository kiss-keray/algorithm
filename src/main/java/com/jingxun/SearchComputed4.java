package com.jingxun;

import cn.hutool.core.util.ByteUtil;
import cn.hutool.core.util.StrUtil;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 生成词到ids的数据
 * 数据 -> [1,2,3]
 */
public class SearchComputed4 {

    private static String rootPath;

    private static final int dirSize = 1024;

    public static void main(String[] args) throws Exception {
        System.out.println("参数：" + Arrays.toString(args));
        rootPath = args[0];
        if (StrUtil.isEmpty(rootPath)) return;
        errorOut = new FileOutputStream(rootPath + "/error.txt");
        for (var i = 0; i < dirSize; i++) {
            SearchComputed.createdDir(new File(rootPath + "/" + i));
        }
        cntInit();
        SearchComputed.fileProcess(20, 40, (line, schema) -> {
            var index = (Number) SearchComputed.getParquetFieldValue(line, schema.getType("orderId"), schema.getFieldIndex("orderId"), 0);
            var data = (List<String>) SearchComputed.getParquetFieldValue(line, schema.getType("data"), schema.getFieldIndex("data"), 0);
            add(new HashSet<>(data), index.intValue());
        });
        errorOut.close();
        if (!map.isEmpty()) {
            map.forEach((k, v) -> {
                System.out.printf("%s %d %d %n", k, v.total, v.cnt);
                if (v.write != null) {
                    try {
                        v.write.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
    }

    private static final Map<String, Integer> wordCntMap = new HashMap<>();

    private static FileOutputStream errorOut;

    private static void cntInit() throws Exception {
        var read = new BufferedReader(new FileReader(rootPath + "/words"));
        String line = null;
        while ((line = read.readLine()) != null) {
            var ws = line.split(" ");
            var cnt = Integer.parseInt(ws[1]);
            // 一个词在100篇文献一下出现  不管这个词
            if (cnt < 100) continue;
            wordCntMap.put(ws[0], Integer.parseInt(ws[1]));
        }
    }

    private static final ConcurrentHashMap<String, Data> map = new ConcurrentHashMap<>();

    static final AtomicInteger ioCnt = new AtomicInteger(0);

    private static void add(Set<String> words, int id) {
        var queue = new LinkedList<>(words);
        String word = null;
        while ((word = queue.poll()) != null) {
            var total = wordCntMap.get(word);
            if (total == null) continue;
            var data = map.computeIfAbsent(word, v -> new Data(total));
            a:
            if (data.lock.compareAndSet(false, true)) {
                try {
                    var _index = data.getIndex();
                    System.arraycopy(ByteUtil.intToBytes(id), 0, data.bytes, _index << 2, 4);
                    data.cnt++;
                    if (_index < Data.len - 1 && data.cnt < data.total) break a;
                    try {
                        syncWord(word, data);
                    } catch (Exception e1) {
                        try {
                            errorOut.write(String.format("%s %s\n", word, e1).getBytes(StandardCharsets.UTF_8));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    if (data.cnt == data.total) {
                        data.clean();
                        map.remove(word);
                        System.gc();
                        System.out.printf("%s -> %d 完成 io=%d\n", word, data.total, ioCnt.get());
                    }
                } finally {
                    data.lock.set(false);
                }
            } else {
                queue.offer(word);
            }
        }
    }


    private static void syncWord(String word, Data data) throws Exception {
        var write = data.write;
        if (write == null) {
            var _ioCnt = ioCnt.getAndIncrement();
            if (_ioCnt > 30000) {
                a:
                for (var i = 0; i < 10; i++) {
                    for (var key : map.keySet()) {
                        var v = map.get(key);
                        if (v.lock.compareAndSet(false, true)) {
                            try {
                                if (v.write != null) {
                                    try {
                                        data.write.close();
                                        data.write = null;
                                        System.out.println("io不够 关闭其他：" + ioCnt.addAndGet(-1));
                                        break a;
                                    } catch (IOException e) {
                                        System.out.println("关闭失败:" + e);
                                    }
                                }
                            } finally {
                                v.lock.set(false);
                            }
                        }
                    }
                }
            }
            write = new FileOutputStream(rootPath + "/" + (word.hashCode() & (dirSize - 1)) + "/" + word, true);
            data.write = write;
        }
        write.write(data.bytes, 0, (data.getIndex() << 2) + 4);
        write.flush();
        if (data.cnt == data.total || data.total < 100000) {
            data.write.close();
            data.write = null;
            ioCnt.getAndAdd(-1);
        }
    }


    static class Data {
        static final int len = Integer.parseInt(System.getProperty("len", "10240"));
        static int len1 = len << 2;
        final AtomicBoolean lock = new AtomicBoolean(false);
        int total;
        int cnt = 0;
        byte[] bytes = new byte[len1];
        FileOutputStream write;

        public Data(int total) {
            Arrays.fill(bytes, (byte) 0);
            this.total = total;
        }

        void clean() {
            bytes = null;
            write = null;
        }

        int getIndex() {
            return cnt & (len - 1);
        }
    }

}
