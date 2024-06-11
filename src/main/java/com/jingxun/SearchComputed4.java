package com.jingxun;

import cn.hutool.core.util.ByteUtil;
import cn.hutool.core.util.StrUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static cn.hutool.poi.excel.sax.ElementName.c;
import static cn.hutool.poi.excel.sax.ElementName.v;

/**
 * 生成词到ids的数据
 * 数据 -> [1,2,3]
 */
public class SearchComputed4 {

    static ThreadPoolExecutor pool = new ThreadPoolExecutor(30, 30, 100, TimeUnit.DAYS, new LinkedBlockingDeque<>());

    private static String rootPath;

    private static final int dirSize = 1024;

    public static void main(String[] args) throws Exception {
        System.out.println("参数：" + Arrays.toString(args));
        rootPath = args[0];
        if (StrUtil.isEmpty(rootPath)) return;
        errorOut = new FileOutputStream(rootPath + "/error.txt");
        for (var i = 0; i < dirSize; i++) {
            SearchComputed1.createdDir(new File(rootPath + "/" + i));
        }
        cntInit();
        Configuration configuration = new Configuration();
        var time = System.currentTimeMillis();
        var semaphore = new Semaphore(2);
        var latch = new AddDownLatch();
        try (var fs = FileSystem.get(configuration)) {
            Path path = new Path("/dataplat/OMDPV2/data/data-process-svc/export/yzh/search/allProcData");
            RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(path, true);
            locatedFileStatusRemoteIterator.next();
            while (locatedFileStatusRemoteIterator.hasNext()) {
                var ph = locatedFileStatusRemoteIterator.next().getPath();
                semaphore.acquire();
                latch.add();
                pool.execute(() -> {
                    try {
                        SearchComputed1.oneFileProcess(fs, ph, 200, (line, schema) -> {
                            var index = (Number) SearchComputed1.getParquetFieldValue(line, schema.getType("orderId"), schema.getFieldIndex("orderId"), 0);
                            var data = (List<String>) SearchComputed1.getParquetFieldValue(line, schema.getType("data"), schema.getFieldIndex("data"), 0);
                            add(new HashSet<>(data), index.intValue());
                            SearchComputed1.oneOk();
                        });
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        latch.countDown();
                        semaphore.release();
                    }
                });
            }
        }
        latch.await();
        System.out.println("耗时:" + (System.currentTimeMillis() - time));
        SearchComputed1.pool.shutdown();
        errorOut.close();
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
        for (var word : words) {
            var total = wordCntMap.get(word);
            if (total == null) continue;
            var data = map.computeIfAbsent(word, v -> new Data(total));
            a:
            synchronized (data.lock) {
                System.arraycopy(ByteUtil.intToBytes(id), 0, data.bytes, data.index, 4);
                data.index += 4;
                data.cnt++;
                if (data.index < data.bytes.length && data.cnt < data.total) break a;
                try {
                    syncWord(word, data);
                } catch (Exception ignore) {
                    try {
                        errorOut.write(String.format("%s\n", word).getBytes(StandardCharsets.UTF_8));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                data.index = 0;
            }
            if (data.cnt == data.total) {
                if (data.write != null) {
                    try {
                        ioCnt.getAndAdd(-1);
                        data.write.close();
                    } catch (Exception e) {
                        try {
                            errorOut.write(String.format("%s\n", word).getBytes(StandardCharsets.UTF_8));
                        } catch (IOException e1) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                data.clean();
                map.remove(word);
                System.gc();
                System.out.printf("%s -> %d 完成 io=%d\n", word, data.total, ioCnt.get());
            }
        }
    }


    private static void syncWord(String word, Data data) throws Exception {
        var write = data.write;
        if (write == null) {
            var _ioCnt = ioCnt.getAndIncrement();
            if (_ioCnt > 30000) {
                for (var key : map.keySet()) {
                    var v = map.get(key);
                    synchronized (v.lock) {
                        if (v.write != null) {
                            System.out.println("io不够 关闭其他：" + data.total);
                            try {
                                data.write.close();
                                data.write = null;
                                ioCnt.getAndAdd(-1);
                            } catch (IOException e) {
                                continue;
                            }
                            break;
                        }
                    }
                }
            }
            write = new FileOutputStream(rootPath + "/" + (word.hashCode() & (dirSize - 1)) + "/" + word, true);
            data.write = write;
        }
        write.write(data.bytes, 0, data.index);
        write.flush();
        if (data.total != data.cnt && data.total < 100000) {
            data.write.close();
            data.write = null;
            ioCnt.getAndAdd(-1);
        }
    }


    static class Data {
        final Object lock = new Object();
        int index = 0;
        int total;
        int cnt = 0;
        byte[] bytes = new byte[4096];
        FileOutputStream write;

        public Data(int total) {
            Arrays.fill(bytes, (byte) 0);
            this.total = total;
        }

        void clean() {
            bytes = null;
            write = null;
        }
    }

}
