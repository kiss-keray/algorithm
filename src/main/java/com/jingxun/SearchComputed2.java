package com.jingxun;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.File;
import java.io.FileWriter;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 生成所有的词数据
 * 数据 10
 * 词->词在多少个ID中
 */
public class SearchComputed2 {

    static ThreadPoolExecutor pool = new ThreadPoolExecutor(30, 30, 100, TimeUnit.DAYS, new LinkedBlockingDeque<>());

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        var time = System.currentTimeMillis();
        var semaphore = new Semaphore(30);
        var latch = new AddDownLatch();
        var map = new ConcurrentHashMap<String, AtomicInteger>(20000, 1);
        try (var fs = FileSystem.get(configuration)) {
            Path path = new Path("/dataplat/OMDPV2/data/data-process-svc/export/yzh/search/allProcData");
            RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(path, true);
            locatedFileStatusRemoteIterator.next();
            while (locatedFileStatusRemoteIterator.hasNext()) {
                var ph = locatedFileStatusRemoteIterator.next().getPath();
                latch.add();
                semaphore.acquire();
                pool.execute(() -> {
                    try {
                        SearchComputed1.oneFileProcess(fs, ph, 200, (line, schema) -> {
                            var data = (List<String>) SearchComputed1.getParquetFieldValue(line, schema.getType("data"), schema.getFieldIndex("data"), 0);
                            for (var w : new HashSet<>(data)) {
                                var cnt = map.computeIfAbsent(w, v -> new AtomicInteger(0));
                                cnt.getAndIncrement();
                            }
                            SearchComputed1.oneOk();
                        });
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        semaphore.release();
                        latch.countDown();
                    }
                });
            }
        }
        latch.await();
        System.out.println("耗时:" + (System.currentTimeMillis() - time));
        var file = new File("./words");
        try (var write = new FileWriter(file)) {
            for (var w : map.entrySet()) {
                write.write(w.getKey() + " " + w.getValue().get() + "\n");
            }
            write.flush();
        }
        System.out.println("完成");
        SearchComputed1.pool.shutdown();
    }

}
