package com.jingxun;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public class SearchComputed {

    static final float multiple = Float.parseFloat(System.getProperty("multiple", "1.0"));

    static ThreadPoolExecutor pool1 = new ThreadPoolExecutor(10, 200, 10, TimeUnit.SECONDS, new LinkedBlockingDeque<>(1));

    public static void fileProcess(int parallel, int parallel1, BiConsumer<Group, MessageType> fun) throws Exception {
        Configuration configuration = new Configuration();
        var time = System.currentTimeMillis();
        var semaphore = new Semaphore(parallel);
        var _ps = parallel1 * parallel;
        var size = (int) (_ps * multiple);
        ExecutorService pool;
        var semaphore1 = new Semaphore((int) (_ps * multiple) + parallel1);
        if (size > 3000) {
            var poolSize = 256;
            if (size > 40_000) poolSize = 1024;
            if (size > 10_000) poolSize = 512;
            System.setProperty("jdk.virtualThreadScheduler.maxPoolSize", String.valueOf(poolSize));
            System.setProperty("jdk.virtualThreadScheduler.parallelism", String.valueOf(poolSize));
            var COUNT = new AtomicInteger();
            pool = new ThreadPoolExecutor(size, size, 10,
                    TimeUnit.SECONDS, new LinkedBlockingDeque<>(),
                    r -> Thread.ofVirtual().name("virtual-sys-thread-", COUNT.getAndIncrement()).unstarted(r));
        } else {
            pool = new ForkJoinPool(size);
        }
        var latch = new AddDownLatch();
        try (var fs = FileSystem.get(configuration)) {
            Path path = new Path("/dataplat/OMDPV2/data/data-process-svc/export/yzh/search/allProcData");
            RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(path, true);
            while (locatedFileStatusRemoteIterator.hasNext()) {
                var _path = locatedFileStatusRemoteIterator.next().getPath();
                if (!_path.getName().endsWith(".parquet")) continue;
                latch.add();
                semaphore.acquire();
                pool1.execute(() -> {
                    try {
                        SearchComputed.oneFileProcess(fs, _path, parallel, semaphore1, pool, (v, v1) -> {
                            fun.accept(v, v1);
                            SearchComputed.oneOk();
                        });
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        semaphore.release();
                        latch.countDown();
                    }
                });
            }
            latch.await();
        }
        System.out.println("耗时:" + (System.currentTimeMillis() - time));
        pool.shutdown();
        pool1.shutdown();
    }

    public static void createdDir(File file) throws IOException {
        if (file.exists()) {
            try (var walk = Files.walk(file.toPath())) {
                walk.sorted(Comparator.reverseOrder()).forEach(v -> {
                    try {
                        Files.delete(v);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }
        if (!file.mkdirs()) throw new IOException("创建失败");
    }

    private static void oneFileProcess(FileSystem fs, Path filePath, int parallel, Semaphore semaphore, ExecutorService pool, BiConsumer<Group, MessageType> fun) throws Exception {
        GroupReadSupport readSupport = new GroupReadSupport();
        if (!filePath.getName().endsWith(".parquet")) return;
        var latch = new AddDownLatch();
        try (var reader = ParquetReader.builder(readSupport, filePath).build();
             var r = ParquetFileReader.open(HadoopInputFile.fromPath(filePath, fs.getConf()))) {
            MessageType schema = r.getFooter().getFileMetaData().getSchema();
            Group line = null;
            while ((line = reader.read()) != null && !Thread.currentThread().isInterrupted()) {
                Group finalLine = line;
                if (parallel > 1) {
                    latch.add();
                    semaphore.acquire();
                    pool.execute(() -> {
                        try {
                            fun.accept(finalLine, schema);
                        } finally {
                            latch.countDown();
                            semaphore.release();
                        }
                    });
                } else {
                    fun.accept(finalLine, schema);
                }
            }
            latch.await();
        }
        System.out.println(filePath + "  完成");
    }


    public static Object getParquetFieldValue(Group line, Type type, int fieldIndex, int index) {
        try {
            if (type instanceof PrimitiveType) {
                Class javaType = type.asPrimitiveType().getPrimitiveTypeName().javaType;
                if (javaType == Binary.class)
                    return line.getValueToString(fieldIndex, index);
                else if (javaType == long.class || javaType == Long.class)
                    return line.getLong(fieldIndex, index);
                else if (javaType == int.class || javaType == Integer.class)
                    return line.getInteger(fieldIndex, index);
                else if (javaType == boolean.class || javaType == Boolean.class)
                    return line.getBoolean(fieldIndex, index);
                else if (javaType == float.class || javaType == Float.class)
                    return line.getFloat(fieldIndex, index);
                else if (javaType == double.class || javaType == Double.class)
                    return line.getDouble(fieldIndex, index);
            } else if (type instanceof GroupType gt) {
                OriginalType orgType = type.getOriginalType();
                GroupType gType = (GroupType) type;
                if (orgType == OriginalType.LIST) {
                    Group group = line.getGroup(fieldIndex, index);
                    GroupType typeGroup = (GroupType) gType.getType(index);
                    Type eleType = typeGroup.getType(0);
                    int count = group.getFieldRepetitionCount(index);
                    List<Object> listData = new ArrayList<Object>();
                    for (int idx = 0; idx < count; idx++) {
                        Group eleGroup = group.getGroup(index, idx);
                        Object obj = getParquetFieldValue(eleGroup, eleType, 0, 0);
                        listData.add(obj);
                    }
                    return listData;
                } else if (orgType == null) {
                    return getParquetFieldValue(line.getGroup(0, 0), gt.getFields().stream().filter(v -> "word".equals(v.getName())).findFirst().get(), gt.getFieldIndex("word"), 0);
                }
            }
        } catch (Throwable e) {
        }
        return null;
    }


    static long time = 0;
    static long _cnt = 0;
    static long _total = 0;

    public synchronized static void oneOk() {
        _cnt++;
        var now = System.currentTimeMillis() / 1000;
        if (time == 0) {
            time = now;
            return;
        }
        if (time == now) return;
        var x = now - time;
        time = now;
        _total += _cnt;
        System.out.printf("%d %s/s%n", _total, _cnt / (float) x);
        _cnt = 0;
    }

}