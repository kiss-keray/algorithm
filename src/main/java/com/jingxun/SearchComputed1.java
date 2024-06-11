package com.jingxun;

import cn.hutool.core.util.ByteUtil;
import lombok.SneakyThrows;
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

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

/**
 * 生成单个字的数据
 * 数 -> [1,2,3,4]
 */
public class SearchComputed1 {

    static ThreadPoolExecutor pool = new ThreadPoolExecutor(200, 200, 100, TimeUnit.DAYS, new LinkedBlockingDeque<>());

    public static void main1(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        var map = new ConcurrentHashMap<String, List<String>>();
        try (var fs = FileSystem.get(configuration)) {
            Path path = new Path("file:///Users/keray/Downloads/part-00000-7591fc3c-243c-4211-bb86-32950f82e818-c000.gz.parquet");
            oneFileProcess(fs, path, 200, (line, schema) -> {
                var word = (String) getParquetFieldValue(line, schema.getType("word"), schema.getFieldIndex("word"), 0);
                var upword = (String) getParquetFieldValue(line, schema.getType("upword"), schema.getFieldIndex("upword"), 0);
                if (word == null || upword == null) return;
                if (word.length() != 2 || upword.length() != 1) return;
                var ls = map.computeIfAbsent(word, v -> new LinkedList<>());
                ls.add(upword);
            });
        }
        var f = new File("/Users/keray/Downloads/upword.txt");
        if (!f.exists()) f.createNewFile();
        try (var file = new FileWriter(f)) {
            map.forEach((k, v) -> {
                try {
                    file.write(String.format("%s %s%n", k, String.join(" ", v)));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            file.flush();
        }
        System.out.println("map:" + map);
        pool.shutdown();
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


    //    private final static String dir = "/Users/keray/Downloads/单个字倒排";
    private final static String dir = "/devdata/xxx";

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        var cnt = new AtomicInteger(0);
        var time = System.currentTimeMillis();
        var file = new File(dir);
        createdDir(file);
        try (var fs = FileSystem.get(configuration)) {
            Path path = new Path("/dataplat/OMDPV2/data/data-process-svc/export/yzh/search/allProcData");
            RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(path, true);
            locatedFileStatusRemoteIterator.next();
            while (locatedFileStatusRemoteIterator.hasNext()) {
                oneFileProcess(fs, locatedFileStatusRemoteIterator.next().getPath(), 200, SearchComputed1::lineWork);
            }
        }
        System.out.println("耗时:" + (System.currentTimeMillis() - time));
        for (var item : blockMap.values()) {
            item.write.flush();
            item.write.close();
        }
        pool.shutdown();
    }

    public static void oneFileProcess(FileSystem fs, Path filePath, int parallel, BiConsumer<Group, MessageType> fun) throws Exception {
        GroupReadSupport readSupport = new GroupReadSupport();
        if (!filePath.getName().endsWith(".parquet")) return;
        var reader = ParquetReader.builder(readSupport, filePath).build();
        var latch = new AddDownLatch();
        var semaphore = new Semaphore(parallel);
        try (var r = ParquetFileReader.open(HadoopInputFile.fromPath(filePath, fs.getConf()))) {
            MessageType schema = r.getFooter().getFileMetaData().getSchema();
            Group line = null;
            while ((line = reader.read()) != null) {
                Group finalLine = line;
                if (parallel > 1) {
                    latch.add();
                    semaphore.acquire();
                    pool.execute(() -> {
                        try {
                            fun.accept(finalLine, schema);
                        } finally {
                            semaphore.release();
                            latch.countDown();
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


    static class Block {
        private FileOutputStream write;
        private byte[] buffer = new byte[4048];
        private AtomicInteger cnt = new AtomicInteger(0);
        private final Object lock = new Object();
    }

    final static ConcurrentHashMap<String, Block> blockMap = new ConcurrentHashMap<>();

    static String[] stopWords = Stream.of("一的不与个之也了但儿则又吱呀呃呗吗吧呜呢呵呸咋和咚咦咧咳哇哈哎哗哟哦哩哪哼唉啊啐啥啦喂喏喽嗡嗬嗯嗳嘎嘘嘛嘻嘿因好如或是".split("")).sorted()
            .toArray(String[]::new);


    @SneakyThrows
    private static void lineWork(Group line, MessageType schema) {
        var index = (Number) getParquetFieldValue(line, schema.getType("orderId"), schema.getFieldIndex("orderId"), 0);
        var data = (List<String>) getParquetFieldValue(line, schema.getType("data"), schema.getFieldIndex("data"), 0);
        var ws = data.stream().flatMap(v -> Stream.of(v.split("")))
                .filter(v -> Arrays.binarySearch(stopWords, v) < 0)
                .distinct().toList();
        for (var w : ws) {
            var block = blockMap.computeIfAbsent(w, v -> {
                var item = new Block();
                try {
                    item.write = new FileOutputStream(dir + "/" + v);
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                }
                return item;
            });
            synchronized (block.lock) {
                var i = block.cnt.getAndIncrement();
                var bs = ByteUtil.intToBytes(index.intValue());
                System.arraycopy(bs, 0, block.buffer, 0, bs.length);
                block.write.write(ByteUtil.intToBytes(index.intValue()));
                if ((i & 1023) == 0) {
                    block.write.write(block.buffer);
                    block.write.flush();
                }
            }
        }
        oneOk();
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
}
