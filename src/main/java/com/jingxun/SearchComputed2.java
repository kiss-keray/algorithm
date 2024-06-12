package com.jingxun;

import java.io.File;
import java.io.FileWriter;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 生成所有的词数据
 * 数据 10
 * 词->词在多少个ID中
 */
public class SearchComputed2 {

    public static void main(String[] args) throws Exception {
        var map = new ConcurrentHashMap<String, AtomicInteger>(5000000, 1);
        SearchComputed.fileProcess(20, 1,(line, schema) -> {
            var data = (List<String>) SearchComputed.getParquetFieldValue(line, schema.getType("data"), schema.getFieldIndex("data"), 0);
            for (var w : new HashSet<>(data)) {
                var cnt = map.computeIfAbsent(w, v -> new AtomicInteger(0));
                cnt.getAndIncrement();
            }
        });
        var file = new File("./words");
        try (var write = new FileWriter(file)) {
            for (var w : map.entrySet()) {
                write.write(w.getKey() + " " + w.getValue().get() + "\n");
            }
            write.flush();
        }
    }

}
