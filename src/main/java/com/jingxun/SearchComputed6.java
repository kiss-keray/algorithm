package com.jingxun;

import org.eclipse.jetty.util.ConcurrentHashSet;

import java.util.HashSet;
import java.util.List;

/**
 * 存储id->词的数据
 */
public class SearchComputed6 {

    public static void main(String[] args) throws Exception {
        var names = new ConcurrentHashSet<String>();
        SearchComputed.fileProcess(20, 100,(line, schema) -> {
            var data = (List<String>) SearchComputed.getParquetFieldValue(line, schema.getType("data"), schema.getFieldIndex("data"), 0);
            var set = new HashSet<>(data);
            var name = Thread.currentThread().toString();
            if (names.add(name)) {
                System.out.println(name);
            }
        });
    }

}
