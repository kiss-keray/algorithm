package com.jingxun;

import cn.hutool.core.util.ByteUtil;
import lombok.SneakyThrows;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.MessageType;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * 生成单个字的数据
 * 数 -> [1,2,3,4]
 */
public class SearchComputed1 {

    private static String dir = "/devdata/xxx";

    public static void main(String[] args) throws Exception {
        dir = args[0];
        SearchComputed.fileProcess(5, 50, SearchComputed1::lineWork);
        for (var item : blockMap.values()) {
            if (item.index > 0) {
                item.write.write(item.buffer, 0, item.index << 2);
            }
            item.write.flush();
            item.write.close();
        }
        SearchComputed.pool.shutdown();
    }


    final static ConcurrentHashMap<String, Block> blockMap = new ConcurrentHashMap<>(20000, 1);

    static String[] stopWords = Stream.of("一的不与个之也了但儿则又吱呀呃呗吗吧呜呢呵呸咋和咚咦咧咳哇哈哎哗哟哦哩哪哼唉啊啐啥啦喂喏喽嗡嗬嗯嗳嘎嘘嘛嘻嘿因好如或是".split("")).sorted().toArray(String[]::new);


    @SneakyThrows
    private static void lineWork(Group line, MessageType schema) {
        var orderId = (Number) SearchComputed.getParquetFieldValue(line, schema.getType("orderId"), schema.getFieldIndex("orderId"), 0);
        var data = (List<String>) SearchComputed.getParquetFieldValue(line, schema.getType("data"), schema.getFieldIndex("data"), 0);
        var ws = data.stream().flatMap(v -> Stream.of(v.split("")))
                .distinct().filter(v -> Arrays.binarySearch(stopWords, v) < 0).toList();
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
                System.arraycopy(ByteUtil.intToBytes(orderId.intValue()), 0, block.buffer, block.index << 2, 4);
                block.index++;
                if (block.index == Block.len) {
                    block.write.write(block.buffer, 0, block.index << 2);
                    block.write.flush();
                    block.index = 0;
                }
            }
        }
    }


    static class Block {
        final static int len = 4048;
        private FileOutputStream write;
        private final byte[] buffer = new byte[len << 2];
        private int index;
        private final Object lock = new Object();
    }

}
