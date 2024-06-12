package com.jingxun;

import cn.hutool.core.util.ByteUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 排序
 */
public class SearchComputed5 {
    public static void main(String[] args) throws Exception {
        var rootDir = args[0];
        var targetDir = args[1];
        var fileDel = "true".equals(args[2]);
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", args[3]);
        var dir = Paths.get(args[0]);
        AtomicReference<Function<File, Set<File>>> fileGet = new AtomicReference<>();
        fileGet.set(parent -> {
            try (Stream<Path> stream = Files.list(parent.toPath())) {
                return stream.flatMap(v -> {
                    var file = v.toFile();
                    if (file.isDirectory()) {
                        return fileGet.get().apply(file).stream();
                    }
                    return Stream.of(file);
                }).collect(Collectors.toSet());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        var now = System.currentTimeMillis();
        var cnt = new AtomicInteger();
        var fs = fileGet.get().apply(dir.toFile());
        var bufferSize = 65536;
        var bufferSizex = (65536 >>> 2) - 1;
        fs.parallelStream().forEach(file -> {
            try {
                if (file.toString().endsWith("DS_Store")) return;
                System.out.println("开始:" + file);
                int[] array;
                var bs = new byte[bufferSize];
                try (var input = new FileInputStream(file)) {
                    array = new int[input.available() >>> 2];
                    int len;
                    var index = 0;
                    while ((len = input.read(bs)) > 0) {
                        for (var i = 0; i < len; i += 4) {
                            if (Thread.currentThread().isInterrupted()) return;
                            array[index++] = ByteUtil.bytesToInt(bs, i, ByteOrder.LITTLE_ENDIAN);
                        }
                    }
                    sort(array);
                }
                var sortField = new File(file.toString().replace(rootDir, targetDir));
                sortField.getParentFile().mkdirs();
                try (var writer = new FileOutputStream(sortField)) {
                    int offset = 0;
                    for (int i = 0; i < array.length; i++) {
                        var val = array[i];
                        if (Thread.currentThread().isInterrupted()) return;
                        offset = i & bufferSizex;
                        System.arraycopy(ByteUtil.intToBytes(val), 0, bs, offset << 2, 4);
                        if (offset == bufferSizex) {
                            writer.write(bs);
                        }
                    }
                    if (offset != 0) {
                        writer.write(bs, 0, (offset + 1) << 2);
                    }
                    writer.flush();
                }
                if (fileDel) file.delete();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            System.out.printf("完成:%d %d %s%n", cnt.getAndIncrement(), fs.size(), file);
        });
        System.out.println("耗时:" + (System.currentTimeMillis() - now) / 1000 + "s");
    }


    public static void sort(int[] array) {
        if (array.length < 50000000) {
            Arrays.sort(array);
            return;
        }
        var sortArray = new byte[30000000];
        var max = 0;
        for (int x : array) {
            max = Math.max(max, x);
            var offset = x >>> 3;
            sortArray[offset] |= (byte) (1 << (x & 7));
        }
        var j = 0;
        for (var i = 0; i <= max; i++) {
            var offset = i >>> 3;
            var bit = 1 << (i & 7);
            if ((sortArray[offset] & bit) == bit) array[j++] = i;
        }
    }
}
