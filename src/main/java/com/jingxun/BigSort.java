package com.jingxun;

import cn.hutool.core.util.ByteUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * [3, 5, 7, 8, 10, 11, 14, 15, 16, 18, 22, 24, 26, 32, 33, 35, 36, 37, 38, 40, 46, 48, 50, 53, 54, 55, 56, 58, 59, 63, 64, 65, 66, 67, 69, 72, 73, 74, 75, 77, 81, 84,
 * 85, 86, 89, 90, 91, 92, 94, 95, 98, 102, 104, 106, 107, 109, 111, 113, 114, 117, 120, 121, 122, 125, 126, 128, 130, 134, 136, 142, 143, 144, 145, 146, 147, 150, 152,
 * 153, 160, 162, 163, 164, 165, 167, 168, 169, 171, 173, 178, 179, 180, 183, 186, 187, 189, 191, 192, 193, 194, 195]
 * <p>
 * [190812977, 190812978, 190812979, 190812981, 190812982, 190812983, 190812984, 190812985, 190812986, 190812987, 190812988, 190812989, 190812990,
 * 190812992, 190812993, 190812995, 190812997, 190812998, 190812999, 190813001, 190813002, 190813003, 190813004, 190813005, 190813006, 190813007, 190813008,
 * 190813009, 190813010, 190813011, 190813012, 190813013, 190813014, 190813015, 190813018, 190813019, 190813021, 190813023, 190813024, 190813025, 190813027, 190813028,
 * 190813029, 190813030, 190813031, 190813032, 190813033, 190813034, 190813035, 190813036, 190813037, 190813038, 190813039, 190813040, 190813041, 190813042,
 * 190813043, 190813044, 190813045, 190813047, 190813048, 190813049, 190813050, 190813051, 190813052, 190813053, 190813054, 190813055, 190813057, 190813058,
 * 190813059, 190813061, 190813062, 190813064, 190813065, 190813066, 190813067, 190813068, 190813069, 190813071, 190813072, 190813073, 190813074, 190813075,
 * 190813076, 190813077, 190813078, 190813079, 190813080, 190813081, 190813082, 190813083, 190813084, 190813085, 190813086, 190813087, 190813088, 190813099, 190813102, 190813119]
 */
public class BigSort {
    public static void main(String[] args) throws Exception {
        var dir = Paths.get("/Users/keray/Downloads/xxx1");
        var semaphore = new Semaphore(20);
        Set<Path> fs;
        try (Stream<Path> stream = Files.list(dir)) {
            fs = stream.collect(Collectors.toSet());
        }
        var now = System.currentTimeMillis();
        var cnt = new AtomicInteger();
        fs.parallelStream().forEach(file -> {
            try {
                semaphore.acquire(1);
                if (file.toString().endsWith("DS_Store")) return;
                System.out.println("开始:" + file);
                int[] array;
                var bs = new byte[65536];
                try (var input = new FileInputStream(file.toFile())) {
                    array = new int[input.available() / 4];
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
                var xs = file.toString().split("/");
                var sortField = new File("/Users/keray/Downloads/xxx/" + xs[xs.length - 1]);
                try (var writer = new FileOutputStream(sortField)) {
                    int offset = 0;
                    for (int i = 0; i < array.length; i++) {
                        var val = array[i];
                        if (Thread.currentThread().isInterrupted()) return;
                        offset = i & 16383;
                        System.arraycopy(ByteUtil.intToBytes(val), 0, bs, offset * 4, 4);
                        if (offset == 16383) {
                            writer.write(bs);
                        }
                    }
                    if (offset != 0) {
                        writer.write(bs, 0, offset * 4 + 4);
                    }
                    writer.flush();
                }
                Files.delete(file);
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                semaphore.release(1);
            }
            System.out.printf("完成:%d %d %s%n", cnt.getAndIncrement(), fs.size(), file);
        });
        System.out.println("耗时:" + (System.currentTimeMillis() - now) / 1000 + "s");
    }


    private static void sort(int[] array) {
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
