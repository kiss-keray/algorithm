package com.jingxun;

import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.ByteUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * 给定一个排列有n个孔洞的模具A 和排列有n个补胶的模具B，模具B中只有m(0 < m <= n)个有效的补胶。孔洞可以重复补
 * 计算出使用X个模具B填补上模具A的所有孔洞，重复补的空洞的次数为Y，找出（X+Y/100）最小的模具B组合
 * todo
 */
public class Keray {
    public static void main(String[] args) throws Exception {
        var x = new FileOutputStream("/Users/keray/Downloads/xxx1/1022/数据1", true);
        x.write(ByteUtil.intToBytes(Integer.MAX_VALUE), 0, 4);
        x.flush();
        Thread.sleep(50000);
        x.close();
        try (var reader = new FileInputStream("/Users/keray/Downloads/xxx1/1022/数据1")) {
            var array = new int[reader.available() / 4];
            System.out.println(reader.available() / 4.0);
            var bs = new byte[65536];
            int len;
            var index = 0;
            while ((len = reader.read(bs)) > 0) {
                for (var i = 0; i < len; i += 4) {
                    array[index++] = ByteUtil.bytesToInt(bs, i, ByteOrder.LITTLE_ENDIAN);
                }
            }
            System.out.println(Arrays.toString(ArrayUtil.sub(array, 0, 100)));
            System.out.println(Arrays.toString(ArrayUtil.sub(array, array.length - 100, array.length)));
        }
    }
}
