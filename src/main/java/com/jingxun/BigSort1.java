package com.jingxun;

import cn.hutool.core.util.ByteUtil;

import java.io.FileInputStream;
import java.nio.ByteOrder;
import java.util.Arrays;

public class BigSort1 {
    public static void main(String[] args) throws Exception {
        var now = System.currentTimeMillis();
        try (var reader = new FileInputStream("/Users/keray/Downloads/xxx/用")) {
            var bs = new byte[65536];
            var array = new int[reader.available() >>> 2];
            var len = 0;
            var index = 0;
            while ((len = reader.read(bs)) > 0) {
                for (var i = 0; i < len; i += 4) {
                    array[index++] = ByteUtil.bytesToInt(bs, i, ByteOrder.LITTLE_ENDIAN);
                }
            }
            System.out.println(System.currentTimeMillis() - now);
            var before = new int[100];
            var end = new int[100];
            System.arraycopy(array, 0, before, 0, before.length);
            System.arraycopy(array, array.length - 100, end, 0, end.length);
            System.out.println(Arrays.toString(before));
            System.out.println(Arrays.toString(end));
        }
    }
    public static void main1(String[] args) throws Exception {
        var now = System.currentTimeMillis();
        try (var reader = new FileInputStream("/Users/keray/Downloads/xxx1/用")) {
            var bs = new byte[4];
            var array = new int[reader.available() >>> 2];
            var len = 0;
            var index = 0;
            while ((len = reader.read(bs)) > 0) {
                array[index++] = ByteUtil.bytesToInt(bs);
            }
            System.out.println(System.currentTimeMillis() - now);
            var before = new int[100];
            var end = new int[100];
            System.arraycopy(array, 0, before, 0, before.length);
            System.arraycopy(array, array.length - 100, end, 0, end.length);
            System.out.println(Arrays.toString(before));
            System.out.println(Arrays.toString(end));
        }
    }
}
