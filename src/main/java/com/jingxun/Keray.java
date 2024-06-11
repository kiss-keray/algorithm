package com.jingxun;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 给定一个排列有n个孔洞的模具A 和排列有n个补胶的模具B，模具B中只有m(0 < m <= n)个有效的补胶。孔洞可以重复补
 * 计算出使用X个模具B填补上模具A的所有孔洞，重复补的空洞的次数为Y，找出（X+Y/100）最小的模具B组合
 * todo
 */
public class Keray {
    public static void main(String[] args) throws Exception {
        System.out.println("数据".hashCode() & 1023);
    }
}
