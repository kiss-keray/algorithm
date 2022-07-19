package com.jingxun;

import cn.hutool.core.util.RandomUtil;

import java.util.Arrays;

public class 随机算法 {


    /**
     * ------|----------|---|-----|-----------|--------|---------|--------------
     *
     *
     * 依次随机
     *
     * @return
     */
    public static int[] demo1(int sum, int size) {
        if (sum < size || size == 0) throw new RuntimeException("总数必须大于人数");
        var balance = sum - size;
        int[] result = new int[size + 1];
        // 保证每个位置低保1
        for (var i = 0; i < size; i++) {
            result[i] += 1;
        }
        for (var i = 0; i < size - 1 && balance > 0; i++) {
            // 当前位置在余额里随机到金额 保底1
            int random = RandomUtil.randomInt(0, balance);
            result[i] += random;
            balance -= random;
        }
        // 最后一个位置直接加上余额
        result[size - 1] += balance;
        result[size] = 0;
        for (var i = 0; i < size; i++) {
            result[size] += result[i];
        }
        return result;
    }

    /**
     * 完全随机
     * 给每个人在范围内随机一个数字，将最后每个位置随机的百分比*总额
     *
     * @param sum
     * @param size
     * @return
     */
    public static int[] demo2(int sum, int size) {
        if (sum < size || size == 0) throw new RuntimeException();
        int[] result = new int[size + 1];
        int all = 0;
        for (var i = 0; i < size; i++) {
            // 1-100随机保证每个位置不为0
            var random = RandomUtil.randomInt(1, 100);
            all += random;
            result[i] = random;
        }
        // 随机数完成后去百分比设置金额
        for (var i = 0; i < size; i++) {
            result[i] = (int) (result[i] * (sum / (double) all));
        }
        result[size] = 0;
        for (var i = 0; i < size; i++) {
            result[size] += result[i];
        }
        return result;
    }


    /**
     * 公平随机算法，保证每个位置都在总数/位置的均值左右
     *
     * @param sum
     * @param size
     * @return
     */
    public static int[] demo3(int sum, int size) {
        if (sum < size || size == 0) throw new RuntimeException();
        int[] result = new int[size + 1];
        var balance = sum;
        for (var i = 0; i < size - 1; i++) {
            // 2 * 余额/剩余坑位  2 * 2 / 2 = 2 3 * 2/ 2 = 3
            var k = 2 * balance  / (size - i);
            var random = RandomUtil.randomInt(1, k);
            result[i] = random;
            balance -= random;
        }
        result[size - 1] = balance;
        result[size] = 0;
        for (var i = 0; i < size; i++) {
            result[size] += result[i];
        }
        return result;
    }

    public static void main(String[] args) {
        System.out.println(Arrays.toString(demo1(100, 10)));
        System.out.println(Arrays.toString(demo2(100, 10)));
        System.out.println(Arrays.toString(demo3(100, 10)));
    }
}
