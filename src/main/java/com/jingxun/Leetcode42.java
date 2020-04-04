package com.jingxun;

/**
 * @author by keray
 * date:2020/4/4 5:17 PM
 * 给定 n 个非负整数表示每个宽度为 1 的柱子的高度图，计算按此排列的柱子，下雨之后能接多少雨水。
 */
public class Leetcode42 {
    public static int trap(int[] height) {
        return work(height, 0, height.length);
    }

    public static int work(int[] height, int start, int end) {
        if (start == end - 1 || start == end) {
            return 0;
        }
        int maxValue = -1,maxValueIndex = -1,aMaxValue = 0,aMaxValueIndex = 0;
        boolean flag = false;
        for (int i = start; i < end; i++) {
            if (height[i] > maxValue) {
                if (maxValue > -1) {
                    flag = true;
                }
                aMaxValue = maxValue;
                aMaxValueIndex = maxValueIndex;
                maxValue = height[i];
                maxValueIndex = i;
            } else {
                if (!flag) {
                    if (height[i] > aMaxValue) {
                        aMaxValue = height[i];
                        aMaxValueIndex = i;
                    }
                }
            }
        }
        int min, max;
        if (maxValueIndex > aMaxValueIndex) {
            min = aMaxValueIndex;
            max = maxValueIndex;
        } else {
            min = maxValueIndex;
            max = aMaxValueIndex;
        }
        int result = 0;
        for (int i = min + 1; i < max; i++) {
            result += (aMaxValue - height[i]);
        }
        result += work(height, start, min + 1);
        result += work(height, max, end);
        return result;
    }

    public static void main(String[] args) {
        int[] a = new int[]{};
        System.out.println(trap(a));
    }
}
