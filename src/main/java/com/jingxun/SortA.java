package com.jingxun;

import java.util.Arrays;

/**
 * @author by keray
 * date:2020/4/4 3:21 PM
 * 排序二位数组
 * 1,5,7,8
 * 2,6
 * 3,7,8,9
 *
 * 行 -> 有序
 * 列 | 有序
 */
public class SortA {

    public static int[] sort(int[][] arr) {
        int len = 0;
        int[] records = new int[arr.length];
        for (int i = 0; i < arr.length; i++) {
            len += arr[i].length;
            records[i] = 0;
        }
        int[] result = new int[len];
        for (int i = 0; i < result.length; i++) {
            result[i] = getMin(records, arr, 0);
        }
        return result;
    }

    private static int getMin(int[] records, int[][] arr, int index) {
        if (index == arr.length - 1) {
            return arr[index][records[index]++];
        }
        if (records[index] >= arr[index].length) {
            return getMin(records, arr, index + 1);
        }
        int minIndex = -1;
        for (int i = 0; i < index; i++) {
            if (records[i] < arr[i].length) {
                minIndex = i;
                break;
            }
        }
        int maxIndex = arr.length;
        for (int i = index + 1; i < arr.length; i++) {
            if (records[i] < arr[i].length && records[i] < records[index]) {
                maxIndex = i;
                break;
            }
        }
        int value = arr[index][records[index]];
        int value1 = minIndex == -1 ? Integer.MAX_VALUE : arr[minIndex][records[minIndex]];
        int value2 = maxIndex == arr.length ? Integer.MAX_VALUE : arr[maxIndex][records[maxIndex]];
        if (value < value1 && value < value2) {
            return arr[index][records[index]++];
        }
        if (value1 < value) {
            return getMin(records, arr, minIndex);
        } else {
            return getMin(records, arr, maxIndex);
        }
    }

    public static void main(String[] args) {
        int[][] a = new int[][]{
                {1, 34, 65, 78, 90},
                {2, 37},
                {3, 38, 68},
                {3, 38, 69},
        };
        System.out.println(Arrays.toString(sort(a)));
    }
}
