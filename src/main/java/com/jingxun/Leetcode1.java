package com.jingxun;

import java.util.Arrays;

/**
 * @author by keray
 * date:2020/4/4 5:17 PM
 * 给定一个数组 和目标数  找出数组中两个数字和等于目标数字的下标
 */
public class Leetcode1 {

    public static void main(String[] args) {
        System.out.println(Arrays.toString(new Leetcode1().twoSum(
                new int[]{150,24,79,50,88,345,3},
                200
        )));
    }

    public int[] twoSum(int[] nums, int target) {
        int[] copy = new int[nums.length >>> 1];
        int vl = 0;
        int vl1 = 0;
        int end = 0;
        for (int i = 0; i < nums.length; i++) {
            vl = target - nums[i];
            vl1 = i;
            if (binaryFetchContains(copy, vl, end)) {
                break;
            }
            if (binaryFetchContains1(copy, nums[i], end)) end++;
        }
        for (int i = 0; i < vl1; i++) {
            if (nums[i] == vl) return new int[]{vl1, i};
        }
        return null;
    }

    public boolean binaryFetchContains(int[] arr, int key, int end) {
        if (end == 0) return false;
        int start = 0;
        while (start <= end) {
            int middle = (start + end) >>> 1;
            if (key < arr[middle]) {
                end = middle - 1;
            } else if (key > arr[middle]) {
                start = middle + 1;
            } else {
                return true;
            }
        }
        return false;
    }

    private void add(int[] arr, int vl, int end, int index) {
        if (index == -1) {
            arr[end] = vl;
        } else {
            if (end - index >= 0) System.arraycopy(arr, index, arr, index + 1, end - index);
            arr[index] = vl;
        }
    }


    public boolean binaryFetchContains1(int[] arr, int vl, int arrEnd) {
        if (arrEnd == 0 || arr[arrEnd - 1] < vl) {
            add(arr, vl, arrEnd, -1);
            return true;
        }
        if (arr[0] > vl) {
            add(arr, vl, arrEnd, 0);
            return true;
        }
        int start = 0;
        int end = arrEnd;
        while (start <= end) {
            if (start == end) {
                int value = arr[start];
                if (value == vl) return false;
                if (value < vl) {
                    add(arr, vl, arrEnd, start + 1);
                } else {
                    add(arr, vl, arrEnd, start);
                }
                return true;
            }
            int middle = (start + end) >>> 1;
            boolean last = middle == start;
            if (vl < arr[middle]) {
                if (last) {
                    end = start;
                } else {
                    end = middle - 1;
                }
            } else if (vl > arr[middle]) {
                if (last) {
                    start = end;
                } else {
                    start = middle + 1;
                }
            } else {
                return false;
            }
        }
        throw new RuntimeException();
    }
}
