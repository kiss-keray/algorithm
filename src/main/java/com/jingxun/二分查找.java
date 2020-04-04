package com.jingxun;

/**
 * @author by keray
 * date:2020/4/4 3:55 PM
 */
public class 二分查找 {
    public static boolean binaryFetchContains(int[] arr, int key) {
        int start = 0;
        int end = arr.length - 1;
        while (start <= end) {
            int middle = (start + end) / 2;
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

}
