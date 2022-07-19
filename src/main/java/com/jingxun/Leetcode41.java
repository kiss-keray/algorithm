package com.jingxun;

/**
 * @author by keray
 * date:2020/4/4 5:17 PM
 * 给你一个未排序的整数数组 nums ，请你找出其中没有出现的最小的正整数。
 *
 * 请你实现时间复杂度为 O(n) 并且只使用常数级别额外空间的解决方案。
 */
public class Leetcode41 {

    public int firstMissingPositive(int[] nums) {
        for (int i = 0; i < nums.length; i++) {
            int k = nums[i];
            if (k == i + 1 || k > nums.length || k < 1) continue;
            while (k <= nums.length && k > 0) {
                int a = nums[k - 1];
                if (a == k) break;
                nums[k - 1] = k;
                k = a;
            }
        }
        for (int i = 0; i < nums.length; i++) {
            if (nums[i] != i + 1) return i + 1;
        }
        return nums.length + 1;
    }


    public static void main(String[] args) {
        System.out.println(new Leetcode41().firstMissingPositive(new int[]{0, 1, 2}));
    }
}
