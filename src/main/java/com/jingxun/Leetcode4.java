package com.jingxun;

/**
 * @author by keray
 * date:2020/4/4 5:17 PM
 * 给定两个大小分别为 m 和 n 的正序（从小到大）数组 nums1 和 nums2。请你找出并返回这两个正序数组的 中位数 。
 * <p>
 * 算法的时间复杂度应该为 O(log (m+n)) 。
 */
public class Leetcode4 {

    public static void main(String[] args) {
        System.out.println(new Leetcode4().findMedianSortedArrays(new int[]{1, 2}, new int[]{3, 4}));
    }

    public double findMedianSortedArrays(int[] nums1, int[] nums2) {
        int m = nums1.length;
        int n = nums2.length;
        int left = (m + n + 1) >>> 1;
        int right = (m + n + 2) >>> 1;
        return (findKth(nums1, 0, nums2, 0, left) + findKth(nums1, 0, nums2, 0, right)) / 2.0;
    }
    //i: nums1的起始位置 j: nums2的起始位置
    public int findKth(int[] nums1, int i, int[] nums2, int j, int k){
        if( i >= nums1.length) return nums2[j + k - 1];//nums1为空数组
        if( j >= nums2.length) return nums1[i + k - 1];//nums2为空数组
        if(k == 1){
            return Math.min(nums1[i], nums2[j]);
        }
        int midVal1 = (i + (k >>> 1) - 1 < nums1.length) ? nums1[i + (k >>> 1) - 1] : Integer.MAX_VALUE;
        int midVal2 = (j + (k >>> 1) - 1 < nums2.length) ? nums2[j + (k >>> 1) - 1] : Integer.MAX_VALUE;
        if(midVal1 < midVal2){
            return findKth(nums1, i + (k >>> 1), nums2, j , k - (k >>> 1));
        }else{
            return findKth(nums1, i, nums2, j + (k >>> 1) , k - (k >>> 1));
        }
    }
}
