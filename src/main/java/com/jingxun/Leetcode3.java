package com.jingxun;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author by keray
 * date:2020/4/4 5:17 PM
 * 给定一个字符串 s ，请你找出其中不含有重复字符的 最长子串 的长度。
 */
public class Leetcode3 {

    public static void main(String[] args) {
        System.out.println(new Leetcode3().lengthOfLongestSubstring(" "));
    }

    public int lengthOfLongestSubstring(String s) {
        int max = 0;
        int len = 0;
        char[] chars = s.toCharArray();
        StringBuilder builder = new StringBuilder();
        for (char ch : chars) {
            int index = builder.indexOf(String.valueOf(ch));
            if (index >= 0) {
                len -= (index + 1);
                builder.delete(0, index + 1);
            }
            builder.append(ch);
            len++;
            max = Math.max(max, len);
        }
        return max;
    }
}
