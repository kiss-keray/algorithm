package com.jingxun;

/**
 * @author by keray
 * date:2020/4/4 3:51 PM
 */
public class 插入排序 {
    static int[] sort(int[] _ints) {
        for (int i = 1; i < _ints.length; i++) {
            int k = _ints[i], j = i;
            for (; j > 0 && _ints[j - 1] > k; j--)
                _ints[j] = _ints[j - 1];
            _ints[j] = k;
        }
        return _ints;
    }

    public static void main(String[] args) {
        int[] ints = new int[100];
        for (int i = 0; i < 100; i++)
            ints[i] = (int) (Math.random() * 200);
        ints = sort(ints);
        for (int _int : ints)
            System.out.println(_int);
    }
}
