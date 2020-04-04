package com.jingxun;

/**
 * @author by keray
 * date:2020/4/4 3:52 PM
 */
public class 堆排序 {
    public static void main(String[] args) {
        int[] a = {34, 43, 67, 72, 25, 63, 18, 82};
        a = sort(a);
        for (int k : a)
            System.out.println(k);
    }

    public static int[] create(int[] ints) {
        for (int i = 0; i < ints.length; i++) {
            ints = pa(ints, i);
        }
        return ints;
    }

    public static int[] pa(int[] ints, int a) {
        if (a == 0 || ints[(a - 1) / 2] > ints[a])
            return ints;
        int k = ints[a];
        ints[a] = ints[(a - 1) / 2];
        ints[(a - 1) / 2] = k;
        a = (a - 1) / 2;
        return pa(ints, a);
    }

    public static int[] sort(int[] ints) {
        ints = create(ints);
        for (int i = 0; i < ints.length; i++) {
            int k = ints[ints.length - 1 - i];
            ints[ints.length - i - 1] = ints[0];
            ints[0] = k;
            ints = ch(ints, 0, i + 1);
        }
        return ints;
    }

    public static int[] ch(int[] ints, int k, int j) {
        if (k * 2 + 2 > ints.length - j) {
            return ints;
        }
        if (k * 2 + 2 == ints.length - j) {
            if (ints[k] > ints[k * 2 + 1]) {
                return ints;
            } else {
                int c = ints[k];
                ints[k] = ints[k * 2 + 1];
                ints[k * 2 + 1] = c;
                return ints;
            }
        }
        if (ints[k] >= ints[k * 2 + 1] && ints[k] >= ints[k * 2 + 2]) {
            return ints;
        }
        if (ints[k * 2 + 1] > ints[k * 2 + 2]) {
            int c = ints[k];
            ints[k] = ints[k * 2 + 1];
            ints[k * 2 + 1] = c;
            k = k * 2 + 1;
        } else if (ints[k * 2 + 1] <= ints[k * 2 + 2]) {
            int c = ints[k];
            ints[k] = ints[k * 2 + 2];
            ints[k * 2 + 2] = c;
            k = k * 2 + 2;
        }
        return ch(ints, k, j);
    }
}
