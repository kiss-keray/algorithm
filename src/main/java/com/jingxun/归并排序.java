package com.jingxun;

/**
 * @author by keray
 * date:2020/4/4 3:51 PM
 */
public class 归并排序 {

    public static void main(String[] args) {
        int[] a = {
                15, 49, 5, 36, 74, 5, 62, 55, 0, 6, 55, 46, 44, 845, 4826, 11, 25, 14, 54, 55, 8, 7, 89, 23, 54, 566, 8, 15
        };
        a = sort(a);
        for (int k : a) {
            System.out.println(k);
        }
    }

    static int[] sort(int[] a) {
        if (a.length == 1) {
            return a;
        }
        if (a.length == 2) {
            int[] c = new int[2];
            c[0] = Math.min(a[0], a[1]);
            c[1] = Math.max(a[0], a[1]);
            return c;
        }
        return sum(sort(split(a)[0]), sort(split(a)[1]));
    }

    static int[][] split(int[] a) {
        int[][] b = new int[2][];
        b[0] = new int[a.length >>> 1];
        b[1] = (a.length & 1) == 0 ? new int[a.length >>> 1] : new int[(a.length >>> 1) + 1];
        for (int i = 0; i < a.length; i++) {
            if (i < a.length >>> 1) {
                b[0][i] = a[i];
            } else {
                b[1][i - (a.length >>> 1)] = a[i];
            }
        }
        return b;
    }

    static int[] sum(int[] a, int[] b) {
        int[] c = new int[a.length + b.length];
        int k = 0, w = 0;
        for (int i = 0; i < c.length; i++) {
            if (k < a.length && (w >= b.length || a[k] < b[w])) {
                c[i] = a[k];
                k++;
            } else {
                c[i] = b[w];
                w++;
            }
        }
        return c;
    }

}
