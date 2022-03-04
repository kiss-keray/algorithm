package com.jingxun;

/**
 * @author by keray
 * date:2020/4/4 5:17 PM
 * 给定一个数组 和目标数  找出数组中两个数字和等于目标数字的下标
 */
public class Leetcode2 {

    public static void main(String[] args) {
        ListNode l = new Leetcode2().addTwoNumbers(
                new ListNode(2, new ListNode(4, new ListNode(9))),
                new ListNode(5, new ListNode(6, new ListNode(4, new ListNode(9))))
        );
        for (; l != null; l = l.next) {
            System.out.print(l.val + "->");
        }
    }

    public static class ListNode {
        int val;
        ListNode next;

        ListNode() {
        }

        ListNode(int val) {
            this.val = val;
        }

        ListNode(int val, ListNode next) {
            this.val = val;
            this.next = next;
        }

        @Override
        public String toString() {
            return val + "->";
        }
    }

    public ListNode addTwoNumbers(ListNode l1, ListNode l2) {
        int last = 0;
        ListNode lastNode = l1;
        for (ListNode l11 = l1, l22 = l2; l11 != null || l22 != null; l11 = l11.next, l22 = l22 == null ? null : l22.next) {
            lastNode = l11 == null ? lastNode : l11;
            if (l11 == null || l22 == null) {
                if (l11 == null) {
                    l11 = l22;
                    l22 = null;
                    lastNode.next = l11;
                    lastNode = l11;
                }
                if (last == 0) {
                    break;
                }
                int k = lastNode.val + last;
                lastNode.val = k % 10;
                last = k / 10;
            } else {
                int k = l11.val + l22.val + last;
                l11.val = k % 10;
                last = k / 10;
            }
        }
        if (last != 0) {
            lastNode.next = new ListNode(last);
        }
        return l1;
    }
}
