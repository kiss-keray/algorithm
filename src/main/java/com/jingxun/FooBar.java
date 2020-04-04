package com.jingxun;

import java.util.concurrent.Semaphore;

/**
 * @author by keray
 * date:2020/4/4 6:52 PM
 *
1115. 交替打印FooBar
我们提供一个类：

class FooBar {
public void foo() {
    for (int i = 0; i < n; i++) {
      print("foo");
    }
}

public void bar() {
    for (int i = 0; i < n; i++) {
      print("bar");
    }
}
}
两个不同的线程将会共用一个 FooBar 实例。其中一个线程将会调用 foo() 方法，另一个线程将会调用 bar() 方法。

请设计修改程序，以确保 "foobar" 被输出 n 次。

来源：力扣（LeetCode）
链接：https://leetcode-cn.com/problems/print-foobar-alternately
著作权归领扣网络所有。商业转载请联系官方授权，非商业转载请注明出处。

 */
public class FooBar {
    private int n;

    Semaphore a = new Semaphore(1);
    Semaphore b = new Semaphore(0);

    public FooBar(int n) {
        this.n = n;
    }

    public void foo(Runnable printFoo) throws InterruptedException {

        for (int i = 0; i < n; i++) {
            a.acquire();
            // printFoo.run() outputs "foo". Do not change or remove this line.
            printFoo.run();
            b.release();
        }
    }

    public void bar(Runnable printBar) throws InterruptedException {

        for (int i = 0; i < n;i++) {
            b.acquire();
            // printBar.run() outputs "bar". Do not change or remove this line.
            printBar.run();
            a.release();
        }
    }
}
