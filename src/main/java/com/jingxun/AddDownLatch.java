package com.jingxun;

import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * 可以动态的add新任务，只有所有新增的任务完成后await才会结束，
 */
public class AddDownLatch {
    private static final class Sync extends AbstractQueuedSynchronizer {

        protected int tryAcquireShared(int acquires) {
            return  (getState() == 0) ? 1 : -1;
        }

        protected boolean tryReleaseShared(int releases) {
            for (; ; ) {
                int c = getState();
                int nextc = c + releases;
                if (compareAndSetState(c, nextc))
                    return nextc == 0;
            }
        }
    }

    private final Sync sync;

    public AddDownLatch() {
        this.sync = new Sync();
    }

    public void await() throws InterruptedException {
        sync.acquireSharedInterruptibly(1);
    }

    public void countDown() {
        sync.releaseShared(-1);
    }

    public void add() {
        sync.releaseShared(1);
    }


}
