package ru.urururu.utils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * @author <a href="mailto:dmitriy.g.matveev@gmail.com">Dmitry Matveev</a>
 */
public class EntityLocker<K> {
    private final Map<K, LockInfo> locks = new HashMap<>();
    private Map<Thread, List<LockInfo>> threads = new ConcurrentHashMap<>();

    public void doWith(K key, Runnable runnable) {
        LockInfo info = get(key);
        try {
            info.lock.lock();
            try {
                runnable.run();
            } finally {
                info.lock.unlock();
            }
        } finally {
            release(info);
        }
    }

    private void checkForPossibleDeadlocks(LockInfo wanted) {
        if (wanted.lock.isHeldByCurrentThread()) {
            // ok, we already own it.
            return;
        }

        List<LockInfo> heldByCurrent = new ArrayList<>(threads.getOrDefault(Thread.currentThread(), Collections.emptyList()));

        for (int i = 0; i < heldByCurrent.size(); i++) {
            LockInfo held = heldByCurrent.get(i);

            if (held == wanted) {
                throw new DeadlockException();
            }

            for (Thread heldThread : held.lock.getQueuedThreads()) {
                heldByCurrent.addAll(threads.getOrDefault(heldThread, Collections.emptyList()));
            }
        }
    }

    private LockInfo get(K key) {
        synchronized (locks) {
            LockInfo result = locks.computeIfAbsent(key, new Function<K, LockInfo>() {
                @Override
                public LockInfo apply(K k) {
                    LockInfo info = new LockInfo(k);

                    info.lock = new ReentrantLockAdapter() {
                        @Override
                        public void lock() {
                            super.lock();
                            if (getHoldCount() == 1) {
                                // we've just acquired our first hold of this lock
                                List<LockInfo> threadLocks = threads.computeIfAbsent(Thread.currentThread(), k -> new ArrayList<>());
                                threadLocks.add(info);
                            }
                        }

                        @Override
                        public void unlock() {
                            if (getHoldCount() == 1) {
                                // we're about to release our last hold of this lock
                                List<LockInfo> threadLocks = threads.get(Thread.currentThread());
                                threadLocks.remove(info);
                                if (threadLocks.isEmpty()) {
                                    threads.remove(Thread.currentThread());
                                }
                            }
                            super.unlock();
                        }
                    };

                    return info;
                }
            });

            checkForPossibleDeadlocks(result);

            result.refCount++;

            return result;
        }
    }

    private void release(LockInfo info) {
        synchronized (locks) {
            if (--info.refCount == 0) {
                // releasing last reference
                locks.remove(info.key);
            }
        }
    }

    @Deprecated
    boolean isStateClean() {
        return locks.isEmpty() && threads.isEmpty();
    }

    private class LockInfo {
        private final K key;
        private ReentrantLockAdapter lock;
        private int refCount = 0;

        LockInfo(K key) {
            this.key = key;
        }
    }

    private static class ReentrantLockAdapter extends ReentrantLock {
        @Override
        public Collection<Thread> getQueuedThreads() {
            return super.getQueuedThreads();
        }
    }
}
