package ru.urururu.utils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

/**
 * @param <K> the type of keys
 *
 * @author <a href="mailto:dmitriy.g.matveev@gmail.com">Dmitry Matveev</a>
 */
public class EntityLocker<K> {
    private final ReentrantReadWriteLock globalLock = new ReentrantReadWriteLock();
    private final Map<K, LockInfo> locks = new HashMap<>();
    private Map<Thread, List<LockInfo>> threads = new ConcurrentHashMap<>();

    /**
     * @param key key describing protected section
     * @param timeout the maximum time to wait for the each necessary lock
     * @param unit the time unit of the {@code time} argument
     * @return {@code true} if the lock was acquired and {@code false}
     *         if the waiting time elapsed before the lock was acquired
     * @param runnable the object whose {@code run} method is invoked if and when lock for specified key is acquired
     *
     * @throws InterruptedException if the current thread is interrupted
     *         while acquiring the lock (and interruption of lock
     *         acquisition is supported)
     *
     * @see Lock#tryLock()
     */
    public boolean tryDoWith(K key, long timeout, TimeUnit unit, Runnable runnable) throws InterruptedException {
        if (!globalLock.readLock().tryLock(timeout, unit)) {
            return false;
        }
        try {
            LockInfo info = get(key, false);
            try {
                return tryDoWithLock(runnable, info.lock, timeout, unit);
            } finally {
                release(info);
            }
        } finally {
            globalLock.readLock().unlock();
        }
    }

    /**
     * @param key object describing protected section
     * @param runnable the object whose {@code run} method is invoked when lock for specified key is acquired
     *
     * @throws DeadlockException if lock for requested key is held by some thread waiting for one of the locks held
     *         (explicitly or implicitly) by the current thread
     */
    public void doWith(K key, Runnable runnable) throws DeadlockException {
        globalLock.readLock().lock();
        try {
            LockInfo info = get(key);
            try {
                doWithLock(runnable, info.lock);
            } finally {
                release(info);
            }
        } finally {
            globalLock.readLock().unlock();
        }
    }

    /**
     * @param timeout the maximum time to wait for the global lock
     * @param unit the time unit of the {@code time} argument
     * @return {@code true} if the lock was acquired and {@code false}
     *         if the waiting time elapsed before the lock was acquired
     * @param runnable the object whose {@code run} method is invoked if and when lock for specified key is acquired
     *
     * @throws InterruptedException if the current thread is interrupted
     *         while acquiring the lock (and interruption of lock
     *         acquisition is supported)
     *
     * @see Lock#tryLock()
     */
    public boolean tryDoWithGlobal(Runnable runnable, long timeout, TimeUnit unit) throws InterruptedException {
        return tryDoWithLock(runnable, globalLock.writeLock(), timeout, unit);
    }

    /**
     * @param runnable the object whose {@code run} method is invoked when global lock is acquired
     */
    public void doWithGlobal(Runnable runnable) {
        doWithLock(runnable, globalLock.writeLock());
    }

    private boolean tryDoWithLock(Runnable runnable, Lock lock, long timeout, TimeUnit unit) throws InterruptedException {
        if (!lock.tryLock(timeout, unit)) {
            return false;
        }
        try {
            runnable.run();
            return true;
        } finally {
            lock.unlock();
        }
    }

    private void doWithLock(Runnable runnable, Lock lock) {
        lock.lock();
        try {
            runnable.run();
        } finally {
            lock.unlock();
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
        return get(key, true);
    }

    private LockInfo get(K key, boolean shouldCheckForDeadlocks) {
        synchronized (locks) { // internal lock, so external user can use locker in his synchronized blocks.
            LockInfo result = locks.computeIfAbsent(key, new Function<K, LockInfo>() {
                @Override
                public LockInfo apply(K k) {
                    LockInfo info = new LockInfo(k);

                    info.lock = new ReentrantLockAdapter() {
                        @Override
                        public void lock() {
                            super.lock();
                            afterLock();
                        }

                        @Override
                        public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
                            boolean result = super.tryLock(timeout, unit);

                            if (result) {
                                afterLock();
                            }

                            return result;
                        }

                        @Override
                        public void unlock() {
                            beforeUnlock();
                            super.unlock();
                        }

                        private void afterLock() {
                            if (getHoldCount() == 1) {
                                // we've just acquired our first hold of this lock
                                List<LockInfo> threadLocks = threads.computeIfAbsent(Thread.currentThread(), k -> new ArrayList<>());
                                threadLocks.add(info);
                            }
                        }

                        private void beforeUnlock() {
                            if (getHoldCount() == 1) {
                                // we're about to release our last hold of this lock
                                List<LockInfo> threadLocks = threads.get(Thread.currentThread());
                                threadLocks.remove(info);
                                if (threadLocks.isEmpty()) {
                                    threads.remove(Thread.currentThread());
                                }
                            }
                        }
                    };

                    return info;
                }
            });

            if (shouldCheckForDeadlocks) {
                checkForPossibleDeadlocks(result);
            }

            result.refCount++;

            return result;
        }
    }

    private void release(LockInfo info) {
        synchronized (locks) { // internal lock, so external user can use locker in his synchronized blocks.
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
