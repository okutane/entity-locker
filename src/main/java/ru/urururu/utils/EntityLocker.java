package ru.urururu.utils;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * @author <a href="mailto:dmitriy.g.matveev@gmail.com">Dmitry Matveev</a>
 */
public abstract class EntityLocker<K> {
    public static <K> EntityLocker.Builder<K> forKeysOf(Class<K> keyClass) {
        return new Builder<>();
    }

    public LockContext with(K key) {
        Lock lock = get(key);
        lock.lock();

        return new LockContext() {
            @Override
            public void close() {
                lock.unlock();
            }
        };
    }

    public abstract Lock get(K key);

    public static class Builder<K> {
        Function<K, Lock> lockFactory;

        public Builder<K> withBucketOfLocks(int numLocks) {
            Lock[] locks = new Lock[numLocks];
            for (int i = 0; i < numLocks; i++) {
                locks[i] = new ReentrantLock();
            }

            lockFactory = new Function<K, Lock>() {
                @Override
                public Lock apply(K k) {
                    return locks[k == null ? 0 : k.hashCode() % numLocks];
                }
            };

            return this;
        }

        public EntityLocker build() {
            return new EntityLocker<K>() {
                @Override
                public Lock get(K key) {
                    return lockFactory.apply(key);
                }
            };
        }
    }
}
