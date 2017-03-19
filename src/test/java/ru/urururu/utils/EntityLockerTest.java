package ru.urururu.utils;

import org.hamcrest.core.IsInstanceOf;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author <a href="mailto:dmitriy.g.matveev@gmail.com">Dmitry Matveev</a>
 */
public class EntityLockerTest {
    @Rule
    public SuccessfulThreads successfulThreads = new SuccessfulThreads();

    @Rule
    public Timeout timeout = new Timeout(500, TimeUnit.MILLISECONDS);

    private EntityLocker<String> locker;

    @Before
    public void setUp() {
        locker = EntityLocker.<String>forKeysOf(String.class).withBucketOfLocks(16).build();
    }

    @Test
    public void testSingleThreadMultipleLocks() {
        Entity<String, Integer> alice = new Entity<>("alice", 500);
        Entity<String, Integer> bob = new Entity<>("bob", 500);

        try (LockContext aliceCtx = locker.with(alice.key)) {
            try (LockContext bobCtx = locker.with(bob.key)) {
                alice.value += 100;
                bob.value -= 100;
            }
        }
    }

    @Test
    public void testParallelForDifferent() throws InterruptedException {
        Entity<String, Integer> alice = new Entity<>("alice", 500);
        Entity<String, Integer> bob = new Entity<>("bob", 500);

        CyclicBarrier barrier = new CyclicBarrier(2);

        successfulThreads.addThread(new Runnable() {
            @Override
            public void run() {
                try (LockContext ctx = locker.with(alice.key)) {
                    await(barrier); // ensure both threads hold some lock at the same time
                }
            }
        });
        successfulThreads.addThread(new Runnable() {
            @Override
            public void run() {
                try (LockContext ctx = locker.with(bob.key)) {
                    await(barrier); // ensure both threads hold some lock at the same time
                }
            }
        });

        successfulThreads.startAll();
        successfulThreads.joinAll();
    }

    @Test
    public void testNoParallelForSame() throws InterruptedException {
        Entity<String, Integer> alice = new Entity<>("alice", 500);

        CyclicBarrier barrier = new CyclicBarrier(2);

        successfulThreads.addThread(new Runnable() {
            @Override
            public void run() {
                try (LockContext ctx = locker.with(alice.key)) {
                    await(barrier); // 1. thread A got Alice.
                    await(barrier, TimeoutException.class); // 2. timeout before thread B get Alice.
                }
            }
        });
        successfulThreads.addThread(new Runnable() {
            @Override
            public void run() {
                await(barrier); // 1. thread A got Alice.
                try (LockContext ctx = locker.with(alice.key)) {
                    await(barrier, BrokenBarrierException.class); // 3. got here after timeout in thread A.
                }
            }
        });

        successfulThreads.startAll();
        successfulThreads.joinAll();
    }

    private void await(CyclicBarrier barrier) {
        try {
            barrier.await(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
            fail(e.getMessage());
        }
    }

    private void await(CyclicBarrier barrier, Class<? extends Exception> expected) {
        try {
            barrier.await(100, TimeUnit.MILLISECONDS);
            assertThat(null, new IsInstanceOf(expected));
        } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
            assertThat(e, new IsInstanceOf(expected));
        }
    }

    private static class Entity<K, V> {
        private final K key;
        private V value;

        Entity(K key, V initialValue) {
            this.key = key;
            this.value = initialValue;
        }
    }

    private static class SuccessfulThreads implements TestRule {
        List<Thread> threads = new ArrayList<>();
        List<Throwable> unhandledExceptions = Collections.synchronizedList(new ArrayList<>());

        void addThread(Runnable r) {
            Thread thread = new Thread(r);

            thread.setUncaughtExceptionHandler((t, e) -> unhandledExceptions.add(e));

            threads.add(thread);
        }

        void startAll() {
            threads.forEach(Thread::start);
        }

        void joinAll() throws InterruptedException {
            for (Thread thread : threads) {
                thread.join();
            }
        }

        @Override
        public Statement apply(Statement statement, Description description) {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    unhandledExceptions.clear();
                    try {
                        statement.evaluate();
                    } finally {
                        threads.forEach(t -> assertEquals(Thread.State.TERMINATED, t.getState()));
                        unhandledExceptions.forEach(Throwable::printStackTrace);
                        assertEquals(Collections.emptyList(), unhandledExceptions);
                    }
                }
            };
        }
    }
}