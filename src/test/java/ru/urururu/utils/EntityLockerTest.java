package ru.urururu.utils;

import org.hamcrest.core.IsEqual;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
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
    private static final int SHORT_TIMEOUT = 30;
    private static final int LONG_TIMEOUT = 60;
    private static final int BARRIER_TIMEOUT = 100;
    private static final int TEST_TIMEOUT = 500;

    private LockerState lockerState = new LockerState();
    private SuccessfulThreads successfulThreads = new SuccessfulThreads();

    @Rule
    public RuleChain chain = RuleChain.outerRule(lockerState)
            .around(successfulThreads)
            .around(new Timeout(TEST_TIMEOUT, TimeUnit.MILLISECONDS));

    private EntityLocker<String> locker;

    @Test
    public void testSingleThreadMultipleLocks() {
        Entity<String, Integer> alice = new Entity<>("alice", 500);
        Entity<String, Integer> bob = new Entity<>("bob", 500);

        locker.doWith(alice.key, new Runnable() {
            @Override
            public void run() {
                locker.doWith(bob.key, new Runnable() {
                    @Override
                    public void run() {
                        alice.value += 100;
                        bob.value -= 100;
                    }
                });
            }
        });
    }

    @Test
    public void testParallelForDifferent() throws InterruptedException {
        Entity<String, Integer> alice = new Entity<>("alice", 500);
        Entity<String, Integer> bob = new Entity<>("bob", 500);

        CyclicBarrier barrier = new CyclicBarrier(2);

        successfulThreads.addThread(new Runnable() {
            @Override
            public void run() {
                locker.doWith(alice.key, new Runnable() {
                    @Override
                    public void run() {
                        await(barrier); // ensure both threads hold some lock at the same time
                    }
                });
            }
        });
        successfulThreads.addThread(new Runnable() {
            @Override
            public void run() {
                locker.doWith(bob.key, new Runnable() {
                    @Override
                    public void run() {
                        await(barrier); // ensure both threads hold some lock at the same time
                    }
                });
            }
        });

        successfulThreads.startAll();
        successfulThreads.joinAll();
    }

    @Test
    public void testSequentialForSame() throws InterruptedException {
        Entity<String, Integer> alice = new Entity<>("alice", 500);

        CyclicBarrier barrier = new CyclicBarrier(2);

        successfulThreads.addThread(new Runnable() {
            @Override
            public void run() {
                locker.doWith(alice.key, () -> {
                    await(barrier); // 1. thread A got Alice.
                    alice.value += 10;
                });
            }
        });
        successfulThreads.addThread(new Runnable() {
            @Override
            public void run() {
                await(barrier); // 1. thread A got Alice.
                locker.doWith(alice.key, () -> alice.value += 20);
            }
        });

        successfulThreads.startAll();
        successfulThreads.joinAll();

        assertThat(alice.value, new IsEqual<>(530));
    }

    @Test
    public void testNoParallelForSame() throws InterruptedException {
        Entity<String, Integer> alice = new Entity<>("alice", 500);

        CyclicBarrier barrier = new CyclicBarrier(2);

        successfulThreads.addThread(new Runnable() {
            @Override
            public void run() {
                locker.doWith(alice.key, new Runnable() {
                    @Override
                    public void run() {
                        await(barrier); // 1. thread A got Alice.
                        await(barrier, TimeoutException.class); // 2. timeout before thread B get Alice.
                    }
                });
            }
        });
        successfulThreads.addThread(new Runnable() {
            @Override
            public void run() {
                await(barrier); // 1. thread A got Alice.
                locker.doWith(alice.key, new Runnable() {
                    @Override
                    public void run() {
                        await(barrier, BrokenBarrierException.class); // 3. got here after timeout in thread A.
                    }
                });
            }
        });

        successfulThreads.startAll();
        successfulThreads.joinAll();
    }

    @Test
    public void testReenterable() {
        Entity<String, Integer> alice = new Entity<>("alice", 500);

        locker.doWith(alice.key, new Runnable() {
            @Override
            public void run() {
                locker.doWith(alice.key, new Runnable() {
                    @Override
                    public void run() {
                        alice.value += 100;
                    }
                });
            }
        });

        assertThat(alice.value, new IsEqual<>(600));
    }

    @Test
    public void testDeadlock() throws InterruptedException {
        Entity<String, Integer> alice = new Entity<>("alice", 500);
        Entity<String, Integer> bob = new Entity<>("bob", 500);
        Entity<String, Integer> carlos = new Entity<>("carlos", 0);

        CyclicBarrier barrier = new CyclicBarrier(3);

        successfulThreads.addExpectedException(DeadlockException.class);

        successfulThreads.addThread(new Runnable() {
            @Override
            public void run() {
                locker.doWith(alice.key, new Runnable() {
                    @Override
                    public void run() {
                        await(barrier); // 1. all threads are locked on different keys
                        locker.doWith(bob.key, new Runnable() {
                            @Override
                            public void run() {
                                bob.value += 10;
                            }
                        });
                    }
                });
            }
        });

        successfulThreads.addThread(new Runnable() {
            @Override
            public void run() {
                locker.doWith(bob.key, new Runnable() {
                    @Override
                    public void run() {
                        await(barrier); // 1. all threads are locked on different keys
                        locker.doWith(carlos.key, new Runnable() {
                            @Override
                            public void run() {
                                carlos.value += 10;
                            }
                        });
                    }
                });
            }
        });

        successfulThreads.addThread(new Runnable() {
            @Override
            public void run() {
                locker.doWith(carlos.key, new Runnable() {
                    @Override
                    public void run() {
                        await(barrier); // 1. all threads are locked on different keys
                        locker.doWith(alice.key, new Runnable() {
                            @Override
                            public void run() {
                                alice.value += 10;
                            }
                        });
                    }
                });
            }
        });

        successfulThreads.startAll();
        successfulThreads.joinAll();

        assertThat(alice.value + bob.value + carlos.value, new IsEqual<>(1020));
    }

    @Test
    public void testNoParallelForDifferentAndGlobal1() throws InterruptedException {
        Entity<String, Integer> alice = new Entity<>("alice", 500);
        Entity<String, Integer> bob = new Entity<>("bob", 500);

        CyclicBarrier barrier = new CyclicBarrier(2);

        successfulThreads.addThread(new Runnable() {
            @Override
            public void run() {
                locker.doWithGlobal(new Runnable() {
                    @Override
                    public void run() {
                        await(barrier); // 1. thread A got global lock.
                        await(barrier, TimeoutException.class); // 2. timeout before thread B get Bob.
                    }
                });
            }
        });
        successfulThreads.addThread(new Runnable() {
            @Override
            public void run() {
                await(barrier); // 1. thread A got global lock.
                locker.doWith(bob.key, new Runnable() {
                    @Override
                    public void run() {
                        await(barrier, BrokenBarrierException.class); // 3. got here after timeout in thread A.
                    }
                });
            }
        });

        successfulThreads.startAll();
        successfulThreads.joinAll();
    }

    @Test
    public void testNoParallelForDifferentAndGlobal2() throws InterruptedException {
        Entity<String, Integer> alice = new Entity<>("alice", 500);
        Entity<String, Integer> bob = new Entity<>("bob", 500);

        CyclicBarrier barrier = new CyclicBarrier(2);

        successfulThreads.addThread(new Runnable() {
            @Override
            public void run() {
                locker.doWith(alice.key, new Runnable() {
                    @Override
                    public void run() {
                        await(barrier); // 1. thread A got Alice.
                        await(barrier, TimeoutException.class); // 2. timeout before thread B get global lock.
                    }
                });
            }
        });
        successfulThreads.addThread(new Runnable() {
            @Override
            public void run() {
                await(barrier); // 1. thread A got Alice.
                locker.doWithGlobal(new Runnable() {
                    @Override
                    public void run() {
                        await(barrier, BrokenBarrierException.class); // 3. got here after timeout in thread A.
                    }
                });
            }
        });

        successfulThreads.startAll();
        successfulThreads.joinAll();
    }

    @Test
    public void testSequentialForDifferentAndGlobal1() throws InterruptedException {
        Entity<String, Integer> alice = new Entity<>("alice", 500);
        Entity<String, Integer> bob = new Entity<>("bob", 500);

        CyclicBarrier barrier = new CyclicBarrier(2);

        successfulThreads.addThread(new Runnable() {
            @Override
            public void run() {
                locker.doWithGlobal(() -> {
                    await(barrier); // 1. thread A got global lock.
                    alice.value += 10;
                });
            }
        });
        successfulThreads.addThread(new Runnable() {
            @Override
            public void run() {
                await(barrier); // 1. thread A got global lock.
                locker.doWith(bob.key, () -> bob.value += 20); // this have to wait
            }
        });

        successfulThreads.startAll();
        successfulThreads.joinAll();

        assertThat(alice.value, new IsEqual<>(510));
        assertThat(bob.value, new IsEqual<>(520));
    }

    @Test
    public void testSequentialForDifferentAndGlobal2() throws InterruptedException {
        Entity<String, Integer> alice = new Entity<>("alice", 500);
        Entity<String, Integer> bob = new Entity<>("bob", 500);

        CyclicBarrier barrier = new CyclicBarrier(2);

        successfulThreads.addThread(new Runnable() {
            @Override
            public void run() {
                locker.doWith(alice.key, () -> {
                    await(barrier); // 1. thread A got Alice.
                    alice.value += 10;
                });
            }
        });
        successfulThreads.addThread(new Runnable() {
            @Override
            public void run() {
                await(barrier); // 1. thread A got Alice.
                locker.doWithGlobal(() -> bob.value += 20); // this have to wait, because we want global lock.
            }
        });

        successfulThreads.startAll();
        successfulThreads.joinAll();

        assertThat(alice.value, new IsEqual<>(510));
        assertThat(bob.value, new IsEqual<>(520));
    }

    @Test
    public void testReenterableGlobalAsLocal() {
        Entity<String, Integer> alice = new Entity<>("alice", 500);

        locker.doWithGlobal(new Runnable() {
            @Override
            public void run() {
                locker.doWith(alice.key, new Runnable() {
                    @Override
                    public void run() {
                        alice.value += 100;
                    }
                });
            }
        });

        assertThat(alice.value, new IsEqual<>(600));
    }

    @Test
    public void testTryDeadlock() throws InterruptedException {
        Entity<String, Integer> alice = new Entity<>("alice", 500);
        Entity<String, Integer> bob = new Entity<>("bob", 500);
        Entity<String, Integer> carlos = new Entity<>("carlos", 0);

        CyclicBarrier barrier = new CyclicBarrier(3);

        successfulThreads.addThread(new Runnable() {
            @Override
            public void run() {
                locker.doWith(alice.key, new Runnable() {
                    @Override
                    public void run() {
                        await(barrier); // 1. all threads are locked on different keys
                        try {
                            boolean done = locker.tryDoWith(bob.key, SHORT_TIMEOUT, TimeUnit.MILLISECONDS, new Runnable() {
                                @Override
                                public void run() {
                                    bob.value += 10;
                                }
                            });

                            assertThat(done, new IsEqual<>(false)); // short timeout
                        } catch (InterruptedException e) {
                            unreachable();
                        }
                    }
                });
            }
        });

        successfulThreads.addThread(new Runnable() {
            @Override
            public void run() {
                locker.doWith(bob.key, new Runnable() {
                    @Override
                    public void run() {
                        await(barrier); // 1. all threads are locked on different keys
                        try {
                            boolean done = locker.tryDoWith(carlos.key, LONG_TIMEOUT, TimeUnit.MILLISECONDS, new Runnable() {
                                @Override
                                public void run() {
                                    carlos.value += 10;
                                }
                            });

                            assertThat(done, new IsEqual<>(true));
                        } catch (InterruptedException e) {
                            unreachable();
                        }
                    }
                });
            }
        });

        successfulThreads.addThread(new Runnable() {
            @Override
            public void run() {
                locker.doWith(carlos.key, new Runnable() {
                    @Override
                    public void run() {
                        await(barrier); // 1. all threads are locked on different keys
                        try {
                            locker.tryDoWith(alice.key, LONG_TIMEOUT, TimeUnit.MILLISECONDS, new Runnable() {
                                @Override
                                public void run() {
                                    alice.value += 10; // executes after short timeout in first thread.
                                }
                            });
                        } catch (InterruptedException e) {
                            unreachable();
                        }
                    }
                });
            }
        });

        successfulThreads.startAll();
        successfulThreads.joinAll();

        assertThat(alice.value + bob.value + carlos.value, new IsEqual<>(1020));
    }

    private void unreachable() {
        fail("unreachable");
    }

    @Test
    public void testTryDeadlockInterrupt() throws InterruptedException {
        Entity<String, Integer> alice = new Entity<>("alice", 500);
        Entity<String, Integer> bob = new Entity<>("bob", 500);
        Entity<String, Integer> carlos = new Entity<>("carlos", 0);

        CyclicBarrier barrier = new CyclicBarrier(4);

        Thread aliceThread = successfulThreads.addThread(new Runnable() {
            @Override
            public void run() {
                locker.doWith(alice.key, new Runnable() {
                    @Override
                    public void run() {
                        await(barrier); // 1. all threads are locked on different keys
                        try {
                            boolean done = locker.tryDoWith(bob.key, LONG_TIMEOUT, TimeUnit.MILLISECONDS, new Runnable() {
                                @Override
                                public void run() {
                                    bob.value += 10;
                                }
                            });

                            assertThat(done, new IsEqual<>(false)); // short timeout
                            unreachable();
                        } catch (InterruptedException e) {
                            // expected
                            Thread.currentThread().interrupt();
                        }
                    }
                });
            }
        });

        successfulThreads.addThread(new Runnable() {
            @Override
            public void run() {
                locker.doWith(bob.key, new Runnable() {
                    @Override
                    public void run() {
                        await(barrier); // 1. all threads are locked on different keys
                        try {
                            boolean done = locker.tryDoWith(carlos.key, LONG_TIMEOUT, TimeUnit.MILLISECONDS, new Runnable() {
                                @Override
                                public void run() {
                                    carlos.value += 10;
                                }
                            });

                            assertThat(done, new IsEqual<>(true));
                        } catch (InterruptedException e) {
                            unreachable();
                        }
                    }
                });
            }
        });

        successfulThreads.addThread(new Runnable() {
            @Override
            public void run() {
                locker.doWith(carlos.key, new Runnable() {
                    @Override
                    public void run() {
                        await(barrier); // 1. all threads are locked on different keys
                        locker.doWith(alice.key, new Runnable() {
                            @Override
                            public void run() {
                                alice.value += 10; // executes after short timeout in first thread.
                            }
                        });
                    }
                });
            }
        });

        successfulThreads.startAll();

        await(barrier); // 1. all threads are locked on different keys
        aliceThread.interrupt();

        successfulThreads.joinAll();

        assertThat(alice.value + bob.value + carlos.value, new IsEqual<>(1020));
    }

    private void await(CyclicBarrier barrier) {
        try {
            barrier.await(BARRIER_TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
            fail(e.getMessage());
        }
    }

    private void await(CyclicBarrier barrier, Class<? extends Exception> expected) {
        try {
            barrier.await(BARRIER_TIMEOUT, TimeUnit.MILLISECONDS);
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
        List<Class<? extends Throwable>> expectedExceptions = new ArrayList<>();
        List<Class> unhandledExceptions = Collections.synchronizedList(new ArrayList<>());

        Thread addThread(Runnable r) {
            Thread thread = new Thread(r);

            thread.setUncaughtExceptionHandler((t, e) -> unhandledExceptions.add(e.getClass()));

            threads.add(thread);

            return thread;
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
                    expectedExceptions.clear();
                    unhandledExceptions.clear();
                    try {
                        statement.evaluate();
                    } finally {
                        threads.forEach(t -> assertEquals(Thread.State.TERMINATED, t.getState()));
                        assertEquals(expectedExceptions, unhandledExceptions);
                    }
                }
            };
        }

        public void addExpectedException(Class<? extends Throwable> exceptionClass) {
            expectedExceptions.add(exceptionClass);
        }
    }

    private class LockerState implements TestRule {
        @Override
        public Statement apply(Statement statement, Description description) {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    locker = new EntityLocker<>();
                    try {
                        statement.evaluate();
                    } finally {
                        assertThat(locker.isStateClean(), new IsEqual<>(true));
                    }
                }
            };
        }
    }
}