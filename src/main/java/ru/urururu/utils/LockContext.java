package ru.urururu.utils;

/**
 * @author <a href="mailto:dmitriy.g.matveev@gmail.com">Dmitry Matveev</a>
 */
public interface LockContext extends AutoCloseable {
    void close();
}
