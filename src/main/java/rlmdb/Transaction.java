package rlmdb;

import java.io.Closeable;

public abstract class Transaction implements Closeable {

    final Env env;

    Transaction(Env env) {
        this.env = env;
    }

    abstract public void commit();
    abstract public void abort();
    abstract public boolean isReadOnly();
    abstract public void close();
}
