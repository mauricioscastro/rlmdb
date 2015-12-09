package rlmdb;

import java.io.IOException;

public class ReadTransaction extends Transaction {

    final org.fusesource.lmdbjni.Transaction tx;

    ReadTransaction(Env env) {
        super(env);
        tx = Env.env.createReadTransaction();
    }

    @Override
    public void close() {
        tx.close();
    }

    @Override
    public void commit() {
        tx.commit();
    }

    @Override
    public void abort() {
        tx.abort();
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }
}
