package rlmdb;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;

public class WriteTransaction extends Transaction {

    private final LedgerHandle lh;

    public WriteTransaction(final Env env) {
        super(env);
        lh = env.createLedger();
    }

    public void add(byte[] data) throws BKException, InterruptedException {
        lh.addEntry(data);
    }

    @Override
    public void close() {
        abort();
    }

    @Override
    public void commit() {
        if (lh == null) return;
        env.commitLedger(lh);
    }

    @Override
    public void abort() {
        if (lh == null) return;
        env.abortLedger(lh);
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }
}

