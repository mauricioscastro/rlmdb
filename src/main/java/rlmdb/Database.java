package rlmdb;

import com.google.common.hash.Hashing;
import com.google.common.primitives.Bytes;
import org.apache.bookkeeper.client.BKException;

import static org.fusesource.lmdbjni.Constants.bytes;

public class Database {

    private final Env env;
    private final byte[] id;
    private final String name;
    private final org.fusesource.lmdbjni.Database db;
    private final byte[] OPERATION_PUT;
    private final byte[] OPERATION_DEL;

    Database(String name, Env env) {
        id = Hashing.crc32c().hashBytes(bytes(name)).asBytes();
        this.name = name;
        this.env = env;
        db = env.env.openDatabase(name);
        OPERATION_PUT = Bytes.concat(id, new byte[]{0x01});
        OPERATION_DEL = Bytes.concat(id, new byte[]{0x00});
    }

    public byte[] get(byte[] key) {
        return db.get(key);
    }

    public byte[] get(ReadTransaction rtx, byte[] key) {
        return db.get(rtx.tx, key);
    }

    public void put(WriteTransaction tx, byte[] key, byte[] value) throws Exception {
        tx.add(Bytes.concat(OPERATION_PUT, new byte[]{(byte)key.length}, key, value));
    }

    public void put(byte[] key, byte[] value) throws Exception {
        try(WriteTransaction tx = env.createWriteTransaction()) {
            put(tx, key, value);
        }
    }

    public void delete(WriteTransaction tx, byte[] key) throws Exception {
        tx.add(Bytes.concat(OPERATION_DEL, new byte[]{(byte)key.length}, key));
    }

    public void delete(byte[] key) throws Exception {
        try(WriteTransaction tx = env.createWriteTransaction()) {
            delete(tx, key);
        }
    }

    public void drop() {
        db.drop(true);
        env.dropDatabase(id);
    }

    public byte[] getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void close() {
        db.close();
    }
}
