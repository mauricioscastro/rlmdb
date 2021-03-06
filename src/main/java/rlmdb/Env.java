package rlmdb;

import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Bytes;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.CancelLeadershipException;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.jmx.ManagedUtil;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.fusesource.lmdbjni.Constants;

import javax.management.JMException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.fusesource.lmdbjni.Constants.bytes;

public class Env implements LeaderSelectorListener {

    private static final Logger logger = Logger.getLogger(Env.class);

    private static final String ZKPATH = "/rlmdb";
    private static final String DB_NAME_MAP_PATH = ZKPATH + "/dbmap";
    private static final String ELECTION_PATH = ZKPATH + "/election";
    private static final String LOG_LIST_PATH = ZKPATH + "/log";
    private static final String LOG_REMOVED_PATH = LOG_LIST_PATH + "/removed";

    private static QuorumPeer quorumPeer;
    private static QuorumPeerConfig quorumPeerConfig;
    private static ServerCnxnFactory cnxnFactory;

    private static ServerConfiguration bookieConfig;
    private static BookieServer bookie;
    private static CuratorFramework curator;

    private static LeaderSelector leaderSelector;

    private static BookKeeper bookkeeper;

    private final long myid;

    private volatile boolean leader = false;

    private final String lastProcessedLedgerPath;

    static final org.fusesource.lmdbjni.Env env = new org.fusesource.lmdbjni.Env();

    public Env(long myid) {
        this.myid = myid;
        lastProcessedLedgerPath = LOG_REMOVED_PATH + "/" + myid;
    }

    public void open(String path, Properties zk, Properties bk) throws InterruptedException, BookieException, KeeperException, IOException, ReplicationException.CompatibilityException, QuorumPeerConfig.ConfigException, ConfigurationException, ReplicationException.UnavailableException {
        open(path, 0, 0644, zk, bk);
    }

    public void open(String path, int flags, Properties zk, Properties bk) throws InterruptedException, BookieException, KeeperException, IOException, ReplicationException.CompatibilityException, QuorumPeerConfig.ConfigException, ConfigurationException, ReplicationException.UnavailableException {
        open(path, flags, 0644, zk, bk);
    }

    public void open(String path, int flags, int mode, Properties zk, Properties bk) throws IOException, QuorumPeerConfig.ConfigException, InterruptedException, BookieException, KeeperException, ReplicationException.CompatibilityException, ReplicationException.UnavailableException, ConfigurationException {
        env.open(path, flags, mode);
        startZookeeper(path, zk);
        startBookKeeper(path, bk);
    }

    public void close() {
        if(bookkeeper != null) try { bookkeeper.close(); } catch (Exception e) { logger.warn("",e); }
        if(curator != null) curator.close();
        if(bookie != null) bookie.shutdown();
        if(quorumPeer != null) quorumPeer.shutdown();
        env.close();
    }

    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {
        synchronized (this) {
            System.out.println("becoming leader");
            leader = true;
            try {
                while (true) {
                    this.wait();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                leader = false;
            }
        }
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        if ( (newState == ConnectionState.SUSPENDED) || (newState == ConnectionState.LOST) ) {
            throw new CancelLeadershipException();
        }
    }

    public boolean leader() {
        return leader;
    }


    public void setMapSize(long size) {
        env.setMapSize(size);
    }

    public void setMaxDbs(long size) {
        env.setMaxDbs(size);
    }

    public Database openDatabase(String name) throws Exception {
        createZKPath(DB_NAME_MAP_PATH + "/" + BaseEncoding.base16().encode(Hashing.crc32c().hashBytes(bytes(name)).asBytes()));
        return new Database(name, this);
    }

    public int getClusterSize() {
        return quorumPeerConfig == null ? -1 : quorumPeerConfig.getServers().size();
    }

    public ReadTransaction createReadTransaction() {
        return new ReadTransaction(this);
    }

    public WriteTransaction createWriteTransaction() {
        return new WriteTransaction(this);
    }

    public static long byteArrayToLong(byte[] array, int offset) {
        return ((long)(array[offset]   & 0xff) << 56) |
                ((long)(array[offset+1] & 0xff) << 48) |
                ((long)(array[offset+2] & 0xff) << 40) |
                ((long)(array[offset+3] & 0xff) << 32) |
                ((long)(array[offset+4] & 0xff) << 24) |
                ((long)(array[offset+5] & 0xff) << 16) |
                ((long)(array[offset+6] & 0xff) << 8) |
                ((long)(array[offset+7] & 0xff));
    }

    public static byte[] longToByteArray(long l) {
        return new byte[] {
                (byte)(0xff & (l >> 56)),
                (byte)(0xff & (l >> 48)),
                (byte)(0xff & (l >> 40)),
                (byte)(0xff & (l >> 32)),
                (byte)(0xff & (l >> 24)),
                (byte)(0xff & (l >> 16)),
                (byte)(0xff & (l >> 8)),
                (byte)(0xff & l)
        };
    }

    public static long byteArrayToLong(byte[] array) {
        return byteArrayToLong(array,0);
    }

    LedgerHandle createLedger() {
        if(!leader) throw new NotLeaderException();
        LedgerHandle lh = null;
        int n = getClusterSize();
        do try {
            try {
                final int m = n > 2 ? n - 1 : n;
                lh = Env.bookkeeper.createLedger(n, m, m, BookKeeper.DigestType.MAC, new byte[]{0x00});
            } catch (BKException bke) {
                if (bke.getCode() != BKException.Code.NotEnoughBookiesException) throw bke;
                else {
                    if (n > 2) n--;
                    TimeUnit.SECONDS.sleep(1);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } while (lh == null);
        return lh;
    }

    void commitLedger(LedgerHandle lh) {
        if(!leader) throw new NotLeaderException();
        try {
            lh.close();
            Stat stat = new Stat();
            for (Long id : quorumPeerConfig.getServers().keySet()) {
                final String p = LOG_LIST_PATH + "/" + id;
                byte[] ledgerList = curator.getData().storingStatIn(stat).forPath(p);
                curator.setData().withVersion(stat.getVersion()).forPath(p, Bytes.concat(ledgerList, longToByteArray(lh.getId()), longToByteArray(System.currentTimeMillis())));
            }
        } catch (Exception e) {
            try { bookkeeper.deleteLedger(lh.getId()); } catch (Exception i) {}
            throw new RuntimeException("commit problem. the ledger with id " + lh.getId() + " removal was attempted after the exception: " + e.getMessage(), e);
        }
    }

    void abortLedger(LedgerHandle lh) {
        if(!leader) throw new NotLeaderException();
        try { lh.close(); } catch (Exception i) {}
        try { bookkeeper.deleteLedger(lh.getId()); } catch (Exception i) {}
    }

    void dropDatabase(byte[] id) {
        try {
            curator.delete().forPath(DB_NAME_MAP_PATH + "/" + BaseEncoding.base16().encode(id));
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void startZookeeper(String path, Properties zk) throws IOException, QuorumPeerConfig.ConfigException {

        String prefix = FilenameUtils.normalize(path + "/zk/");
        File dataDir = new File(FilenameUtils.normalize(prefix+zk.getProperty("dataDir")));
        File dataLogDir = new File(FilenameUtils.normalize(prefix+zk.getProperty("dataLogDir",zk.getProperty("dataDir"))));

        FileUtils.forceMkdir(dataDir);
        if(!dataLogDir.equals(dataDir)) FileUtils.forceMkdir(dataLogDir);
        FileUtils.write(new File(dataDir.getCanonicalPath()+"/myid"), Long.toString(myid));

        zk.setProperty("dataDir", dataDir.getCanonicalPath());
        zk.setProperty("dataLogDir", dataLogDir.getCanonicalPath());

        cnxnFactory = ServerCnxnFactory.createFactory();
        quorumPeer = new QuorumPeer();

        quorumPeerConfig = new QuorumPeerConfig();
        quorumPeerConfig.parseProperties(zk);

        logger.info("cluster size = " + quorumPeerConfig.getServers().size());

        try {
            ManagedUtil.registerLog4jMBeans();
        } catch (JMException e) {
            logger.warn("unable to register log4j jmx control", e);
        }

        cnxnFactory.configure(quorumPeerConfig.getClientPortAddress(), quorumPeerConfig.getMaxClientCnxns());

        quorumPeer.setClientPortAddress(quorumPeerConfig.getClientPortAddress());
        quorumPeer.setTxnFactory(new FileTxnSnapLog(dataLogDir, dataDir));
        quorumPeer.setQuorumPeers(quorumPeerConfig.getServers());
        quorumPeer.setElectionType(quorumPeerConfig.getElectionAlg());
        quorumPeer.setMyid(quorumPeerConfig.getServerId());
        quorumPeer.setTickTime(quorumPeerConfig.getTickTime());
        quorumPeer.setMinSessionTimeout(quorumPeerConfig.getMinSessionTimeout());
        quorumPeer.setMaxSessionTimeout(quorumPeerConfig.getMaxSessionTimeout());
        quorumPeer.setInitLimit(quorumPeerConfig.getInitLimit());
        quorumPeer.setSyncLimit(quorumPeerConfig.getSyncLimit());
        quorumPeer.setQuorumVerifier(quorumPeerConfig.getQuorumVerifier());
        quorumPeer.setCnxnFactory(cnxnFactory);
        quorumPeer.setZKDatabase(new ZKDatabase(quorumPeer.getTxnFactory()));
        quorumPeer.setLearnerType(quorumPeerConfig.getPeerType());
        quorumPeer.setSyncEnabled(quorumPeerConfig.getSyncEnabled());
        quorumPeer.setQuorumListenOnAllIPs(quorumPeerConfig.getQuorumListenOnAllIPs());

        quorumPeer.start();
    }

    private void startBookKeeper(String path, Properties bk) throws IOException, ConfigurationException, InterruptedException, BookieException, ReplicationException.CompatibilityException, ReplicationException.UnavailableException, KeeperException {

        curator = CuratorFrameworkFactory.newClient(bk.getProperty("zkServers"), 2000, 10000, new ExponentialBackoffRetry(1000, 3));
        curator.start();
        curator.blockUntilConnected();

        try {
            Stat stat = new Stat();

            createZKPath("/ledgers");
            createZKPath("/available");
            createZKPath(ZKPATH);
            createZKPath(LOG_LIST_PATH);
            createZKPath(LOG_REMOVED_PATH);

            for (Long id : quorumPeerConfig.getServers().keySet())
                for(String p: new String[] { LOG_LIST_PATH +"/"+id, LOG_REMOVED_PATH+"/"+ id })
                    createZKPath(p);

        } catch(Exception e) {
            logger.warn("",e);
        }

        leaderSelector = new LeaderSelector(curator, ELECTION_PATH, this);
        leaderSelector.autoRequeue();
        leaderSelector.start();

        String prefix = FilenameUtils.normalize(path + "/bk/");
        String journalDirectory = FilenameUtils.normalize(prefix + bk.getProperty("journalDirectory"));
        String ledgerDirectories = FilenameUtils.normalize(prefix + bk.getProperty("ledgerDirectories"));

        FileUtils.forceMkdir(new File(journalDirectory));
        FileUtils.forceMkdir(new File(ledgerDirectories));

        bk.setProperty("journalDirectory", journalDirectory);
        bk.setProperty("ledgerDirectories", ledgerDirectories);

        StringWriter sw = new StringWriter();
        bk.store(sw,null);

        PropertiesConfiguration bkp = new PropertiesConfiguration();
        bkp.load(new StringReader(sw.toString()));

        bookieConfig = new ServerConfiguration();
        bookieConfig.append(bkp);
        try {
            bookie = new BookieServer(bookieConfig);
        } catch(KeeperException.NoNodeException e) {
            logger.warn("",e);
        }

        bookie.start();

        bookkeeper = new BookKeeper(new ClientConfiguration().setZkServers(bk.getProperty("zkServers")).setZkTimeout(Integer.parseInt(bk.getProperty("zkTimeout"))));
    }


    private void createZKPath(String path) throws Exception {
        try { curator.create().forPath(path); }
        catch(KeeperException.NodeExistsException kne) {}
        catch(KeeperException.NoNodeException kne) {}
        curator.setData().withVersion(new Stat().getVersion()).forPath(path, new byte[]{});
    }

//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////

    static {
        System.setProperty("log4j.defaultInitOverride", "true");
        LogManager.resetConfiguration();
        LogManager.getRootLogger().setLevel(Level.DEBUG);
        LogManager.getRootLogger().addAppender(new ConsoleAppender(new PatternLayout("%d{yyyy-MM-dd HH:mm:ss.SSS} %c %5p: %m%n"), ConsoleAppender.SYSTEM_ERR));
    }

    public void stupidTest() throws Exception {

        while(true) {
            TimeUnit.SECONDS.sleep(1);
            if(!leader) {
                System.out.println("following...");
                byte[] lastProcessedLedger = curator.getData().forPath(lastProcessedLedgerPath);
                byte[] ledgerList = curator.getData().forPath(LOG_LIST_PATH);

                int listStart = Bytes.indexOf(ledgerList, lastProcessedLedger);
                listStart = (listStart == -1 ? 0 : listStart) + 8;

                LedgerHandle lh = null;
                for(int i = listStart; i < ledgerList.length; i += 8) try {
                    lh = bookkeeper.openLedger(byteArrayToLong(ledgerList,i), BookKeeper.DigestType.MAC, new byte[]{0x00});
                    LedgerEntry le = lh.readEntries(0,lh.getLastAddConfirmed()).nextElement();
                    System.out.println(new String(le.getEntry()) + ": " + lh.getId());
                } catch(Exception e) {
                    System.out.println(e.getMessage());
                    continue;
                } finally {
                    if(lh != null) {
                        lh.close();
                        curator.setData().forPath(lastProcessedLedgerPath, longToByteArray(lh.getId()));
                    }
                }
            } else {
                Stat stat = new Stat();

                byte[] ledgerList = curator.getData().storingStatIn(stat).forPath(LOG_LIST_PATH);

                System.out.println("ledgerList=" + BaseEncoding.base16().encode(ledgerList));

                if (ledgerList.length > 0) {
                    byte[] nextarray = new byte[]{};
                    long first = Long.MAX_VALUE;
                    for (Long id : quorumPeerConfig.getServers().keySet()) {
                        nextarray = curator.getData().forPath(LOG_REMOVED_PATH + "/" + id);
                        System.out.println("nextarray.length=" + nextarray.length);
                        System.out.println("       nextarray=" + BaseEncoding.base16().encode(nextarray));
                        if(nextarray.length > 0) {
                            long next = byteArrayToLong(nextarray);
                            if (next < first) first = next;
                        }
                    }

                    System.out.println("first=" + first);

                    if(first < Long.MAX_VALUE) {
                        int listIndex = Bytes.indexOf(ledgerList,nextarray);
                        ledgerList = Arrays.copyOfRange(ledgerList, listIndex < 0 ? 0 : listIndex, ledgerList.length);
                    }
                }

                System.out.println("ledgerList=" + BaseEncoding.base16().encode(ledgerList));

                System.out.print("leading... ");
                LedgerHandle lh = null;
                do try {
                    // for cluster with 2N+1 cluster members where N >= 1 and M = N-1:
                    // try createLedger(N, M, M, ...) until N=3 then try with M=N=2
                    lh = bookkeeper.createLedger(2, 2, 2, BookKeeper.DigestType.MAC, new byte[]{0x00});
                } catch(BKException bke) {
                    if(bke.getCode() != BKException.Code.NotEnoughBookiesException) System.out.println(bke.getMessage());
                    else TimeUnit.MILLISECONDS.sleep(500);
                } while(lh == null);
                if(lh != null) try {
                    lh.addEntry(UUID.randomUUID().toString().getBytes());
                    //curator.setData().forPath("/rlmdb-log", lh.getId())
                    System.out.print(lh.getId());
                } catch(Exception e) {
                    continue;
                    //e.printStackTrace(System.out);
                } finally {
                    lh.close();
                    curator.setData().withVersion(stat.getVersion()).forPath(LOG_LIST_PATH, Bytes.concat(ledgerList, longToByteArray(lh.getId())));
                }
                System.out.println();
            }
        }

    }

    public static void main(String[] arg) throws Exception {

        long id = Long.parseLong(arg[0]);

        Env env = new Env(id);

        Properties zk = new Properties();
        zk.load(new FileInputStream(arg[1])); //"/tmp/zookeeper1.properties"));

        Properties bk = new Properties();
        bk.load(new FileInputStream(arg[2])); //"/tmp/bookieserver1.conf"));

        env.open("/tmp/db" + arg[0], Constants.FIXEDMAP, zk, bk);
        System.out.println("opened");
//        env.stupidTest();
        Database db = env.openDatabase("coldb");
        System.out.println("created db");
        if(env.leader()) db.put(bytes("oi"), bytes("hello"));
        System.out.println("wrote");
        env.close();
    }

}
