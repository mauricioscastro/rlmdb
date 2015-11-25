package rlmdb;

import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
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
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.jmx.ManagedUtil;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.fusesource.lmdbjni.Constants;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import javax.management.JMException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

public class Env extends org.fusesource.lmdbjni.Env implements LeaderSelectorListener {

    private static final Logger logger = Logger.getLogger(Env.class);

    private static final String ELECTION_PATH = "/rlmdb";

    private static QuorumPeer quorumPeer;
    private static QuorumPeerConfig quorumPeerConfig;
    private static ServerCnxnFactory cnxnFactory;

    private static ServerConfiguration bookieConfig;
    private static BookieServer bookie;
    private static CuratorFramework curator;

    private static LeaderSelector leaderSelector;

    private static BookKeeper bookkeeper;

    private static DB ldb;

    private volatile boolean leader = false;

    public void open(String path, long myid, Properties zk, Properties bk) throws InterruptedException, BookieException, KeeperException, IOException, ReplicationException.CompatibilityException, QuorumPeerConfig.ConfigException, ConfigurationException, ReplicationException.UnavailableException {
        open(path, 0, 0644, myid, zk, bk);
    }

    public void open(String path, int flags, long myid, Properties zk, Properties bk) throws InterruptedException, BookieException, KeeperException, IOException, ReplicationException.CompatibilityException, QuorumPeerConfig.ConfigException, ConfigurationException, ReplicationException.UnavailableException {
        open(path, flags, 0644, myid, zk, bk);
    }

    public void open(String path, int flags, int mode, long myid, Properties zk, Properties bk) throws IOException, QuorumPeerConfig.ConfigException, InterruptedException, BookieException, KeeperException, ReplicationException.CompatibilityException, ReplicationException.UnavailableException, ConfigurationException {
        super.open(path, flags, mode);
        openLevelDB(path);
        startZookeeper(path, zk, myid);
        startBookKeeper(path, bk);
    }

    @Override
    public void close() {
        if(bookkeeper != null) try { bookkeeper.close(); } catch (Exception e) { logger.warn("",e); }
        if(curator != null) curator.close();
        if(bookie != null) bookie.shutdown();
        if(quorumPeer != null) quorumPeer.shutdown();
        if(ldb != null) try { ldb.close(); } catch (Exception e) { logger.warn("",e); }
        super.close();
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

    private void startZookeeper(String path, Properties zk, long myid) throws IOException, QuorumPeerConfig.ConfigException {

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
            curator.create().forPath("/ledgers");
            curator.create().forPath("/ledgers/available");
        } catch(Exception e) {
            if(!(e instanceof KeeperException.NodeExistsException)) logger.warn("",e);
        }

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
            System.err.println("\n\n" + e.getMessage().toUpperCase() + "\n\n");
        }

        bookie.start();

        leaderSelector = new LeaderSelector(curator, ELECTION_PATH, this);
        leaderSelector.autoRequeue();
        leaderSelector.start();

        bookkeeper = new BookKeeper(new ClientConfiguration().setZkServers(bk.getProperty("zkServers")).setZkTimeout(Integer.parseInt(bk.getProperty("zkTimeout"))));
    }

    private static void openLevelDB(String path) throws IOException {
        Options options = new Options();
        options.createIfMissing(true);
        ldb = factory.open(new File(path+"/ldb"), options);

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

    public static void main(String[] arg) throws Exception {

        Env env = new Env();

        Properties zk = new Properties();
        zk.load(new FileInputStream(arg[1])); //"/tmp/zookeeper1.properties"));

        Properties bk = new Properties();
        bk.load(new FileInputStream(arg[2])); //"/tmp/bookieserver1.conf"));

        env.open("/tmp/db" + arg[0], Constants.FIXEDMAP, Long.parseLong(arg[0]), zk, bk);

        boolean running = true;

        if(curator.checkExists().forPath("/rlmdb-log") == null) curator.create().forPath("/rlmdb-log");

        while(running) {
            TimeUnit.SECONDS.sleep(1);
            if(!env.leader()) {
                for(long i = 1; i < 1500; i++) try {
                    LedgerHandle lh = bookkeeper.openLedger(i, BookKeeper.DigestType.MAC, new byte[]{0x00});
                    LedgerEntry le = lh.readEntries(0,lh.getLastAddConfirmed()).nextElement();
                    System.out.println(new String(le.getEntry()) + ": " + lh.getId());
                    lh.close();
                } catch(Exception e) { continue; }
//               if(arg[0].equals("1")) for(long i = 1; i < 10; i++) try {
//                    bookkeeper.deleteLedger(i);
//                } catch(Exception e) { continue; }
                System.out.println("following...");
            } else {
                System.out.print("leading... ");
                LedgerHandle lh = null;
                do try {
                     lh = bookkeeper.createLedger(2, 2, 2, BookKeeper.DigestType.MAC, new byte[]{0x00});
                } catch(BKException bke) {
                    if(bke.getCode() != BKException.Code.NotEnoughBookiesException) break;
                    else TimeUnit.MILLISECONDS.sleep(500);
                } while(lh == null);
                if(lh != null) try {
                    lh.addEntry(UUID.randomUUID().toString().getBytes());
                    //curator.setData().forPath("/rlmdb-log", lh.getId())
                    System.out.print(lh.getId());
                } catch(Exception e) {
                    e.printStackTrace(System.out);
                } finally {
                    lh.close();
                }
                System.out.println();
            }
        }

        env.close();
    }
}
