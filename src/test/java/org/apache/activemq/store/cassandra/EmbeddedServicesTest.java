package org.apache.activemq.store.cassandra;


import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 *
 */
public class EmbeddedServicesTest {

    static Logger log = LoggerFactory.getLogger(EmbeddedServicesTest.class);
    private static EmbeddedCassandraService cassandra;
    public static final String STORAGE_CONFIG = "storage-config";
    public static final String ZK_DATA = "zookeeper-data";
    private static EmbeddedZookeeperService zks;
    private static Thread zkThread;
    private static String zkData;
    private static String thriftPort;
    private static String zkPort;


    @BeforeClass
    public static void setup() throws IOException, TTransportException, InterruptedException {
        System.setProperty("org.apache.activemq.default.directory.prefix", "target" + File.separator);
        Properties testProps = new Properties();
        testProps.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("test.properties"));
        String storageConfig = testProps.getProperty(STORAGE_CONFIG);

        try {
            System.setProperty(STORAGE_CONFIG, storageConfig);
            CassandraServiceDataCleaner cleaner = new CassandraServiceDataCleaner();
            cleaner.prepare();
            cassandra = new EmbeddedCassandraService();
            cassandra.init();
            Thread t = new Thread(cassandra);
            t.setDaemon(true);
            t.start();
        } catch (Throwable e) {
            log.error("Exception in setup", e);
            throw new RuntimeException(e);
        }

        zkData = testProps.getProperty(ZK_DATA);
        zks = new EmbeddedZookeeperService();
        thriftPort = testProps.getProperty("thrift-port");
        zkPort = testProps.getProperty("zookeeper-port");
        startZookeeper();
    }


    public static String getZookeeperPort() {
        return zkPort;
    }

    public static String getZookeeperConnectString() {
        return "localhost:" + zkPort;
    }

    public static int getCassandraPort() {
        return Integer.parseInt(thriftPort);
    }

    public static void stopZookeeper() {
        zkThread.interrupt();
    }

    public static void startZookeeper() throws IOException, InterruptedException {
        zks.init(getZookeeperPort(), zkData);
        zkThread = new Thread(zks);
        zkThread.setDaemon(true);
        zkThread.start();
        for (int i = 0; i < 10; i++) {
            ZooKeeper zooKeeper = new ZooKeeper(getZookeeperConnectString(), 10000, new Watcher() {
                @Override
                public void process(WatchedEvent event) {

                }
            });
            Thread.sleep(1000);
            if (zooKeeper.getState().equals(ZooKeeper.States.CONNECTING)) {
                Thread.sleep(2000);
            }
            if (zooKeeper.getState().equals(ZooKeeper.States.CONNECTED)) {
                return;
            }

        }
        throw new RuntimeException("Zookeeper wasnt available after 10 tries");
    }

    @AfterClass
    public static void tearDown() {
        cassandra.stop();
        stopZookeeper();
    }


    public void logColumnName(ColumnOrSuperColumn cos, Class nameType) {
        log.debug("column:{}", getName(cos, nameType));
    }

    private Object getName(ColumnOrSuperColumn cos, Class nameType) {
        return get(cos.getColumn().getName(), nameType);
    }

    private Object getValue(ColumnOrSuperColumn cos, Class valueType) {
        return get(cos.getColumn().getValue(), valueType);
    }

    private Object get(byte[] bytes, Class type) {
        if (type.equals(String.class)) {
            return CassandraUtils.getString(bytes);
        } else if (type.equals(Long.class)) {
            return CassandraUtils.getLong(bytes);
        } else {
            return "UnsupportedType for conversion";
        }
    }

    public void logColumnNameAndValue(ColumnOrSuperColumn cos, Class nameType, Class valueType) {
        log.debug("column:{} value:{}", getName(cos, nameType), getValue(cos, valueType));
    }


}
