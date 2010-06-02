package org.apache.activemq.store.cassandra;

import org.apache.activemq.broker.BrokerService;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ZooKeeperMasterElectorTest extends EmbeddedServicesTest {

    Logger log = LoggerFactory.getLogger(ZooKeeperMasterElectorTest.class);

    @Test(timeout = 30000)
    public void startMasterElector() throws InterruptedException {
        ZooKeeperMasterElector elector = new ZooKeeperMasterElector();
        elector.setZookeeperConnectString(getZookeeperConnectString());
        elector.start();
        elector.waitTillMaster();
        elector.stop();
    }


    @Test
    public void simulateLostConnectionInBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistent(true);
        broker.setDataDirectory("target");
        CassandraPersistenceAdapter adapter = new CassandraPersistenceAdapter();
        CassandraClient cassandraClient = new CassandraClient();
        cassandraClient.setCassandraHost("localhost");
        cassandraClient.setCassandraPort(getCassandraPort());
        ZooKeeperMasterElector elector = new ZooKeeperMasterElector();
        elector.setZookeeperConnectString(getZookeeperConnectString());
        adapter.setCassandraClient(cassandraClient);
        adapter.setMasterElector(elector);
        broker.setPersistenceAdapter(adapter);
        broker.start();
        elector.waitTillMaster();
        elector.process(new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.Disconnected, null));
        Thread.sleep(5000);
        Assert.assertFalse(broker.isStarted());
        elector.stop();
    }


}
