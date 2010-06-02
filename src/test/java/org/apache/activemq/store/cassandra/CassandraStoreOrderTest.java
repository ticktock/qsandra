package org.apache.activemq.store.cassandra;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.*;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.StoreOrderTest;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;


public class CassandraStoreOrderTest extends StoreOrderTest {
    private CassandraPersistenceAdapter cassandraPersistenceAdapter;
    Logger log = LoggerFactory.getLogger(CassandraStoreOrderTest.class);
    static EmbeddedServicesTest servicesHolder = new EmbeddedServicesTest();

    @BeforeClass
    public static void setupClass() throws Exception {
        servicesHolder.setup();
    }

    @AfterClass
    public static void stop() {
        servicesHolder.tearDown();
    }

    @Override
    protected void dumpMessages() throws Exception {
        cassandraPersistenceAdapter.createQueueMessageStore((ActiveMQQueue) destination).recover(new MessageRecoveryListener() {
            @Override
            public boolean recoverMessage(Message message) throws Exception {
                log.info("Dumped:{} {}", message.getMessageId().getBrokerSequenceId(), message.getContent());
                return true;
            }

            @Override
            public boolean recoverMessageReference(MessageId ref) throws Exception {
                return false;
            }

            @Override
            public boolean hasSpace() {
                return true;
            }

            @Override
            public boolean isDuplicate(MessageId ref) {
                return false;
            }
        });
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService svc = super.createBroker();
        svc.setDataDirectory("target");
        svc.setTmpDataDirectory(new File("target"));
        return svc;
    }

    @Override
    protected void setPersistentAdapter(BrokerService brokerService) throws Exception {
        cassandraPersistenceAdapter = new CassandraPersistenceAdapter();
        CassandraClient cassandraClient = new CassandraClient();
        cassandraClient.setCassandraHost("localhost");
        cassandraClient.setCassandraPort(EmbeddedServicesTest.getCassandraPort());
        cassandraClient.setConsistencyLevel(ConsistencyLevel.QUORUM);
        ZooKeeperMasterElector masterElector = new ZooKeeperMasterElector();
        masterElector.setZookeeperConnectString(EmbeddedServicesTest.getZookeeperConnectString());
        cassandraPersistenceAdapter.setCassandraClient(cassandraClient);
        cassandraPersistenceAdapter.setMasterElector(masterElector);
        brokerService.setPersistenceAdapter(cassandraPersistenceAdapter);

    }
}
