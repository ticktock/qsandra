package org.apache.activemq.store.cassandra;

import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.PersistenceAdapterTestSupport;
import org.apache.cassandra.thrift.ConsistencyLevel;


public class CassandraPersistenceAdapterTest extends PersistenceAdapterTestSupport {

    static EmbeddedServicesTest servicesHolder = new EmbeddedServicesTest();

    @Override
    protected void setUp() throws Exception {
        servicesHolder.setup();
        super.setUp();    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();    //To change body of overridden methods use File | Settings | File Templates.
        servicesHolder.tearDown();
    }

    @Override
    protected PersistenceAdapter createPersistenceAdapter(boolean delete) throws Exception {
        CassandraPersistenceAdapter adapter = new CassandraPersistenceAdapter();
        CassandraClient cassandraClient = new CassandraClient();
        cassandraClient.setCassandraHost("localhost");
        cassandraClient.setCassandraPort(EmbeddedServicesTest.getCassandraPort());
        cassandraClient.start();
        ZooKeeperMasterElector elector = new ZooKeeperMasterElector();
        elector.setZookeeperConnectString(EmbeddedServicesTest.getZookeeperConnectString());
        adapter.setCassandraClient(cassandraClient);
        adapter.setMasterElector(elector);
        if (delete) {
            adapter.deleteAllMessages();
        }
        brokerService.setPersistenceAdapter(adapter);
        adapter.setBrokerService(brokerService);
        return adapter;
    }
}
