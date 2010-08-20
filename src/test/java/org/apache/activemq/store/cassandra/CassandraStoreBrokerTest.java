package org.apache.activemq.store.cassandra;

import junit.extensions.TestSetup;
import junit.framework.Test;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerTest;
import org.apache.cassandra.thrift.ConsistencyLevel;

public class CassandraStoreBrokerTest extends BrokerTest {


    static EmbeddedServicesTest servicesHolder = new EmbeddedServicesTest();


    public CassandraStoreBrokerTest() {
        super();    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService brokerService = new BrokerService();
        brokerService.setUseJmx(false);
        CassandraPersistenceAdapter adapter = new CassandraPersistenceAdapter();
        CassandraClient cassandraClient = new CassandraClient();
        cassandraClient.setCassandraHost("localhost");
        cassandraClient.setCassandraPort(EmbeddedServicesTest.getCassandraPort());
       
        adapter.setCassandraClient(cassandraClient);
        brokerService.setPersistenceAdapter(adapter);
        adapter.setBrokerService(brokerService);
        persistenceAdapter = adapter;
        adapter.start();
        adapter.deleteAllMessages();
        return brokerService;
    }

    public static Test suite() {
        return new TestSetup(suite(CassandraStoreBrokerTest.class)) {
            @Override
            protected void setUp() throws Exception {
                servicesHolder.setup();
            }

            @Override
            protected void tearDown() throws Exception {
                servicesHolder.tearDown();
            }
        };
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

}

