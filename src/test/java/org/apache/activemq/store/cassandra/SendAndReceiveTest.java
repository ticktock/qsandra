package org.apache.activemq.store.cassandra;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public class SendAndReceiveTest extends EmbeddedServicesTest {

    Logger log = LoggerFactory.getLogger(SendAndReceiveTest.class);
    public static final String BROKER_URI = "tcp://localhost:61616";
    BrokerService broker;
    CassandraPersistenceAdapter adapter;
    ActiveMQConnectionFactory factory;
    ActiveMQQueue queue;
    Connection conn;
    Session session;
    int messages = 250;
    private boolean useCassandra = true;

    @Test
    public void testWithNoRestart() throws Exception {
        doSendAndReceive(false);
    }

    @Test
    public void testWithRestart() throws Exception {
        doSendAndReceive(true);
    }


    public void doSendAndReceive(boolean restart) throws Exception {

        Set<Message> sentMsgs = new HashSet<Message>();
        MessageProducer producer = session.createProducer(queue);
        int sent = 0;
        for (; sent <= messages; sent++) {
            String text = getMessageBody(sent);
            log.debug("Sending: {}", text);
            Message m = session.createTextMessage(text);
            producer.send(m);
            sentMsgs.add(m);
        }
        producer.close();

        if (restart) {
            stopBrokerService();
            createBrokerService();
        }

        MessageConsumer consumer = session.createConsumer(queue);

        Message m = null;
        int received = 0;
        int nulls = 0;
        Set<Message> recMsg = new HashSet<Message>();

        while (recMsg.size() < sentMsgs.size() && nulls < 3) {
            m = consumer.receive(10000);
            if (m != null) {
                String text = ((TextMessage) m).getText();
                log.debug("Received:{}", text);
                Assert.assertEquals(getMessageBody(received), text);
                log.debug("recievedCount:{}", ++received);
                recMsg.add(m);
            } else {
                log.error("received null");
                nulls++;
            }
        }
        Assert.assertEquals(null, consumer.receive(2000));
        consumer.close();
        Assert.assertEquals(sent, received);
    }


    private String getMessageBody(int i) {
        return "TEST:" + i;
    }

    @After()
    public void stopBrokerService() throws Exception {
        session.close();
        conn.close();
        broker.stop();
    }

    @Before
    public void createBrokerService() throws Exception {
        queue = new ActiveMQQueue("test.queue.1");
        broker = new BrokerService();
        broker.addConnector(BROKER_URI);
        broker.setPersistent(true);
        broker.setDataDirectory("target");
        if (useCassandra) {
            adapter = new CassandraPersistenceAdapter();
            CassandraClient cassandraClient = new CassandraClient();
            cassandraClient.setCassandraHost("localhost");
            cassandraClient.setCassandraPort(getCassandraPort());
            ZooKeeperMasterElector elector = new ZooKeeperMasterElector();
            elector.setZookeeperConnectString(getZookeeperConnectString());
            adapter.setCassandraClient(cassandraClient);
            adapter.setMasterElector(elector);
            broker.setPersistenceAdapter(adapter);
        }
        broker.start();
        factory = new ActiveMQConnectionFactory(BROKER_URI);
        conn = factory.createConnection();
        conn.start();
        session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);


    }

}
