package org.apache.activemq.store.cassandra;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.HashSet;
import java.util.Set;


public class TopicTest extends EmbeddedServicesTest {

    Logger log = LoggerFactory.getLogger(TopicTest.class);
    public static final String BROKER_URI = "tcp://localhost:61616";
    BrokerService broker;
    CassandraPersistenceAdapter adapter;
    ActiveMQConnectionFactory factory;
    ActiveMQTopic topic;
    Connection conn;
    Connection connSelect;
    Session session;
    Session select;
    TopicSubscriber allSub;
    TopicSubscriber selectSub;
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
        MessageProducer producer = session.createProducer(topic);

        int sent = 1;
        while (sent <= messages) {
            String text = getMessageBody(sent);
            log.debug("Sending: {}", text);
            Message m = session.createTextMessage(text);
            if (sent % 2 == 0) {
                m.setBooleanProperty("selectme", true);
            }
            producer.send(m);
            sentMsgs.add(m);
            sent++;
        }
        producer.close();

        if (restart) {
            stopBrokerService();
            createBrokerService();
        }

        Message m = null;
        int received = 0;
        int nulls = 0;
        Set<Message> recMsg = new HashSet<Message>();

        while (recMsg.size() < sentMsgs.size() && nulls < 3) {
            m = allSub.receive(10000);
            if (m != null) {
                received++;
                String text = ((TextMessage) m).getText();
                log.debug("Received:{}", text);
                log.debug("recievedCount:{}", received);
                Assert.assertEquals(getMessageBody(received), text);
                recMsg.add(m);
            } else {
                log.error("received null");
                nulls++;
            }
        }
        Assert.assertEquals(null, allSub.receive(2000));
        allSub.close();
        Assert.assertEquals(sentMsgs.size(), recMsg.size());


        log.info("Subscription with No Selector Works");

        received = 0;
        nulls = 0;
        recMsg = new HashSet<Message>();

        while (recMsg.size() < (sentMsgs.size() / 2) && nulls < 3) {
            m = selectSub.receive(10000);
            if (m != null) {
                received++;
                String text = ((TextMessage) m).getText();
                log.debug("Received:{}", text);
                log.debug("recievedCount:{}", received);
                recMsg.add(m);
            } else {
                log.error("received null");
                nulls++;
            }
        }
        Assert.assertEquals(null, selectSub.receive(2000));
        selectSub.close();
        Assert.assertEquals((sentMsgs.size() / 2), recMsg.size());
        log.info("Subscription with Selector Works");

        select.unsubscribe("selectSub");
        session.unsubscribe("allSubscription");


        log.info("Unsubscribe works");

        broker.removeDestination(topic);
    }


    private String getMessageBody(int i) {
        return "TEST:" + i;
    }

    @After()
    public void stopBrokerService() throws Exception {
        session.close();
        select.close();
        conn.close();
        connSelect.close();
        broker.stop();
    }

    @Before
    public void createBrokerService() throws Exception {
        topic = new ActiveMQTopic("test.topic.1");
        broker = new BrokerService();
        broker.addConnector(BROKER_URI);
        broker.setPersistent(true);
        broker.setDataDirectory("target");
        if (useCassandra) {
            adapter = new CassandraPersistenceAdapter();
            CassandraClient cassandraClient = new CassandraClient();
            cassandraClient.setCassandraHost("localhost");
            cassandraClient.setCassandraPort(getCassandraPort());
            cassandraClient.setConsistencyLevel(ConsistencyLevel.QUORUM);
            ZooKeeperMasterElector elector = new ZooKeeperMasterElector();
            elector.setZookeeperConnectString(getZookeeperConnectString());
            adapter.setCassandraClient(cassandraClient);
            adapter.setMasterElector(elector);
            broker.setPersistenceAdapter(adapter);
        }
        broker.start();
        factory = new ActiveMQConnectionFactory(BROKER_URI);
        conn = factory.createConnection();
        conn.setClientID("TopicTest");
        conn.start();
        session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        allSub = session.createDurableSubscriber(topic, "allSubscription", null, false);

        connSelect = factory.createConnection();
        connSelect.setClientID("TopicTestSelector");
        connSelect.start();
        select = connSelect.createSession(false, Session.AUTO_ACKNOWLEDGE);
        selectSub = select.createDurableSubscriber(topic, "selectSub", "selectme = TRUE", false);

    }
}