package org.apache.activemq.store.cassandra;

import org.apache.activemq.command.ActiveMQQueue;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import javax.jms.*;

/**
 *
 */
public class SpringBrokerTest {

    Logger log = LoggerFactory.getLogger(SpringBrokerTest.class);

    @Test
    public void loadBrokerAndAdapterFromSpringContext() throws JMSException {
        ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:broker.xml");
        ConnectionFactory cf = (ConnectionFactory) ctx.getBean("cf");
        ActiveMQQueue queue = new ActiveMQQueue("test123");
        Connection conn = cf.createConnection();
        conn.start();
        Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(queue);
        MessageConsumer consumer = session.createConsumer(queue);
        TextMessage textMessage = session.createTextMessage("TEST123");
        producer.send(textMessage);
        TextMessage rec = (TextMessage) consumer.receive(10000);
        Assert.assertNotNull(rec);
        Assert.assertEquals("TEST123", rec.getText());
    }


}
