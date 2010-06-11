package org.apache.activemq.store.cassandra;

import org.apache.activemq.command.*;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;


/**
 *
 */
public class CassandraClientTest extends EmbeddedServicesTest {


    private static CassandraClient client;
    private static BloomFilter dups = BloomFilter.getFilter(1000, 0.01d);

    @BeforeClass
    public static void createAdapter() throws TTransportException {
        log.info("------------>CassandraClientTest.createAdapter");
        client = new CassandraClient();
        client.setCassandraHost("localhost");
        client.setCassandraPort(getCassandraPort());
        client.setConsistencyLevel(ConsistencyLevel.QUORUM);
        client.start();

    }


    public void removeDestinations(AtomicInteger destinationCount) throws TTransportException {
        assertFalse(client.createDestination("queue://test.queue.1", false, destinationCount));
        client.deleteQueue(new ActiveMQQueue("queue://test.queue.1"), destinationCount);
        client.deleteTopic(new ActiveMQTopic("topic://test,topic.1"), destinationCount);
    }


    private void createDestinations(AtomicInteger destinationCount) throws TTransportException {
        client.createDestination("queue://test.queue.1", false, destinationCount);
        client.createDestination("topic://test.topic.1", true, destinationCount);
        assertEquals(2, client.getDestinations().size());
        assertEquals(2, client.getDestinationCount());
    }


    @Test
    public void messageCrud() throws TTransportException, UnsupportedEncodingException, InterruptedException {
        createDestinations(new AtomicInteger(0));
        ActiveMQQueue queue = new ActiveMQQueue("test.queue.1");
        byte[] fakeMessage = "Happy".getBytes("UTF-8");
        ProducerId producerId = new ProducerId("PRODUCERKEY");
        MessageId messageId = new MessageId(producerId, 123123123L);
        messageId.setBrokerSequenceId(321321321L);
        AtomicLong count = new AtomicLong(0);
        client.saveMessage(queue, 1, messageId, fakeMessage, count, dups);
        assertEquals(1, client.getMessageCount(queue));
        log.info("Saved");
        byte[] retrieved = client.getMessage(queue, 1);
        assertEquals(new String(fakeMessage), new String(retrieved));
        client.deleteMessage(queue, messageId, count);
        try {
            byte[] rte2 = client.getMessage(queue, 1);
            assertFalse(new String(fakeMessage).equals(new String(rte2)));
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof NotFoundException);
        }
        assertEquals(0, client.getMessageCount(queue));
    }


    @Test
    public void subscriberCrud() throws TTransportException, UnsupportedEncodingException, InterruptedException {

        createDestinations(new AtomicInteger(0));
        ActiveMQTopic topic = new ActiveMQTopic("test.topic.1");
        SubscriptionInfo info = new SubscriptionInfo();
        info.setDestination(topic);
        info.setSelector("test = '123'");
        String clientName = "testClientName";
        info.setClientId(clientName);
        client.addSubscription(topic, info, 1);
        SubscriptionInfo info2 = client.lookupSubscription(topic, clientName, null);
        assertEquals(topic.getQualifiedName(), info2.getDestination().getQualifiedName());
        MessageId id = new MessageId("PRODUCERID", 123123);
        id.setBrokerSequenceId(7);
        client.saveMessage(topic, 1, id, new byte[1], new AtomicLong(0), dups);
        client.acknowledge(topic, clientName, null, id);
        assertEquals(1, client.lookupAllSubscriptions(topic).length);
        assertNotNull(client.lookupSubscription(topic, clientName, null));

        client.deleteSubscription(topic, clientName, null);

        assertEquals(0, client.lookupAllSubscriptions(topic).length);
        assertNull(client.lookupSubscription(topic, clientName, null));


    }


    @Test
    public void testQuery() throws TException, UnsupportedEncodingException, TimedOutException, InvalidRequestException, UnavailableException {

        createDestinations(new AtomicInteger(0));
        ActiveMQQueue queue = new ActiveMQQueue("queue://test.queue.1");
        byte[] fakeMessage = "Happy".getBytes("UTF-8");
        ProducerId producerId = new ProducerId("PRODUCERKEY");
        AtomicLong count = new AtomicLong(0);
        long prodseq = 1;
        long brokerseq = 1;
        for (int i = 1; i <= 1000; i++, prodseq++, brokerseq++) {
            MessageId messageId = new MessageId(producerId, prodseq);
            messageId.setBrokerSequenceId(brokerseq);
            client.saveMessage(queue, i, messageId, fakeMessage, count, dups);
            log.debug("saved:{}", i);
        }
        ColumnParent p = new ColumnParent(CassandraClientUtil.MESSAGES_FAMILY());
        byte[] start = new byte[0];
        byte[] end = new byte[0];
        byte[] lastCol = new byte[0];
        int limit = 100;
        SlicePredicate predicate = new SlicePredicate();
        SliceRange range = new SliceRange(start, end, false, limit);
        predicate.setSlice_range(range);


        /*  List<ColumnOrSuperColumn> orSuperColumns = client.getCassandraConnection().get_slice(CassandraIdentifier.KEYSPACE.string(), CassandraUtils.getDestinationKey(queue), p, predicate, ConsistencyLevel.QUORUM);
        for (ColumnOrSuperColumn orSuperColumn : orSuperColumns) {
            logColumnName(orSuperColumn, Long.class);
        }
        ColumnOrSuperColumn next = orSuperColumns.get(orSuperColumns.size() - 1);
        predicate.getSlice_range().setStart(next.getColumn().getName());
        orSuperColumns = client.getCassandraConnection().get_slice(CassandraIdentifier.KEYSPACE.string(), CassandraUtils.getDestinationKey(queue), p, predicate, ConsistencyLevel.QUORUM);
        for (ColumnOrSuperColumn orSuperColumn : orSuperColumns) {
            logColumnName(orSuperColumn, Long.class);
        }*/
    }


}
