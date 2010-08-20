package org.apache.activemq.store.cassandra;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.*;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TransactionStore;
import org.apache.activemq.store.memory.MemoryTransactionStore;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class CassandraPersistenceAdapter implements PersistenceAdapter, BrokerServiceAware {

    private Logger log = LoggerFactory.getLogger(CassandraPersistenceAdapter.class);

    private MemoryTransactionStore transactionStore;
    private AtomicLong sequenceGenerator = new AtomicLong(0);
    private AtomicInteger destinationCount = new AtomicInteger(0);
    private WireFormat wireFormat = new OpenWireFormat();
    private CassandraClient cassandra;
    private ConcurrentMap<ActiveMQQueue, CassandraMessageStore> queues = new ConcurrentHashMap<ActiveMQQueue, CassandraMessageStore>();
    private ConcurrentMap<ActiveMQTopic, CassandraTopicMessageStore> topics = new ConcurrentHashMap<ActiveMQTopic, CassandraTopicMessageStore>();
    private BrokerService brokerService;
    private MasterElector masterElector;

    public MasterElector getMasterElector() {
        return masterElector;
    }

    public void setMasterElector(MasterElector masterElector) {
        this.masterElector = masterElector;
    }

    public void setCassandraClient(CassandraClient cassandraClient) {
        this.cassandra = cassandraClient;
    }

    public CassandraClient getCassandra() {
        return cassandra;
    }

    public WireFormat getWireFormat() {
        return wireFormat;
    }

    public Set<ActiveMQDestination> getDestinations() {
        return cassandra.getDestinations();
    }

    public MessageStore createQueueMessageStore(ActiveMQQueue destination) throws IOException {
        CassandraMessageStore store = queues.get(destination);
        if (store == null) {
            cassandra.createDestination(CassandraClientUtil.getDestinationKey(destination), false, destinationCount);
            store = new CassandraMessageStore(this, destination);
            try {
                store.start();
            } catch (Exception e) {
                log.error("Error Starting queue:" + CassandraClientUtil.getDestinationKey(destination), e);
                throw new IOException(e);
            }
            queues.putIfAbsent(destination, store);
            store = queues.get(destination);
        }
        return transactionStore.proxy(store);
    }

    public TopicMessageStore createTopicMessageStore(ActiveMQTopic destination) throws IOException {
        CassandraTopicMessageStore store = topics.get(destination);
        if (store == null) {
            cassandra.createDestination(CassandraClientUtil.getDestinationKey(destination), true, destinationCount);
            store = new CassandraTopicMessageStore(this, destination);
            try {
                store.start();
            } catch (Exception e) {
                log.error("Error Starting queue:" + CassandraClientUtil.getDestinationKey(destination), e);
                throw new IOException(e);
            }

            topics.putIfAbsent(destination, store);
            store = topics.get(destination);
        }
        return transactionStore.proxy(store);
    }

    public void removeQueueMessageStore(ActiveMQQueue destination) {
        log.warn("removeQueueMessageStore for {}", destination.getQualifiedName());
        cassandra.deleteQueue(destination, destinationCount);
    }

    public void removeTopicMessageStore(ActiveMQTopic destination) {
        log.warn("removeTopicMessageStore for {}", destination.getQualifiedName());
        cassandra.deleteTopic(destination, destinationCount);
    }

    public TransactionStore createTransactionStore() throws IOException {
        if (transactionStore == null) {
            transactionStore = new MemoryTransactionStore(this);
        }
        return this.transactionStore;
    }

    public void beginTransaction(ConnectionContext context) throws IOException {
        log.debug("beginTransaction");
    }

    public void commitTransaction(ConnectionContext context) throws IOException {
        log.debug("commitTransaction");
    }

    public void rollbackTransaction(ConnectionContext context) throws IOException {
        log.debug("rollbackTransaction");
    }

    public long getLastMessageBrokerSequenceId() throws IOException {
        DestinationMaxIds max = cassandra.getMaxStoreId();
        sequenceGenerator.set(max.getMaxStoreId());
        long brokerSeq = max.getMaxBrokerSeq();
        log.debug("getLastSequence: store {}, broker {}", sequenceGenerator.get(), brokerSeq);
        return brokerSeq;
    }

    
    public long getLastProducerSequenceId(ProducerId id) throws IOException {
        return cassandra.getMaxProducerSequenceId(id);
    }

    public AtomicLong getStoreSequenceGenerator() {
        return sequenceGenerator;
    }

    public void deleteAllMessages() throws IOException {
        Set<ActiveMQDestination> destinations = getDestinations();
        for (ActiveMQDestination destination : destinations) {
            if (destination instanceof ActiveMQTopic) {
                removeTopicMessageStore((ActiveMQTopic) destination);
            } else {
                removeQueueMessageStore((ActiveMQQueue) destination);
            }
        }
    }

    public void setUsageManager(SystemUsage usageManager) {

    }

    public void setBrokerName(String brokerName) {

    }

    public void setDirectory(File dir) {

    }

    public void checkpoint(boolean sync) throws IOException {

    }

    public long size() {
        return 0;
    }

    public void start() throws Exception {
        //Zookeeper master election
        cassandra.start();
        if (masterElector != null) {
            masterElector.setMasterLostHandler(new Runnable() {
                @Override
                public void run() {
                    try {
                        log.warn("Lost Master Status, stopping broker");
                        brokerService.stop();
                        brokerService.waitUntilStopped();
                        /*
                         It would be good if we could just restart the brokerService and
                         have it restart the master elector and wait till it is master again
                         but the BrokerService seems to put an ErrorBroker in such that
                         once you stop the broker service, you cant force restart it.

                         org.apache.activemq.broker.BrokerStoppedException:
                         Broker has been stopped:
                         org.apache.activemq.broker.BrokerService$3@1d840cd
	                     at org.apache.activemq.broker.ErrorBroker.nowMasterBroker(ErrorBroker.java:297)
	                     at org.apache.activemq.broker.MutableBrokerFilter.nowMasterBroker(MutableBrokerFilter.java:307)
                         see jiras AMQ-2245 and 2503
                         so the following line of code is commented out
                         */
                        //brokerService.start(true);
                        /*
                         Assume that this is running in a java service wrapper that would restart the whole process.
                        */
                    } catch (Exception e) {
                        log.error("Exception Stopping Broker when Master Status Lost!", e);
                    }
                }
            });

            masterElector.start();
            log.debug("Master Elector started, waiting to be master");
            masterElector.waitTillMaster();
            isMaster();
        } else {
            isMaster();
        }

    }

    private void isMaster() throws Exception {
        log.info("This Broker is now Master");
        int count = cassandra.getDestinationCount();
        destinationCount = new AtomicInteger(count);
        brokerService.getBroker().nowMasterBroker();
    }


    public void stop() throws Exception {

        if (masterElector != null) {
            masterElector.stop();
        }
        cassandra.stop();
    }


    byte[] marshall(Message message) throws IOException {
        ByteSequence byteSequence = getWireFormat().marshal(message);
        return byteSequence.getData();
    }

    Message unmarshall(byte[] message) throws IOException {
        ByteSequence byteSequence = new ByteSequence(message);
        return (Message) getWireFormat().unmarshal(byteSequence);
    }

    @Override
    public void setBrokerService(BrokerService brokerService) {
        this.brokerService = brokerService;
    }
}
