package org.apache.activemq.store.cassandra;

import org.apache.activemq.command.*;
import org.apache.cassandra.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.activemq.store.cassandra.CassandraIdentifier.*;
import static org.apache.activemq.store.cassandra.CassandraUtils.*;
import static org.apache.cassandra.thrift.ConsistencyLevel.*;

/**
 *
 */
public class CassandraClient {


    public static final ColumnPath DESTINATION_QUEUE_SIZE_COLUMN_PATH = new ColumnPath(DESTINATIONS_FAMILY.string());
    public static final ColumnPath BROKER_DESTINATION_COUNT_COLUMN_PATH = new ColumnPath(BROKER_FAMILY.string());

    static {
        DESTINATION_QUEUE_SIZE_COLUMN_PATH.setColumn(DESTINATION_QUEUE_SIZE_COLUMN.bytes());
        BROKER_DESTINATION_COUNT_COLUMN_PATH.setColumn(BROKER_DESTINATION_COUNT.bytes());
    }

    /*Subscriptions Column Family Constants*/
    public static final String SUBSCRIPTIONS_CLIENT_SUBSCRIPTION_DELIMITER = "~~~~~";
    public static final String SUBSCRIPTIONS_DEFAULT_SUBSCRIPTION_NAME = "@NOT_SET@";

    public static final byte[] ZERO = getBytes(0L);


    private static ThreadLocal<Cassandra.Client> cassandraClients = new ThreadLocal<Cassandra.Client>();
    private static ThreadLocal<TTransport> cassandraTransports = new ThreadLocal<TTransport>();
    private static Logger log = LoggerFactory.getLogger(CassandraClient.class);

    private String cassandraHost;
    private int cassandraPort;

    private ConsistencyLevel consistencyLevel = QUORUM;


    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        if (!EnumSet.of(QUORUM, DCQUORUM, DCQUORUMSYNC).contains(consistencyLevel)) {
            throw new IllegalArgumentException("Only QUORUM or DCQUORUM or DCQUORUMSYNC are supported consistency levels.");
        }
        this.consistencyLevel = consistencyLevel;
    }

    public String getCassandraHost() {
        return cassandraHost;
    }

    public void setCassandraHost(String cassandraHost) {
        this.cassandraHost = cassandraHost;
    }

    public int getCassandraPort() {
        return cassandraPort;
    }

    public void setCassandraPort(int cassandraPort) {
        this.cassandraPort = cassandraPort;
    }

    Cassandra.Client getCassandraConnection() {
        Cassandra.Client client = cassandraClients.get();
        TTransport tr = cassandraTransports.get();
        if (client == null || !tr.isOpen()) {
            tr = new TSocket(cassandraHost, cassandraPort);
            TProtocol proto = new TBinaryProtocol(tr);
            client = new Cassandra.Client(proto);
            try {
                tr.open();
            } catch (TTransportException e) {
                log.error("Unable to open transport", e);
                throw new RuntimeException(e);
            }
            cassandraTransports.set(tr);
            cassandraClients.set(client);
        }
        return client;
    }

    void discacrdCassandraConnection() {
        cassandraClients.remove();
        cassandraTransports.remove();
    }

    /*Broker CF Methods*/


    public int getDestinationCount() {
        try {
            ColumnOrSuperColumn cosc = getCassandraConnection().get(KEYSPACE.string(), BROKER_KEY.string(), BROKER_DESTINATION_COUNT_COLUMN_PATH, consistencyLevel);
            return getInt(cosc.getColumn().getValue());
        } catch (NotFoundException e) {
            log.warn("Broker Destination Count not found, inserting 0");
        } catch (Exception e) {
            log.error("Exception in getDestinationCount", e);
            throw new RuntimeException(e);
        }

        try {
            insertDestinationCount(0);
            return 0;
        } catch (Exception e) {
            log.error("Exception in getDestinationCount while inserting 0", e);
            throw new RuntimeException(e);
        }

    }

    public void insertDestinationCount(int count) throws TException, TimedOutException, InvalidRequestException, UnavailableException {
        getCassandraConnection().insert(KEYSPACE.string(), BROKER_KEY.string(), BROKER_DESTINATION_COUNT_COLUMN_PATH, getBytes(count), timestamp(), consistencyLevel);
    }


    /*Destination CF Methods*/

    public boolean createDestination(String name, boolean isTopic, AtomicInteger destinationCount) {
        ColumnPath columnPath = new ColumnPath(DESTINATIONS_FAMILY.string());
        columnPath.setColumn(DESTINATION_IS_TOPIC_COLUMN.bytes());
        try {
            getCassandraConnection().get(KEYSPACE.string(), name, columnPath, consistencyLevel);
            log.info("Destination {} Exists", name);
            return false;
        } catch (NotFoundException e) {
            log.warn("Destination {} not found, Creating, topic:{} ", name, isTopic);
            try {
                Map<String, Map<String, List<Mutation>>> mutations = map();
                Map<String, List<Mutation>> mutation = map();
                List<Mutation> destinationMutations = list();
                destinationMutations.add(getInsertOrUpdateColumnMutation(DESTINATION_IS_TOPIC_COLUMN.string(), isTopic));
                destinationMutations.add(getInsertOrUpdateColumnMutation(DESTINATION_MAX_STORE_SEQUENCE_COLUMN.string(), 0L));
                mutation.put(DESTINATIONS_FAMILY.string(), destinationMutations);
                mutations.put(name, mutation);
                destinationMutations.add(getInsertOrUpdateColumnMutation(DESTINATION_QUEUE_SIZE_COLUMN.bytes(), ZERO, timestamp()));
                Map<String, List<Mutation>> brokerMutation = map();
                List<Mutation> brokerMutations = list();
                brokerMutations.add(getInsertOrUpdateColumnMutation(BROKER_DESTINATION_COUNT.bytes(), getBytes(destinationCount.incrementAndGet()), timestamp()));
                brokerMutation.put(BROKER_FAMILY.string(), brokerMutations);
                mutations.put(BROKER_KEY.string(), brokerMutation);

                getCassandraConnection().batch_mutate(KEYSPACE.string(), mutations, consistencyLevel);


                return true;
            } catch (Exception e2) {
                destinationCount.decrementAndGet();
                throw new RuntimeException(e2);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public Set<ActiveMQDestination> getDestinations() {
        SlicePredicate predicate = new SlicePredicate();
        predicate.setSlice_range(new SliceRange(new byte[0], new byte[0], false, Integer.MAX_VALUE));
        ColumnParent parent = new ColumnParent(DESTINATIONS_FAMILY.string());
        KeyRange keyRange = new KeyRange();
        keyRange.setStart_key(getString(new byte[0]));
        keyRange.setEnd_key(getString(new byte[0]));
        try {
            List<KeySlice> slices = getCassandraConnection().get_range_slices(KEYSPACE.string(), parent, predicate, keyRange, consistencyLevel);
            List<String> keys = new ArrayList<String>();
            for (KeySlice keySlice : slices) {
                keys.add(keySlice.getKey());
            }
            SlicePredicate topicPredicate = new SlicePredicate();
            topicPredicate.setColumn_names(Collections.singletonList(getBytes("isTopic")));
            Map<String, List<ColumnOrSuperColumn>> result = getCassandraConnection().multiget_slice(KEYSPACE.string(), keys, parent, predicate, consistencyLevel);

            Set<ActiveMQDestination> destinations = set();
            for (Map.Entry<String, List<ColumnOrSuperColumn>> stringListEntry : result.entrySet()) {
                if (stringListEntry.getValue().size() == 1) {
                    boolean isTopic = getBoolean(stringListEntry.getValue().get(0).getColumn().getValue());
                    if (isTopic) {
                        destinations.add(ActiveMQDestination.createDestination(stringListEntry.getKey(), ActiveMQDestination.TOPIC_TYPE));
                    } else {
                        destinations.add(ActiveMQDestination.createDestination(stringListEntry.getKey(), ActiveMQDestination.QUEUE_TYPE));
                    }
                }
            }
            return destinations;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public void deleteQueue(ActiveMQDestination destination, AtomicInteger destinationCount) {
        try {
            String key = getDestinationKey(destination);
            getCassandraConnection().remove(KEYSPACE.string(), key, new ColumnPath(MESSAGES_FAMILY.string()), timestamp(), consistencyLevel);
            getCassandraConnection().remove(KEYSPACE.string(), key, new ColumnPath(DESTINATIONS_FAMILY.string()), timestamp(), consistencyLevel);
            getCassandraConnection().remove(KEYSPACE.string(), key, new ColumnPath(MESSAGE_TO_STORE_ID_FAMILY.string()), timestamp(), consistencyLevel);
            getCassandraConnection().remove(KEYSPACE.string(), key, new ColumnPath(STORE_IDS_IN_USE_FAMILY.string()), timestamp(), consistencyLevel);
            getCassandraConnection().remove(KEYSPACE.string(), key, new ColumnPath(SUBSCRIPTIONS_FAMILY.string()), timestamp(), consistencyLevel);
            Map<String, Map<String, List<Mutation>>> mutations = map();
            Map<String, List<Mutation>> mutation = map();
            List<Mutation> brokerMutations = list();
            mutations.put(BROKER_KEY.string(), mutation);
            mutation.put(BROKER_FAMILY.string(), brokerMutations);
            brokerMutations.add(getInsertOrUpdateColumnMutation(BROKER_DESTINATION_COUNT.bytes(), getBytes(destinationCount.decrementAndGet()), timestamp()));
            getCassandraConnection().batch_mutate(KEYSPACE.string(), mutations, consistencyLevel);
        } catch (Exception e) {
            destinationCount.incrementAndGet();
            log.error("Exception in deleteQueue", e);
            throw new RuntimeException(e);
        }
    }

    public void deleteTopic(ActiveMQTopic destination, AtomicInteger destinationCount) {
        deleteQueue(destination, destinationCount);
    }


    public DestinationMaxIds getMaxStoreId() {

        int destinations = getDestinations().size();
        if (destinations == 0) {
            return new DestinationMaxIds(null, 0, 0);
        }
        ColumnParent columnParent = new ColumnParent(DESTINATIONS_FAMILY.string());
        SlicePredicate slicePredicate = new SlicePredicate();
        slicePredicate.setColumn_names(Collections.singletonList(DESTINATION_MAX_STORE_SEQUENCE_COLUMN.bytes()));
        KeyRange keyRange = new KeyRange();
        keyRange.setStart_key("");
        keyRange.setEnd_key("");
        keyRange.setCount(destinations);
        try {
            List<KeySlice> cols = getCassandraConnection().get_range_slices(KEYSPACE.string(), columnParent, slicePredicate, keyRange, consistencyLevel);
            DestinationMaxIds max = new DestinationMaxIds(null, 0, 0);
            long storeVal = 0;
            long broker = 0;
            for (KeySlice col : cols) {
                String key = col.getKey();
                for (ColumnOrSuperColumn columnOrSuperColumn : col.getColumns()) {
                    if (Arrays.equals(columnOrSuperColumn.getColumn().getName(), DESTINATION_MAX_STORE_SEQUENCE_COLUMN.bytes())) {
                        storeVal = getLong(columnOrSuperColumn.getColumn().getValue());
                    } else if (Arrays.equals(columnOrSuperColumn.getColumn().getName(), DESTINATION_MAX_BROKER_SEQUENCE_COLUMN.bytes())) {
                        broker = getLong(columnOrSuperColumn.getColumn().getValue());
                    }
                }
                if (storeVal > max.getMaxStoreId()) {
                    max = new DestinationMaxIds(ActiveMQDestination.createDestination(key, ActiveMQDestination.QUEUE_TYPE), storeVal, broker);
                }
            }


            return max;
        } catch (Exception e) {
            log.error("Error getting Max Store ID", e);
            throw new RuntimeException(e);
        }

    }


    private byte[] getMessageIndexKey(MessageId id) {
        return getBytes(getMessageIndexKeyString(id));
    }

    private String getMessageIndexKeyString(MessageId id) {
        return new StringBuilder(id.getProducerId().toString()).append("->").append(id.getProducerSequenceId()).toString();
    }

    public long getStoreId(ActiveMQDestination destination, MessageId identity) {
        ColumnPath path = new ColumnPath(MESSAGE_TO_STORE_ID_FAMILY.string());
        path.setColumn(getMessageIndexKey(identity));
        String key = getDestinationKey(destination);
        try {
            ColumnOrSuperColumn cosc = getCassandraConnection().get(KEYSPACE.string(), key, path, consistencyLevel);
            return getLong(cosc.getColumn().getValue());
        } catch (Exception e) {
            log.error("Exception in getStoreId", e);
            throw new RuntimeException(e);
        }
    }

    /*Messages CF Methods*/

    public byte[] getMessage(ActiveMQDestination destination, long storeId) {
        ColumnPath path = new ColumnPath(MESSAGES_FAMILY.string());
        path.setColumn(getBytes(storeId));
        try {
            ColumnOrSuperColumn cosc = getCassandraConnection().get(KEYSPACE.string(), getDestinationKey(destination), path, consistencyLevel);
            byte[] messageBytes = cosc.getColumn().getValue();
            if (messageBytes.length == 0) {
                throw new NotFoundException();
            }
            return messageBytes;
        } catch (NotFoundException e) {
            log.error("Message Not Found");
            throw new RuntimeException(e);
        } catch (Exception e) {
            log.error("Exception getting message", e);
            throw new RuntimeException(e);
        }
    }


    public void saveMessage(ActiveMQDestination destination, long id, MessageId messageId, byte[] messageBytes, AtomicLong queueSize) {
        Map<String, Map<String, List<Mutation>>> mutations = map();
        Map<String, List<Mutation>> saveMutation = map();

        String cassandraKey = getDestinationKey(destination);
        mutations.put(cassandraKey, saveMutation);

        List<Mutation> messageMutations = list();
        List<Mutation> destinationMutations = list();
        List<Mutation> indexMutations = list();
        List<Mutation> storeIdsMutations = list();
        saveMutation.put(MESSAGES_FAMILY.string(), messageMutations);
        saveMutation.put(DESTINATIONS_FAMILY.string(), destinationMutations);
        saveMutation.put(MESSAGE_TO_STORE_ID_FAMILY.string(), indexMutations);
        saveMutation.put(STORE_IDS_IN_USE_FAMILY.string(), storeIdsMutations);
        log.debug("Saving message with id:{}", id);
        log.debug("Saving message with brokerSeq id:{}", messageId.getBrokerSequenceId());
        long current = timestamp();
        messageMutations.add(getInsertOrUpdateColumnMutation(getBytes(id), messageBytes, current));
        destinationMutations.add(getInsertOrUpdateColumnMutation(DESTINATION_QUEUE_SIZE_COLUMN.bytes(), getBytes(queueSize.incrementAndGet()), current));


        destinationMutations.add(getInsertOrUpdateColumnMutation(DESTINATION_MAX_STORE_SEQUENCE_COLUMN.bytes(), getBytes(id), current));
        destinationMutations.add(getInsertOrUpdateColumnMutation(DESTINATION_MAX_BROKER_SEQUENCE_COLUMN.bytes(), getBytes(messageId.getBrokerSequenceId()), current));

        indexMutations.add(getInsertOrUpdateColumnMutation(getMessageIndexKey(messageId), getBytes(id), current));
        storeIdsMutations.add(getInsertOrUpdateColumnMutation(getBytes(id), new byte[1], current));
        try {
            getCassandraConnection().batch_mutate(KEYSPACE.string(), mutations, consistencyLevel);
        } catch (Exception e) {
            queueSize.decrementAndGet();
            log.error("Exception savingMessage", e);
            throw new RuntimeException(e);
        }
    }

    public void deleteMessage(ActiveMQDestination destination, MessageId id, AtomicLong queueSize) {
        long column = getStoreId(destination, id);
        long current = timestamp();
        Mutation delete = getDeleteColumnMutation(getBytes(column), current);
        Map<String, Map<String, List<Mutation>>> mutations = map();
        Map<String, List<Mutation>> saveMutation = map();

        String cassandraKey = getDestinationKey(destination);
        mutations.put(cassandraKey, saveMutation);

        List<Mutation> messageMutations = list();

        List<Mutation> indexMutations = list();
        List<Mutation> destinationMutations = list();
        List<Mutation> storeIdsMutations = list();
        saveMutation.put(MESSAGES_FAMILY.string(), messageMutations);
        saveMutation.put(STORE_IDS_IN_USE_FAMILY.string(), storeIdsMutations);
        saveMutation.put(MESSAGE_TO_STORE_ID_FAMILY.string(), indexMutations);
        saveMutation.put(DESTINATIONS_FAMILY.string(), destinationMutations);
        messageMutations.add(delete);
        destinationMutations.add(getInsertOrUpdateColumnMutation(DESTINATION_QUEUE_SIZE_COLUMN.bytes(), getBytes(queueSize.decrementAndGet()), current));
        indexMutations.add(getDeleteColumnMutation(getMessageIndexKey(id), current));
        storeIdsMutations.add(getDeleteColumnMutation(getBytes(column), current));
        try {
            getCassandraConnection().batch_mutate(KEYSPACE.string(), mutations, consistencyLevel);
            log.debug("Deleted Message {}", column);
        } catch (Exception e) {
            queueSize.incrementAndGet();
            log.error("Unable to delete message", e);
        }
    }

    public void deleteAllMessages(ActiveMQDestination destination, AtomicLong queueSize) {
        ColumnPath path = new ColumnPath(MESSAGES_FAMILY.string());
        try {
            getCassandraConnection().remove(KEYSPACE.string(), getDestinationKey(destination), path, timestamp(), consistencyLevel);
            queueSize.set(0);
        } catch (Exception e) {
            log.error("Unable to delete all messages", e);
        }
    }

    public int getMessageCount(ActiveMQDestination destination) {
        try {
            ColumnOrSuperColumn col = getCassandraConnection().get(KEYSPACE.string(), getDestinationKey(destination), DESTINATION_QUEUE_SIZE_COLUMN_PATH, consistencyLevel);
            byte[] countBytes = col.getColumn().getValue();
            long count = getLong(countBytes);
            if (count > Integer.MAX_VALUE) {
                throw new IllegalStateException("Count Higher than Max int, something wrong");
            } else {
                return Long.valueOf(count).intValue();
            }
        } catch (Exception e) {
            log.error("Error during getMessageCount for :" + getDestinationKey(destination), e);
            discacrdCassandraConnection();
            throw new RuntimeException(e);
        }
    }

    public List<byte[]> recoverMessages(ActiveMQDestination destination, AtomicLong batchPoint, int maxReturned) {
        if (log.isDebugEnabled()) {
            log.debug("recoverMessages({},{},{})", new Object[]{getDestinationKey(destination), batchPoint.get(), maxReturned});
        }
        if (maxReturned < 1) {
            throw new IllegalArgumentException("cant get less than one result");
        }
        String key = getDestinationKey(destination);
        byte[] start = batchPoint.get() == -1 ? new byte[0] : getBytes(batchPoint.get());
        byte[] end = new byte[0];
        List<byte[]> messages = list();
        recoverMessagesFromTo(key, start, end, maxReturned, messages, maxReturned);
        return messages;
    }


    private void recoverMessagesFromTo(String key, byte[] start, byte[] end, int limit, List<byte[]> messages, int messagelimit) {

        if (log.isDebugEnabled()) {
            log.debug("recoverMessagesFromTo({},{},{},{},{})", new Object[]{key, safeGetLong(start), safeGetLong(end), limit, messages.size()});
        }
        SlicePredicate predicate = new SlicePredicate();
        SliceRange range = new SliceRange(start, end, false, limit);
        predicate.setSlice_range(range);

        List<ColumnOrSuperColumn> cols;

        try {
            cols = getCassandraConnection().get_slice(KEYSPACE.string(), key, new ColumnParent(MESSAGES_FAMILY.string()), predicate, consistencyLevel);
        } catch (InvalidRequestException e) {
            log.error("recoverMessagesFromTo({},{},{},{},{})", new Object[]{key, safeGetLong(start), safeGetLong(end), limit, messages.size()});
            log.error("InvalidRequestException", e);
            discacrdCassandraConnection();
            throw new RuntimeException(e);
        } catch (UnavailableException e) {
            log.error("recoverMessagesFromTo({},{},{},{},{})", new Object[]{key, safeGetLong(start), safeGetLong(end), limit, messages.size()});
            log.error("UnavailableException", e);
            discacrdCassandraConnection();
            throw new RuntimeException(e);
        } catch (TimedOutException e) {
            log.error("recoverMessagesFromTo({},{},{},{},{})", new Object[]{key, safeGetLong(start), safeGetLong(end), limit, messages.size()});
            log.error("TimedOutException", e);
            discacrdCassandraConnection();
            throw new RuntimeException(e);
        } catch (TException e) {
            log.error("recoverMessagesFromTo({},{},{},{},{})", new Object[]{key, safeGetLong(start), safeGetLong(end), limit, messages.size()});
            log.error("TException", e);
            discacrdCassandraConnection();
            throw new RuntimeException(e);
        }

        for (ColumnOrSuperColumn col : cols) {
            Column c = col.getColumn();

            messages.add(c.getValue());
            if (log.isDebugEnabled()) {
                log.debug("recovered message with id: {}", safeGetLong(c.getName()));
            }
            if (messages.size() >= messagelimit) {
                break;
            }
        }


    }

    /*Subscription CF Messages*/

    public void addSubscription(ActiveMQDestination destination, SubscriptionInfo subscriptionInfo, long ack) {
        Map<String, Map<String, List<Mutation>>> mutations = map();
        Map<String, List<Mutation>> saveMutation = map();
        List<Mutation> mutationList = list();
        Mutation insert = new Mutation();
        mutationList.add(insert);
        mutations.put(getDestinationKey(destination), saveMutation);
        saveMutation.put(SUBSCRIPTIONS_FAMILY.string(), mutationList);

        ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
        insert.setColumn_or_supercolumn(columnOrSuperColumn);
        List<Column> cols = list();
        SuperColumn superColumn = new SuperColumn(getBytes(getSubscriptionSuperColumnName(subscriptionInfo)), cols);
        columnOrSuperColumn.setSuper_column(superColumn);

        byte[] selector = nullSafeGetBytes(subscriptionInfo.getSelector());
        byte[] subscribedDestination = getBytes(getDestinationKey(subscriptionInfo.getSubscribedDestination()));
        byte[] lastAck = getBytes(ack);
        long current = timestamp();
        if (subscriptionInfo.getSelector() != null) {
            cols.add(getColumn(SUBSCRIPTIONS_SELECTOR_SUBCOLUMN.bytes(), selector, current));
        }
        cols.add(getColumn(SUBSCRIPTIONS_SUB_DESTINATION_SUBCOLUMN.bytes(), subscribedDestination, current));
        cols.add(getColumn(SUBSCRIPTIONS_LAST_ACK_SUBCOLUMN.bytes(), lastAck, current));
        try {
            getCassandraConnection().batch_mutate(KEYSPACE.string(), mutations, consistencyLevel);
            log.debug("created subscription on {} for client {} subscription {} with selector {} and lastAck {}",
                    new Object[]{
                            getDestinationKey(destination),
                            subscriptionInfo.getClientId(),
                            subscriptionInfo.getSubscriptionName(),
                            subscriptionInfo.getSelector(),
                            ack
                    });
        } catch (Exception e) {
            log.error("Exception addingSubscription:", e);
            discacrdCassandraConnection();
            throw new RuntimeException(e);
        }

    }

    public SubscriptionInfo lookupSubscription(ActiveMQDestination destination, String clientId, String subscriptionName) {
        ColumnPath path = new ColumnPath(SUBSCRIPTIONS_FAMILY.string());
        path.setSuper_column(getBytes(getSubscriptionSuperColumnName(clientId, subscriptionName)));
        try {
            ColumnOrSuperColumn columnOrSuperColumn = getCassandraConnection().get(KEYSPACE.string(), getDestinationKey(destination), path, consistencyLevel);
            log.debug("retrieved supercolumn of {} for client {} subscriptionName {}", new Object[]{getDestinationKey(destination), clientId, subscriptionName});
            SuperColumn superColumn = columnOrSuperColumn.getSuper_column();
            SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
            subscriptionInfo.setClientId(clientId);
            subscriptionInfo.setSubscriptionName(subscriptionName);
            subscriptionInfo.setDestination(destination);
            byte type = destination.isTopic() ? ActiveMQDestination.TOPIC_TYPE : ActiveMQDestination.QUEUE_TYPE;
            for (Column column : superColumn.getColumns()) {
                if (Arrays.equals(SUBSCRIPTIONS_LAST_ACK_SUBCOLUMN.bytes(), column.getName())) {
                    //skip
                } else if (Arrays.equals(SUBSCRIPTIONS_SELECTOR_SUBCOLUMN.bytes(), column.getName())) {
                    String selector = getString(column.getValue());
                    subscriptionInfo.setSelector(selector);
                } else if (Arrays.equals(SUBSCRIPTIONS_SUB_DESTINATION_SUBCOLUMN.bytes(), column.getName())) {
                    String name = getString(column.getValue());
                    subscriptionInfo.setSubscribedDestination(ActiveMQDestination.createDestination(name, type));
                } else {
                    log.error("Recieved unexpected column from Subscription Super Column {}", getString(column.getName()));
                }
            }

            return subscriptionInfo;
        } catch (NotFoundException e) {
            log.warn("lookupSubsctription({},{}) found no subscription, returning null", clientId, subscriptionName);
            return null;
        } catch (Exception e) {
            log.error("Exception in lookupSubscription", e);
            discacrdCassandraConnection();
            throw new RuntimeException(e);
        }

    }

    public SubscriptionInfo[] lookupAllSubscriptions(ActiveMQDestination destination) {
        ColumnParent path = new ColumnParent(SUBSCRIPTIONS_FAMILY.string());
        SlicePredicate slicePredicate = new SlicePredicate();
        SliceRange sliceRange = new SliceRange(new byte[0], new byte[0], false, Integer.MAX_VALUE);
        slicePredicate.setSlice_range(sliceRange);

        try {
            List<ColumnOrSuperColumn> coscs = getCassandraConnection().get_slice(KEYSPACE.string(), getDestinationKey(destination), path, slicePredicate, consistencyLevel);
            List<SubscriptionInfo> info = new ArrayList<SubscriptionInfo>(coscs.size());
            for (ColumnOrSuperColumn columnOrSuperColumn : coscs) {
                SuperColumn superColumn = columnOrSuperColumn.getSuper_column();
                SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
                subscriptionInfo.setClientId(getClientIdFromSubscriptionSuperColumnName(superColumn));
                subscriptionInfo.setSubscriptionName(getSubscriptionNameFromSubscriptionSuperColumnName(superColumn));
                subscriptionInfo.setDestination(destination);
                byte type = destination.isTopic() ? ActiveMQDestination.TOPIC_TYPE : ActiveMQDestination.QUEUE_TYPE;
                for (Column column : superColumn.getColumns()) {
                    if (Arrays.equals(SUBSCRIPTIONS_LAST_ACK_SUBCOLUMN.bytes(), column.getName())) {
                        //skip
                    } else if (Arrays.equals(SUBSCRIPTIONS_SELECTOR_SUBCOLUMN.bytes(), column.getName())) {
                        String selector = getString(column.getValue());
                        subscriptionInfo.setSelector(selector);
                    } else if (Arrays.equals(SUBSCRIPTIONS_SUB_DESTINATION_SUBCOLUMN.bytes(), column.getName())) {
                        String name = getString(column.getValue());
                        subscriptionInfo.setSubscribedDestination(ActiveMQDestination.createDestination(name, type));
                    } else {
                        log.error("Recieved unexpected column from Subscription Super Column {}", getString(column.getName()));
                    }
                }
                info.add(subscriptionInfo);
            }


            return info.toArray(new SubscriptionInfo[info.size()]);
        } catch (Exception e) {
            log.error("Exception in lookupSubscription", e);
            discacrdCassandraConnection();
            throw new RuntimeException(e);
        }

    }

    public void acknowledge(ActiveMQDestination destination, String clientId, String subscriptionName, MessageId messageId) {


        Map<String, Map<String, List<Mutation>>> mutations = map();
        Map<String, List<Mutation>> saveMutation = map();
        List<Mutation> mutationList = list();
        Mutation insert = new Mutation();
        mutationList.add(insert);
        mutations.put(getDestinationKey(destination), saveMutation);
        saveMutation.put(SUBSCRIPTIONS_FAMILY.string(), mutationList);

        ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
        insert.setColumn_or_supercolumn(columnOrSuperColumn);
        List<Column> cols = list();
        SuperColumn superColumn = new SuperColumn(getBytes(getSubscriptionSuperColumnName(clientId, subscriptionName)), cols);
        columnOrSuperColumn.setSuper_column(superColumn);

        long lastAckStoreId = getStoreId(destination, messageId);
        byte[] lastAck = getBytes(lastAckStoreId);
        long timestamp = timestamp();
        cols.add(getColumn(SUBSCRIPTIONS_LAST_ACK_SUBCOLUMN.bytes(), lastAck, timestamp));

        try {
            getCassandraConnection().batch_mutate(KEYSPACE.string(), mutations, consistencyLevel);
            log.debug("Acked {} for client {} sub {}", new Object[]{messageId.getBrokerSequenceId(), clientId, subscriptionName});
        } catch (Exception e) {
            log.error("Exception acking:", e);
            discacrdCassandraConnection();
            throw new RuntimeException(e);
        }

    }

    public void deleteSubscription(ActiveMQDestination destination, String clientId, String subscriptionName) {
        ColumnPath path = new ColumnPath(SUBSCRIPTIONS_FAMILY.string());
        path.setSuper_column(getBytes(getSubscriptionSuperColumnName(clientId, subscriptionName)));
        try {
            getCassandraConnection().remove(KEYSPACE.string(), getDestinationKey(destination), path, timestamp(), consistencyLevel);
            log.debug("deletedSubscription on {} for client {} subscriptionName {}", new Object[]{getDestinationKey(destination), clientId, subscriptionName});
        } catch (Exception e) {
            log.error("Exception in deleteSubscription", e);
            discacrdCassandraConnection();
            throw new RuntimeException(e);
        }
    }


    public int getMessageCountFrom(ActiveMQDestination destination, long storeId) {
        SlicePredicate predicate = new SlicePredicate();
        SliceRange range = new SliceRange(getBytes(storeId), new byte[0], false, Integer.MAX_VALUE);
        predicate.setSlice_range(range);
        try {
            List<ColumnOrSuperColumn> coscs = getCassandraConnection().get_slice(KEYSPACE.string(), getDestinationKey(destination), new ColumnParent(STORE_IDS_IN_USE_FAMILY.string()), predicate, consistencyLevel);
            return coscs.size();
        } catch (Exception e) {
            log.error("Exception in getMessageCountFrom {}:{}", getDestinationKey(destination), storeId);
            log.error("Ex:", e);
            throw new RuntimeException(e);
        }
    }

    public int getLastAckStoreId(ActiveMQDestination destination, String clientid, String subsriptionName) {
        ColumnPath path = new ColumnPath(SUBSCRIPTIONS_FAMILY.string());
        path.setSuper_column(getBytes(getSubscriptionSuperColumnName(clientid, subsriptionName)));
        path.setColumn(SUBSCRIPTIONS_LAST_ACK_SUBCOLUMN.bytes());
        try {
            ColumnOrSuperColumn cosc = getCassandraConnection().get(KEYSPACE.string(), getDestinationKey(destination), path, consistencyLevel);
            long result = getLong(cosc.getColumn().getValue());
            return Long.valueOf(result).intValue();
        } catch (NotFoundException e) {
            log.debug("LastAckStoreId not found, returning 0");
            return 0;
        } catch (Exception e) {
            log.error("Exception in getLastAckStoreId, {} {} {}", new Object[]{getDestinationKey(destination), clientid, subsriptionName});
            log.error("Ex:", e);
            throw new RuntimeException(e);
        }
    }


    private static String getSubscriptionSuperColumnName(SubscriptionInfo info) {
        return info.getClientId() + SUBSCRIPTIONS_CLIENT_SUBSCRIPTION_DELIMITER + nullSafeGetSubscriptionName(info);
    }

    private static String nullSafeGetSubscriptionName(SubscriptionInfo info) {
        return info.getSubscriptionName() != null ? info.getSubscriptionName() : SUBSCRIPTIONS_DEFAULT_SUBSCRIPTION_NAME;
    }

    private static String getSubscriptionSuperColumnName(String clientId, String subscriptionName) {
        return clientId + SUBSCRIPTIONS_CLIENT_SUBSCRIPTION_DELIMITER + (subscriptionName != null ? subscriptionName : SUBSCRIPTIONS_DEFAULT_SUBSCRIPTION_NAME);
    }

    public static String getSubscriberId(String clientId, String subscriptionName) {
        return getSubscriptionSuperColumnName(clientId, subscriptionName);
    }

    private static String getClientIdFromSubscriptionSuperColumnName(SuperColumn superColumn) {
        String key = getString(superColumn.getName());
        String[] split = key.split(SUBSCRIPTIONS_CLIENT_SUBSCRIPTION_DELIMITER);
        return split[0];
    }

    private static String getSubscriptionNameFromSubscriptionSuperColumnName(SuperColumn superColumn) {
        String key = getString(superColumn.getName());
        String[] split = key.split(SUBSCRIPTIONS_CLIENT_SUBSCRIPTION_DELIMITER);
        if (split[1].equals(SUBSCRIPTIONS_DEFAULT_SUBSCRIPTION_NAME)) {
            return null;
        } else {
            return split[1];
        }
    }

    /*util*/

    private static <K, V> Map<K, V> map() {
        return new HashMap<K, V>();
    }

    private static <I> List<I> list() {
        return new ArrayList<I>();
    }

    private static <I> Set<I> set() {
        return new HashSet<I>();
    }


}
