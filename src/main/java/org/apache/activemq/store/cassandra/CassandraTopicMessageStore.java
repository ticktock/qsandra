package org.apache.activemq.store.cassandra;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.*;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.TopicMessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class CassandraTopicMessageStore extends CassandraMessageStore implements TopicMessageStore {

    private Logger log = LoggerFactory.getLogger(CassandraTopicMessageStore.class);
    private Map<String, AtomicLong> subscriberLastMessageMap = new ConcurrentHashMap<String, AtomicLong>();

    public CassandraTopicMessageStore(CassandraPersistenceAdapter cassandra, ActiveMQTopic destination) {
        super(cassandra, destination);
    }


    @Override
    public void acknowledge(ConnectionContext context, String clientId, String subscriptionName, MessageId messageId) throws IOException {
        getAdapter().getCassandra().acknowledge(getDestination(), clientId, subscriptionName, messageId);
    }

    @Override
    public void deleteSubscription(String clientId, String subscriptionName) throws IOException {
        getAdapter().getCassandra().deleteSubscription(getDestination(), clientId, subscriptionName);
    }

    @Override
    public void recoverSubscription(String clientId, String subscriptionName, MessageRecoveryListener listener) throws Exception {
        long lastAcked = getAdapter().getCassandra().getLastAckStoreId(getDestination(), clientId, subscriptionName);
        AtomicLong last = new AtomicLong(lastAcked + 1);
        List<byte[]> messages = getAdapter().getCassandra().recoverMessages(getDestination(), last, Integer.MAX_VALUE);
        for (byte[] message : messages) {
            if (!listener.recoverMessage(getAdapter().unmarshall(message))) {
                break;
            }
        }
    }

    @Override
    public void recoverNextMessages(String clientId, String subscriptionName, int maxReturned, MessageRecoveryListener listener) throws Exception {
        String subcriberId = getAdapter().getCassandra().getSubscriberId(clientId, subscriptionName);
        AtomicLong last = subscriberLastMessageMap.get(subcriberId);
        if (last == null) {
            long lastAcked = getAdapter().getCassandra().getLastAckStoreId(getDestination(), clientId, subscriptionName);
            last = new AtomicLong(lastAcked + 1);
            subscriberLastMessageMap.put(subcriberId, last);
        }
        List<byte[]> messages = getAdapter().getCassandra().recoverMessages(getDestination(), last, maxReturned);
        Message lastRecovered = null;
        for (int i = 0; i < messages.size(); i++) {
            Message real = getAdapter().unmarshall(messages.get(i));
            if (listener.hasSpace()) {
                listener.recoverMessage(real);
                lastRecovered = real;
                if (log.isDebugEnabled()) {
                    log.debug("recovered message with BrokerSequence:{} for {}", real.getMessageId().getBrokerSequenceId(), subcriberId);
                }

            } else {
                log.debug("stopped recovery");
                break;
            }
        }
        if (lastRecovered != null) {
            long lastStore = getAdapter().getCassandra().getStoreId(getDestination(), lastRecovered.getMessageId());
            last.set(lastStore + 1);
        }
    }

    @Override
    public void resetBatching(String clientId, String subscriptionName) {
        String id = getAdapter().getCassandra().getSubscriberId(clientId, subscriptionName);
        subscriberLastMessageMap.remove(id);
    }

    @Override
    public int getMessageCount(String clientId, String subscriberName) throws IOException {
        long storeId = getAdapter().getCassandra().getLastAckStoreId(getDestination(), clientId, subscriberName);
        return getAdapter().getCassandra().getMessageCountFrom(getDestination(), storeId);
    }

    @Override
    public SubscriptionInfo lookupSubscription(String clientId, String subscriptionName) throws IOException {
        return getAdapter().getCassandra().lookupSubscription(getDestination(), clientId, subscriptionName);
    }

    @Override
    public SubscriptionInfo[] getAllSubscriptions() throws IOException {
        return getAdapter().getCassandra().lookupAllSubscriptions(getDestination());
    }

    @Override
    public void addSubsciption(SubscriptionInfo subscriptionInfo, boolean retroactive) throws IOException {
        long lastAck = -1;
        if (!retroactive) {
            lastAck = getAdapter().getLastMessageBrokerSequenceId();
        }
        getAdapter().getCassandra().addSubscription(getDestination(), subscriptionInfo, lastAck);
    }


}