package org.apache.activemq.store.cassandra;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.*;
import org.apache.activemq.store.AbstractMessageStore;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.usage.MemoryUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class CassandraMessageStore extends AbstractMessageStore {

    Logger log = LoggerFactory.getLogger(CassandraMessageStore.class);
    private CassandraPersistenceAdapter adapter;

    private MemoryUsage memoryUsage;
    protected AtomicLong lastStoreSequenceId = new AtomicLong(-1);
    private AtomicLong queueSize = new AtomicLong(0);


    public CassandraMessageStore(CassandraPersistenceAdapter adapter, ActiveMQDestination destination) {
        super(destination);
        this.adapter = adapter;
    }

    public void addMessage(ConnectionContext context, Message message) throws IOException {
        getAdapter().getCassandra().saveMessage(destination, getAdapter().getStoreSequenceGenerator().incrementAndGet(), message.getMessageId(), getAdapter().marshall(message), queueSize);
    }

    public Message getMessage(MessageId identity) throws IOException {
        long id = getAdapter().getCassandra().getStoreId(destination, identity);
        byte[] messageBytes = getAdapter().getCassandra().getMessage(destination, id);
        return getAdapter().unmarshall(messageBytes);

    }

    public void removeMessage(ConnectionContext context, MessageAck ack) throws IOException {
        MessageId id = ack.getLastMessageId();
        getAdapter().getCassandra().deleteMessage(destination, id, queueSize);
    }

    public void removeAllMessages(ConnectionContext context) throws IOException {
        getAdapter().getCassandra().deleteAllMessages(destination, queueSize);
    }


    public void recover(MessageRecoveryListener container) throws Exception {
        List<byte[]> messages = getAdapter().getCassandra().recoverMessages(destination, new AtomicLong(-1), Integer.MAX_VALUE);
        for (byte[] message : messages) {
            if (!container.recoverMessage(getAdapter().unmarshall(message))) {
                break;
            }
        }
    }

    public void recoverNextMessages(int maxReturned, MessageRecoveryListener listener) throws Exception {
        List<byte[]> messages = getAdapter().getCassandra().recoverMessages(destination, lastStoreSequenceId, maxReturned);
        Message lastRecovered = null;
        for (int i = 0; i < messages.size(); i++) {
            Message real = getAdapter().unmarshall(messages.get(i));
            if (listener.recoverMessage(real)) {
                lastRecovered = real;
                if (log.isDebugEnabled()) {
                    log.debug("recovered message with BrokerSequence:{}", real.getMessageId().getBrokerSequenceId());
                }

            } else {
                log.debug("stopped recovery");
                break;
            }
        }
        if (lastRecovered != null) {
            long lastStore = getAdapter().getCassandra().getStoreId(destination, lastRecovered.getMessageId());
            lastStoreSequenceId.set(lastStore + 1);
        }
    }

    public void setBatch(MessageId messageId) throws Exception {
        long storeId = getAdapter().getCassandra().getStoreId(destination, messageId);
        lastStoreSequenceId.set(storeId + 1);
        log.debug("setBatch {}", lastStoreSequenceId.get());
    }

    public void resetBatching() {
        lastStoreSequenceId.set(-1);
        log.debug("resetBatch {}", lastStoreSequenceId.get());
    }

    public ActiveMQDestination getDestination() {
        return destination;
    }


    public int getMessageCount() throws IOException {
        return queueSize.intValue();
    }


    public void start() throws Exception {
        log.debug("start()");
        int count = getAdapter().getCassandra().getMessageCount(destination);
        queueSize.set(count);
        if (log.isDebugEnabled()) {
            log.debug("Destination: {} has {} ", CassandraUtils.getDestinationKey(destination), queueSize.get());
        }
    }

    public void stop() throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("stop()");
            log.debug("Destination: {} has {} ", CassandraUtils.getDestinationKey(destination), queueSize.get());
            log.debug("Store: {} has {} ", CassandraUtils.getDestinationKey(destination), getAdapter().getCassandra().getMessageCount(destination));
        }
    }


    protected CassandraPersistenceAdapter getAdapter() {
        return adapter;
    }
}
