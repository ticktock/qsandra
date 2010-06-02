package org.apache.activemq.store.cassandra;

import org.apache.activemq.command.ActiveMQDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class DestinationMaxIds {

    private ActiveMQDestination destination;
    private long maxStoreId;
    private long maxBrokerSeq;

    public DestinationMaxIds(ActiveMQDestination destination, long maxStoreId, long maxBrokerSeq) {
        this.destination = destination;
        this.maxStoreId = maxStoreId;
        this.maxBrokerSeq = maxBrokerSeq;
    }

    public ActiveMQDestination getDestination() {
        return destination;
    }

    public long getMaxStoreId() {
        return maxStoreId;
    }

    public long getMaxBrokerSeq() {
        return maxBrokerSeq;
    }
}
