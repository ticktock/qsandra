package org.apache.activemq.store.cassandra;

/**
 *
 */
public enum CassandraIdentifier {

    KEYSPACE("MessageStore"),

    BROKER_FAMILY("Broker"),
    BROKER_KEY("Broker"),
    BROKER_DESTINATION_COUNT("destination-count"),


    DESTINATIONS_FAMILY("Destinations"),
    DESTINATION_IS_TOPIC_COLUMN("isTopic"),
    DESTINATION_MAX_STORE_SEQUENCE_COLUMN("max-store-sequence"),
    DESTINATION_MAX_BROKER_SEQUENCE_COLUMN("max-broker-sequence"),
    DESTINATION_QUEUE_SIZE_COLUMN("queue-size"),


    MESSAGES_FAMILY("Messages"),

    MESSAGE_TO_STORE_ID_FAMILY("MessageIdToStoreId"),

    STORE_IDS_IN_USE_FAMILY("StoreIdsInUse"),


    SUBSCRIPTIONS_FAMILY("Subscriptions"),
    SUBSCRIPTIONS_SELECTOR_SUBCOLUMN("selector"),
    SUBSCRIPTIONS_LAST_ACK_SUBCOLUMN("lastMessageAck"),
    SUBSCRIPTIONS_SUB_DESTINATION_SUBCOLUMN("subscribedDestination");

    private byte[] bytes;
    private String string;

    CassandraIdentifier(String id) {
        string = id;
        bytes = CassandraUtils.getBytes(string);
    }

    public String string() {
        return string;
    }


    public byte[] bytes() {
        return bytes;
    }
}