package org.apache.activemq.store.cassandra;

/**
 *
 */
public interface MasterElector {

    void waitTillMaster();

    void setMasterLostHandler(Runnable handler);

    void start();

    void stop();

    boolean isMaster();
}
