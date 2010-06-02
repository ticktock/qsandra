package org.apache.activemq.store.cassandra;

import org.apache.cassandra.thrift.CassandraDaemon;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;

/**
 *
 */
public class EmbeddedCassandraService implements Runnable {

    CassandraDaemon cassandraDaemon;

    public void init() throws IOException, TTransportException {
        cassandraDaemon = new CassandraDaemon();
        cassandraDaemon.init(null);
    }

    public void run() {
        cassandraDaemon.start();
    }

    public void stop() {
        cassandraDaemon.stop();
    }


}