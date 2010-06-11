package org.apache.activemq.store.cassandra;

import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.PersistenceAdapterFactory;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 *
 */
public class CassandraPersistenceAdapterFactory implements PersistenceAdapterFactory {

    private Logger log = Logger.getLogger(CassandraPersistenceAdapterFactory.class);
    private String cassandraHost;
    private int cassandraPort;
    private String zookeeperConnectString;


    public PersistenceAdapter createPersistenceAdapter() throws IOException {
        CassandraPersistenceAdapter adapter = new CassandraPersistenceAdapter();
        CassandraClient client = new CassandraClient();
        client.setCassandraHost(cassandraHost);
        client.setCassandraPort(cassandraPort);
        adapter.setCassandraClient(client);
        ZooKeeperMasterElector zookeeperMasterElector = new ZooKeeperMasterElector();
        zookeeperMasterElector.setZookeeperConnectString(zookeeperConnectString);
        adapter.setMasterElector(zookeeperMasterElector);
        return adapter;
    }

    public int getCassandraPort() {
        return cassandraPort;
    }

    public void setCassandraPort(int cassandraPort) {
        this.cassandraPort = cassandraPort;
    }

    public String getCassandraHost() {
        return cassandraHost;
    }

    public void setCassandraHost(String cassandraHost) {
        this.cassandraHost = cassandraHost;
    }

    public String getZookeeperConnectString() {
        return zookeeperConnectString;
    }

    public void setZookeeperConnectString(String zookeeperConnectString) {
        this.zookeeperConnectString = zookeeperConnectString;
    }


}
