package org.apache.activemq.store.cassandra;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 */
public class EmbeddedZookeeperService implements Runnable {

    Logger log = LoggerFactory.getLogger(EmbeddedZookeeperService.class);

    ZooKeeperServerMain zksm;
    ServerConfig serverConfig;

    public void init(String port, String data) {
        zksm = new ZooKeeperServerMain();
        serverConfig = new ServerConfig();
        serverConfig.parse(new String[]{port, data});

    }

    public void run() {
        try {
            zksm.runFromConfig(serverConfig);
        } catch (IOException e) {
            log.error("Cant Run Zookeeper", e);
        }
    }


}
