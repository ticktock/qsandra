package org.apache.activemq.store.cassandra;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 */
public class ZooKeeperMasterElector implements MasterElector, Watcher {


    private static Logger log = LoggerFactory.getLogger(ZooKeeperMasterElector.class);
    private ZooKeeper zk;
    private String path;
    private List<String> paths;
    private ReentrantLock lock = new ReentrantLock();
    private Condition condition = lock.newCondition();
    private Runnable lostMasterHandler;
    private AtomicBoolean isMaster = new AtomicBoolean(false);
    private String connectString;

    @Override
    public boolean isMaster() {
        return isMaster.get();
    }

    @Override
    public void waitTillMaster() {
        while (!isMaster.get()) {
            try {
                lock.lock();
                if (path.equals(paths.get(0))) {
                    log.warn("Path {} is the first path, this node is master", path);
                    isMaster.set(true);
                    return;
                } else {
                    log.warn("Path {} is not the first path, waiting...", path);
                    condition.await();
                }
            } catch (InterruptedException e) {
                log.error("InterruptedException while waiting on new paths", e);
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void setMasterLostHandler(Runnable handler) {
        lostMasterHandler = handler;
    }

    private void lostConnection() {
        log.warn("Lost Zookeeper Connection");
        if (isMaster.compareAndSet(true, false)) {
            log.error("Lost Zookeeper Connection and acting master, running lostMasterhandler");
            if (lostMasterHandler != null) {
                lostMasterHandler.run();
            } else {
                log.error("Lost Zookeeper Connection and acting master, but lostMasterhandler is NULL!!");
            }
        }
    }

    private void setPaths(List<String> newPaths) {
        lock.lock();
        Collections.sort(newPaths);
        paths = newPaths;
        for (String newPath : newPaths) {
            log.info("AllPaths:{}", newPath);
        }
        condition.signalAll();
        lock.unlock();
    }

    @Override
    public void start() {
        try {
            if (connectString == null) {
                throw new IllegalStateException("connectString cannot be null");
            }
            zk = new ZooKeeper(connectString, 10000, this);
            if (zk.exists("/qsandra", false) == null) {
                zk.create("/qsandra", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            path = zk.create("/qsandra/broker_", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            path = path.substring("/qsandra/".length());
            log.info("Path:{}", path);
            setPaths(zk.getChildren("/qsandra", true));

        } catch (Exception e) {
            log.error("Exception in start", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void process(WatchedEvent event) {

        log.debug("ZKEvent {} {} {} {}", new Object[]{event.getType().toString(), event.getPath(), event.getState().toString(), event.getWrapper()});
        if (Event.KeeperState.Disconnected.equals(event.getState()) || Event.KeeperState.Expired.equals(event.getState())) {
            lostConnection();
        } else if (("/qsandra").equals(event.getPath()) && event.getType().equals(Event.EventType.NodeChildrenChanged)) {
            try {
                setPaths(zk.getChildren("/qsandra", true));
            } catch (KeeperException e) {
                log.error("KeeperException while getting updated children", e);
            } catch (InterruptedException e) {
                log.error("InterruptedException while getting updated children", e);
            } catch (Exception e) {
                log.error("Exception while processing event", e);
            }
        }

    }

    public void stop() {
        try {
            zk.close();
        } catch (InterruptedException e) {
            log.error("Exception in stop", e);
            throw new RuntimeException(e);
        }
    }

    public void setZookeeperConnectString(String connect) {
        connectString = connect;
    }
}
