#qsandra - A Cassandra backed PersistenceAdapter for ActiveMQ

##Intro
While this adapter can be used against any existing cassandra installation, the goal is to provide an ActiveMQ broker cluster
that is available across multiple datacenters, that can tolerate the loss of a datacenter with no impact on availability
(like the existing ActiveMQ pure master-slave, except capable of more than 2 brokers/datacenters) while not having to bring the broker cluster down and copy data files around to
 restore a failed master (unlike the existing ActiveMQ pure master-slave), and have message state easily replicated to multiple datacenters
 without expensive database or storage software and hardware.

##Running
###Configuration
####ActiveMQ
To configure ActiveMQ to use Cassandra for message persistence, you need to know a few pieces of information.

You need the host and port of whatever is doing cassandra thrift interface load balancing for you.
If running more than one broker instance and using ZooKeeper for master election, you need your zookeeper connect string.

Here is an example spring config.

		<beans xmlns="http://www.springframework.org/schema/beans" 
  				xmlns:broker="http://activemq.apache.org/schema/core"
  				xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  				xsi:schemaLocation="http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans-2.0.xsd
  				http://activemq.apache.org/schema/core http://activemq.apache.org/schema/core/activemq-core.xsd">

         	<broker:broker useJmx="true" persistent="true">

                <broker:persistenceAdapter>
                    <ref bean="adapter"/>
                </broker:persistenceAdapter>

                <broker:transportConnectors>
                    <broker:transportConnector name="tcp"
                                               uri="tcp://messages.example.com:60001"/>
                </broker:transportConnectors>

            </broker:broker>

            <bean id="adapter" class="org.apache.activemq.store.cassandra.CassandraPersistenceAdapter">
                <property name="cassandraClient" ref="cassandraClient"/>
                <property name="masterElector" ref="masterElector"/>
            </bean>

            <bean id="cassandraClient" class="org.apache.activemq.store.cassandra.CassandraClient">
                <property name="cassandraHost" value="cassandra.example.com"/>
                <property name="cassandraPort" value="9160"/>
            </bean>

            <bean id="masterElector" class="org.apache.activemq.store.cassandra.ZooKeeperMasterElector">
                <property name="zookeeperConnectString" 
                value="zookeeper.datacenter1.example.com:9260,zookeeper.datacenter2.example.com:9260,zookeeper.datacenter3.example.com:9260"/>
            </bean>
		<beans> 

####Cassandra
The keyspace defined [here](qsandra/blob/master/src/main/resources/keyspace.xml) needs to be deployed into your cassandra cluster, *after you modify
the ReplicaPlacementStrategy,ReplicationFactor,and EndPointSnitch appropriately for your use case* 

####ZooKeeper
If you are using the ZooKeeperMasterElector, a persistent node will be created at /qsandra/, and ephemeral, sequential nodes willbe created
under this node, so if you have multiple broker clusters using the same ZooKeeper, use appropriate chroot suffixes in the connect strings
to partition the master election appropriately.

##Usecases
All of the usecases assume a familiarity with, or at least the willingness to learn, the techniques for running an Apache Cassandra cluster,
and in cases where there is more than a single ActiveMQ broker (when using ActiveMQ failover://), Apache ZooKeeper, which is used to elect the
master broker. (You can also write your own master election if you dont want to use zookeeper). Most of the work here is setting up Cassandra
and ZooKeeper for appropriate replication and availablity.


###Multi Datacenter HA ActiveMQ Broker Cluster
Due to the QUORUM mechanics of both Cassandra and ZooKeeper, to tolerate the loss of a datacenter while not impacting the availability of ActiveMQ
there must be at least 3 datacenters involved in this configuration. Each datacenter needs at least one zooKeeper instance, and at least one cassandra instance.

So lets say we have 3 datacenters, with 1 ZooKeeper node, 2 Cassandra nodes and 2 ActiveMQ brokers per data center.

Use the DatacenterShardStragegy for replica placement, set the ReplicationFactor to 6, and the ReplicationFactor for each data center to 2.

With QUORUM ConsistencyLevel for reads and writes, reads and writes will succeed when 4 (ReplicationFactor / 2 + 1) reads or writes are successful.
So this means if a datacenter is network partitioned or lost, we keep on trucking. Any 2 nodes of the 6 can be unavailable.

Similarly with Zookeeper, any 1 node of the 3 can be unavailable.

If the master broker is in the datacenter that dies, or is partitioned, one of the other brokers in the other datacenter will be elected master,
and clients should fail over (using failover:// brokerURL)


###Single Datacenter, Single Broker instance
This is a much simpler configuration. If a MasterElector is not set on the PersistenceAdapter, the broker will assume it is master.



##Building
There is a maven repo at [http://maven.shorrockin.com/](http://maven.shorrockin.com/) that thankfully unpacks and deploys all the cassandra dependencies, so

        mvn clean install

should do it.

##Installing
If you have a standard activemq installation, take the assembly (.zip or .tar.gz) produced by mvn install, or downloaded from
[http://http://www.ticktock.com/maven/com/ticktock/qsandra/1.1-SNAPSHOT/qsandra-1.1-SNAPSHOT-install.tar.gz](http://http://www.ticktock.com/maven/com/ticktock/qsandra/1.1-SNAPSHOT/qsandra-1.1-SNAPSHOT-install.tar.gz) or
[http://http://www.ticktock.com/maven/com/ticktock/qsandra/1.1-SNAPSHOT/qsandra-1.1-SNAPSHOT-install.zip](http://http://www.ticktock.com/maven/com/ticktock/qsandra/1.1-SNAPSHOT/qsandra-1.1-SNAPSHOT-install.zip)

and place it in the root of your activemq install and unpack. This will place the necessary jars in lib/optional. YOu can now include this persistence adapter in your broker configuration.

If using maven, and running an embedded activemq broker, you can get the adapter as a maven dependncy by adding the following repository and dependency to your pom

Add to repositories:
        <repository>
            <id>ticktock</id>
            <name>Ticktock Repository</name>
            <url>http://www.ticktock.com/maven/</url>
        </repository>

Add to dependencies:
        <dependency>
            <groupId>com.tickock</groupId>
            <artifactId>qsandra</artifactId>
            <version>1.1-SNAPSHOT</version>
        </dependency>

