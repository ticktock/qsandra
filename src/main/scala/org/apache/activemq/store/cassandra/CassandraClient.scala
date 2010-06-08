package org.apache.activemq.store.cassandra.scala

import com.shorrockin.cascal.session._
import reflect.BeanProperty
import org.apache.activemq.store.cassandra.*
import org.apache.activemq.store.cassandra.scala.*
import CassandraClient.Id._
import org.apache.activemq.command.ActiveMQDestination
import org.apache.cassandra.utils.BloomFilter
import com.shorrockin.cascal.model.Column

class CassandraClient() {
  @BeanProperty var cassandraHost: String = _
  @BeanProperty var cassandraPort: int = _
  @BeanProperty var cassandraTimeout: int = _



  protected var pool: SessionPool = null

  def start() = {
    val params = new PoolParams(20, ExhaustionPolicy.Fail, 500L, 6, 2)
    var hosts = Host(cassandraHost, cassandraPort, cassandraTimeout) :: Nil
    pool = new SessionPool(hosts, params, Consistency.Quorum)
  }

  def stop() = {
    pool.close
  }

  protected def withSession[E](block: Session => E): E = {
    val session = pool.checkout
    try {
      block(session)
    } finally {
      pool.checkin(session)
    }
  }

  def getDestinationCount(): int = {
    withSession {
      session =>
        session.get
        (KEYSPACE \ BROKER_FAMILY \ BROKER_KEY \ BROKER_DESTINATION_COUNT)
        match {
          case Some(x) =>
            x.get
          case None =>
            insertDestinationCount(0)
            0
        }
    }
  }

  def insertDestinationCount(count: int) = {
    withSession {
      session =>
        session.insert(KEYSPACE \ BROKER_FAMILY \ BROKER_KEY \ (BROKER_DESTINATION_COUNT, count))
    }
  }

  def getMessageIdFilterFor(destination: ActiveMQDestination, size: long): BloomFilter = {
    val filterSize = Math.max(size, 10000)
    val bloomFilter = BloomFilter.getFilter(filterSize, 0.01d);
    var start = ""
    val end = ""
    var count = 0
    while (count < filterSize) {
      withSession {
        session =>
          val cols = session.list(KEYSPACE \ MESSAGE_TO_STORE_ID_FAMILY \ destination, RangePredicate(start, end))
          for (val col: Column <- cols) {
            bloomFilter.add(col.name)
            count ++
          }
      }
    }
    bloomFilter
  }

}

object CassandraClient {
  implicit def destinationKey(destination: ActiveMQDestination): String = {
    destination.getQualifiedName
  }

  implicit def destinationFromKey(key: String): ActiveMQDestination = {
    ActiveMQDestination.createDestination(key, ActiveMQDestination.QUEUE_TYPE)
  }

  object Id {
    val KEYSPACE = "MessageStore"
    val BROKER_FAMILY = "Broker"
    val BROKER_KEY = "Broker"
    val BROKER_DESTINATION_COUNT = "destination-count"

    val DESTINATIONS_FAMILY = "Destinations"
    val DESTINATION_IS_TOPIC_COLUMN = "isTopic"
    val DESTINATION_MAX_STORE_SEQUENCE_COLUMN = "max-store-sequence"
    val DESTINATION_MAX_BROKER_SEQUENCE_COLUMN = "max-broker-sequence"
    val DESTINATION_QUEUE_SIZE_COLUMN = "queue-size"


    val MESSAGES_FAMILY = "Messages"

    val MESSAGE_TO_STORE_ID_FAMILY = "MessageIdToStoreId"

    val STORE_IDS_IN_USE_FAMILY = "StoreIdsInUse"


    val SUBSCRIPTIONS_FAMILY = "Subscriptions"
    val SUBSCRIPTIONS_SELECTOR_SUBCOLUMN = "selector"
    val SUBSCRIPTIONS_LAST_ACK_SUBCOLUMN = "lastMessageAck"
    val SUBSCRIPTIONS_SUB_DESTINATION_SUBCOLUMN = "subscribedDestination";

  }

}