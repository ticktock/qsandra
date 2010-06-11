package org.apache.activemq.store.cassandra

import com.shorrockin.cascal.session._
import com.shorrockin.cascal.utils.Conversions._
import collection.jcl.Conversions._
import reflect.BeanProperty
import CassandraClient._
import org.apache.cassandra.utils.BloomFilter
import grizzled.slf4j.Logger
import org.apache.activemq.store.cassandra.{DestinationMaxIds => Max}
import java.util.concurrent.atomic.{AtomicLong, AtomicInteger}
import org.apache.activemq.command.{SubscriptionInfo, MessageId, ActiveMQDestination}
import collection.jcl.{ArrayList, HashSet, Set}
import org.apache.cassandra.thrift.{NotFoundException}
import java.lang.String
import collection.mutable.{HashMap, ListBuffer}

class CassandraClient() {
  @BeanProperty var cassandraHost: String = _
  @BeanProperty var cassandraPort: Int = _
  @BeanProperty var cassandraTimeout: Int = _
  @BeanProperty var verifyKeyspace: Boolean = true;


  protected var pool: SessionPool = null

  def start() = {
    val params = new PoolParams(20, ExhaustionPolicy.Fail, 500L, 6, 2)
    var hosts = Host(cassandraHost, cassandraPort, cassandraTimeout) :: Nil
    pool = new SessionPool(hosts, params, Consistency.Quorum)
    if (verifyKeyspace) {
      verifyMessageStoreKeyspace
    }
  }

  def stop() = {
    pool.close
  }

  protected def withSession[E](block: Session => E): E = {
    pool.borrow {session => block(session)}
  }

  def getDestinationCount(): Int = {
    withSession {
      session =>
        session.get(KEYSPACE \ BROKER_FAMILY \ BROKER_KEY \ BROKER_DESTINATION_COUNT) match {
          case Some(x) =>
            x.value
          case None =>
            insertDestinationCount(0)
            0
        }
    }
  }

  def insertDestinationCount(count: Int) = {
    withSession {
      session =>
        session.insert(KEYSPACE \ BROKER_FAMILY \ BROKER_KEY \ (BROKER_DESTINATION_COUNT, count))
    }
  }

  def getMessageIdFilterFor(destination: ActiveMQDestination, size: Long): BloomFilter = {
    val filterSize = Math.max(size, 10000)
    val bloomFilter = BloomFilter.getFilter(filterSize, 0.01d);
    var start = ""
    val end = ""
    var counter: Int = 0
    while (counter < size) {
      withSession {
        session =>
          val cols = session.list(KEYSPACE \ MESSAGE_TO_STORE_ID_FAMILY \ destination, RangePredicate(start, end))
          cols.foreach(col => {
            bloomFilter.add(col.name)
            start = col.name
            counter = counter + 1;
          })

      }
    }
    bloomFilter
  }


  def createDestination(name: String, isTopic: Boolean, destinationCount: AtomicInteger): Boolean = {
    withSession {
      session =>
        session.get(KEYSPACE \ DESTINATIONS_FAMILY \ name \ DESTINATION_IS_TOPIC_COLUMN) match {
          case Some(x) =>
            logger.info("Destination %s exists".format(name))
            return false
          case None =>
            val topic = KEYSPACE \ DESTINATIONS_FAMILY \ name \ (DESTINATION_IS_TOPIC_COLUMN, isTopic)
            val maxStore = KEYSPACE \ DESTINATIONS_FAMILY \ name \ (DESTINATION_MAX_STORE_SEQUENCE_COLUMN, 0L)
            val queueSize = KEYSPACE \ DESTINATIONS_FAMILY \ name \ (DESTINATION_QUEUE_SIZE_COLUMN, 0L)
            val destCount = KEYSPACE \ BROKER_FAMILY \ BROKER_KEY \ (BROKER_DESTINATION_COUNT, destinationCount.incrementAndGet)
            try {
              session.batch(Insert(topic) :: Insert(maxStore) :: Insert(queueSize) :: Insert(destCount))
              true
            } catch {
              case e: RuntimeException =>
                destinationCount.decrementAndGet
                throw e
            }

        }
    }
  }


  def getDestinations(): java.util.Set[ActiveMQDestination] = {
    val destinations = new HashSet[ActiveMQDestination]
    withSession {
      session =>
        session.list(KEYSPACE \ DESTINATIONS_FAMILY, KeyRange("", "", 10000)).foreach {
          case (key, cols) => {
            destinations.add(key.value)
          }
        }

    }
    destinations
  }


  def deleteQueue(destination: ActiveMQDestination, destinationCount: AtomicInteger): Unit = {
    withSession {
      session =>
        session.remove(KEYSPACE \ MESSAGES_FAMILY \ destination)
        session.remove(KEYSPACE \ DESTINATIONS_FAMILY \ destination)
        session.remove(KEYSPACE \ MESSAGE_TO_STORE_ID_FAMILY \ destination)
        session.remove(KEYSPACE \ STORE_IDS_IN_USE_FAMILY \ destination)
        session.remove(KEYSPACE \\ SUBSCRIPTIONS_FAMILY \ destination)
        try {
          val count = KEYSPACE \ BROKER_FAMILY \ BROKER_KEY \ (BROKER_DESTINATION_COUNT, destinationCount.decrementAndGet)
          session.insert(count)
        } catch {
          case e: RuntimeException =>
            destinationCount.incrementAndGet
            throw e
        }
    }

  }

  def deleteTopic(destination: ActiveMQDestination, destinationCount: AtomicInteger): Unit = {
    deleteQueue(destination, destinationCount);
  }

  def getMaxStoreId(): Max = {
    var max: Max = new Max(null, 0, 0)
    val destinations = getDestinations.size
    if (destinations == 0) {
      return max;
    }
    var storeVal: Long = 0
    var broker: Long = 0
    withSession {
      session =>
        session.list(KEYSPACE \ DESTINATIONS_FAMILY, new KeyRange("", "", 10000), ColumnPredicate(
          List(DESTINATION_MAX_STORE_SEQUENCE_COLUMN, DESTINATION_MAX_BROKER_SEQUENCE_COLUMN)
          ), Consistency.Quorum).foreach {
          case (key, columns) => {
            columns.foreach {
              col => {
                if (col.name == bytes(DESTINATION_MAX_STORE_SEQUENCE_COLUMN))
                  storeVal = col.value
                else if (col.name == bytes(DESTINATION_MAX_BROKER_SEQUENCE_COLUMN))
                  broker = col.value
              }
            }
          }
          if (storeVal > max.getMaxStoreId) {
            max = new Max(key.value, storeVal, broker)
          }
        }
    }
    max
  }

  def getStoreId(destination: ActiveMQDestination, id: MessageId): Long = {
    withSession {
      session =>
        session.get(KEYSPACE \ MESSAGE_TO_STORE_ID_FAMILY \ destination \ id.toString) match {
          case Some(x) =>
            x.value
          case None =>
            logger.error("Store Id not found in destination %s for id %s".format(destination, id))
            throw new RuntimeException("Store Id not found");
        }
    }
  }

  def getMessage(destination: ActiveMQDestination, storeId: Long): Array[Byte] = {
    withSession {
      session =>
        session.get(KEYSPACE \ MESSAGES_FAMILY \ destination \ storeId) match {
          case Some(x) =>
            x.value
          case None =>
            logger.error("Message Not Found for destination:%s id:%s".format(destination, storeId))
            throw new RuntimeException(new NotFoundException);
        }
    }
  }

  def saveMessage(destination: ActiveMQDestination, id: Long, messageId: MessageId, message: Array[Byte], queueSize: AtomicLong, duplicateDetector: BloomFilter): Unit = {
    withSession {
      session =>
        if (duplicateDetector.isPresent(messageId.toString)) {
          session.get(KEYSPACE \ MESSAGE_TO_STORE_ID_FAMILY \ destination \ messageId.toString) match {
            case Some(x) => {
              logger.warn("Duplicate Message Save recieved from broker for %s...ignoring".format(messageId))
              return
            }
            case None =>
              logger.warn("NotFoundException while confirming duplicate, BloomFilter false positive, continuing")
          }
        }

        logger.debug("Saving message with id:%d".format(id));
        logger.debug("Saving message with messageId:%s".format(messageId.toString));
        logger.debug("Saving message with brokerSeq id:%d".format(messageId.getBrokerSequenceId()));

        val mesg = KEYSPACE \ MESSAGES_FAMILY \ destination \ (id, message)
        val destQ = KEYSPACE \ DESTINATIONS_FAMILY \ destination \ (DESTINATION_QUEUE_SIZE_COLUMN, bytes(queueSize.incrementAndGet))
        val destStore = KEYSPACE \ DESTINATIONS_FAMILY \ destination \ (DESTINATION_MAX_STORE_SEQUENCE_COLUMN, id)
        val destBrok = KEYSPACE \ DESTINATIONS_FAMILY \ destination \ (DESTINATION_MAX_BROKER_SEQUENCE_COLUMN, messageId.getBrokerSequenceId)
        val idx = KEYSPACE \ MESSAGE_TO_STORE_ID_FAMILY \ destination \ (messageId.toString, id)
        val storeId = KEYSPACE \ STORE_IDS_IN_USE_FAMILY \ destination \ (id, "")
        try {
          session.batch(Insert(mesg) :: Insert(destQ) :: Insert(destStore) :: Insert(destBrok) :: Insert(idx) :: Insert(storeId));
          duplicateDetector.add(messageId.toString)
        } catch {
          case e: RuntimeException =>
            queueSize.decrementAndGet
            logger.error("Exception saving message", e)
            throw e
        }

    }

  }

  def deleteMessage(destination: ActiveMQDestination, id: MessageId, queueSize: AtomicLong): Unit = {
    val col = getStoreId(destination, id);
    val dest = KEYSPACE \ DESTINATIONS_FAMILY \ destination \ (DESTINATION_QUEUE_SIZE_COLUMN, queueSize.decrementAndGet)
    val mes = KEYSPACE \ MESSAGES_FAMILY \ destination
    val store = KEYSPACE \ STORE_IDS_IN_USE_FAMILY \ destination
    val idx = KEYSPACE \ MESSAGE_TO_STORE_ID_FAMILY \ destination
    try {
      withSession {
        session =>
          session.batch(Delete(mes, ColumnPredicate(col :: Nil)) :: Delete(store, ColumnPredicate(col :: Nil)) :: Delete(idx, ColumnPredicate(id.toString :: Nil)) :: Insert(dest))
      }
    } catch {
      case e: RuntimeException =>
        queueSize.incrementAndGet
        logger.error("Exception saving message", e)
        throw e
    }
  }

  def deleteAllMessages(destination: ActiveMQDestination, queueSize: AtomicLong): Unit = {
    withSession {
      session =>
        session.remove(KEYSPACE \ MESSAGES_FAMILY \ destination)
        queueSize.set(0)
    }
  }

  def getMessageCount(destination: ActiveMQDestination): Int = {
    withSession {
      session =>
        session.get(KEYSPACE \ DESTINATIONS_FAMILY \ destination \ DESTINATION_QUEUE_SIZE_COLUMN) match {
          case Some(x) =>
            long(x.value).intValue
          case None =>
            throw new RuntimeException("Count not found for destination" + destination);
        }
    }
  }

  def recoverMessages(destination: ActiveMQDestination, batchPoint: AtomicLong, maxReturned: Int): java.util.List[Array[Byte]] = {
    logger.debug("recoverMessages(%s, %s,%s)".format(destination, batchPoint, maxReturned))
    var start: Array[Byte] = new Array[Byte](0)
    if (batchPoint.get != -1) {
      start = batchPoint.get
    }
    val end: Array[Byte] = new Array[Byte](0)
    val messages = new ArrayList[Array[Byte]]
    recoverMessagesFromTo(destination, start, end, maxReturned, messages)
    messages
  }

  private def recoverMessagesFromTo(key: String, start: Array[Byte], end: Array[Byte], limit: Int, messages: ArrayList[Array[Byte]]): Unit = {
    logger.debug("recoverMessagesFrom(%s,%s,%s,%s,%s)".format(key, start, end, limit, messages.length))
    withSession {
      session =>
        val range = RangePredicate(Some(start), Some(end), Order.Ascending, Some(limit))
        session.list(KEYSPACE \ MESSAGES_FAMILY \ key, range, Consistency.Quorum).foreach {
          col =>
            if (messages.size < limit) messages.add(col.value)
        }
    }
  }

  def addSubscription(destination: ActiveMQDestination, subscriptionInfo: SubscriptionInfo, ack: Long): Unit = {
    withSession {
      session =>
        val supercolumnName = subscriptionSupercolumn(subscriptionInfo)
        val supercolumn = KEYSPACE \\ SUBSCRIPTIONS_FAMILY \ destination \ supercolumnName
        val subdest = supercolumn \ (SUBSCRIPTIONS_SUB_DESTINATION_SUBCOLUMN, subscriptionInfo.getSubscribedDestination)
        val ackcol = supercolumn \ (SUBSCRIPTIONS_LAST_ACK_SUBCOLUMN, ack)
        var list = Insert(subdest) :: Insert(ackcol)
        if (subscriptionInfo.getSelector != null) {
          val selcolopt = supercolumn \ (SUBSCRIPTIONS_SELECTOR_SUBCOLUMN, subscriptionInfo.getSelector)
          list.add(Insert(selcolopt))
        }
        session.batch(list)
    }
  }

  def lookupSubscription(destination: ActiveMQDestination, clientId: String, subscriptionName: String): SubscriptionInfo = {
    withSession {
      session =>
        var subscriptionInfo = new SubscriptionInfo
        subscriptionInfo.setClientId(clientId)
        subscriptionInfo.setSubscriptionName(subscriptionName)
        subscriptionInfo.setDestination(destination)
        session.get(KEYSPACE \\ SUBSCRIPTIONS_FAMILY \ destination \ getSubscriptionSuperColumnName(clientId, subscriptionName)) match {
          case Some(seq) => {
            seq.foreach {
              column => {
                string(column.name) match {
                  case SUBSCRIPTIONS_SELECTOR_SUBCOLUMN =>
                    subscriptionInfo.setSelector(column.value)
                  case SUBSCRIPTIONS_SUB_DESTINATION_SUBCOLUMN =>
                    subscriptionInfo.setSubscribedDestination(column.value)
                  case _ => None
                }
              }
            }
            subscriptionInfo
          }
          case None => {
            logger.info("lookupSubscription failed to find the subscription")
            return null
          }
        }

    }
  }

  def lookupAllSubscriptions(destination: ActiveMQDestination): Array[SubscriptionInfo] = {
    withSession {
      session =>
        val subs = new ListBuffer[SubscriptionInfo]
        session.list(KEYSPACE \\ SUBSCRIPTIONS_FAMILY \ destination, EmptyPredicate, Consistency.Quorum).foreach {
          case (superCol, columnList) => {
            var key: String = superCol.value
            logger.warn(string(columnList.length))
            var subscriptionInfo = new SubscriptionInfo
            subscriptionInfo.setClientId(getClientIdFromSubscriptionKey(key))
            subscriptionInfo.setSubscriptionName(getSubscriptionNameFromSubscriptionKey(key))
            subscriptionInfo.setDestination(destination)
            columnList.foreach {
              column => {
                string(column.name) match {
                  case SUBSCRIPTIONS_SELECTOR_SUBCOLUMN =>
                    logger.debug("got selector col")
                    subscriptionInfo.setSelector(column.value)
                  case SUBSCRIPTIONS_SUB_DESTINATION_SUBCOLUMN =>
                    logger.debug("got sub dest col")
                    subscriptionInfo.setSubscribedDestination(column.value)
                  case _ => None
                }
              }
            }

            subs + subscriptionInfo
          }
        }
        subs.toArray
    }

  }

  def acknowledge(destination: ActiveMQDestination, clientId: String, subscriptionName: String, id: MessageId): Unit = {
    val lastAckStoreId: Long = getStoreId(destination, id);
    val superCol = KEYSPACE \\ SUBSCRIPTIONS_FAMILY \ destination \ getSubscriptionSuperColumnName(clientId, subscriptionName)
    val ackCol = superCol \ (SUBSCRIPTIONS_LAST_ACK_SUBCOLUMN, lastAckStoreId)
    withSession {
      session =>
        session.insert(ackCol)
    }
  }

  def deleteSubscription(destination: ActiveMQDestination, clientId: String, subscriptionName: String): Unit = {
    val superCol = KEYSPACE \\ SUBSCRIPTIONS_FAMILY \ destination \ getSubscriptionSuperColumnName(clientId, subscriptionName)
    withSession {
      session =>
        session.remove(superCol)
    }
  }

  def getMessageCountFrom(destination: ActiveMQDestination, storeId: Long): Int = {
    withSession {
      session =>
        session.list(KEYSPACE \ STORE_IDS_IN_USE_FAMILY \ destination, RangePredicate(Some(storeId), None, Order.Ascending, None)).size
    }
  }

  def getLastAckStoreId(destination: ActiveMQDestination, clientId: String, subscriptionName: String): Int = {
    withSession {
      session =>
        session.get(KEYSPACE \\ SUBSCRIPTIONS_FAMILY \ destination \ getSubscriptionSuperColumnName(clientId, subscriptionName) \ SUBSCRIPTIONS_LAST_ACK_SUBCOLUMN) match {
          case Some(col) =>
            long(col.value).intValue
          case None => {
            logger.debug("LastAckStoreId Not found, returning 0");
            0
          }

        }
    }
  }

  def getSubscriberId(clientId: String, subscriptionName: String): String = {
    return getSubscriptionSuperColumnName(clientId, subscriptionName)
  }

  def verifyMessageStoreKeyspace(): Unit = {
    withSession {
      session =>

        val keyspace = convertMap(session.client.describe_keyspace(KEYSPACE))

        val stdCols: List[String] = MESSAGES_FAMILY :: STORE_IDS_IN_USE_FAMILY :: DESTINATIONS_FAMILY :: BROKER_FAMILY :: MESSAGE_TO_STORE_ID_FAMILY :: Nil
        val superCols: List[String] = SUBSCRIPTIONS_FAMILY :: Nil
        val allCols: List[String] = stdCols ++ superCols


        allCols.foreach {
          family =>
            keyspace.get(family) match {
              case Some(x) => None
              case None => throw new RuntimeException("ColumnFamily: %s missing from keyspace".format(family))
            }
        }

        stdCols.foreach {
          family =>
            (keyspace.get(family): @unchecked) match {
              case Some(map) => convertMap(map).get(DESCRIBE_CF_TYPE) match {
                case Some(colType) => colType match {
                  case DESCRIBE_CF_TYPE_STANDARD => None
                  case _ => throw new RuntimeException("Type of the ColumnFamily was not expected to be:%s".format(colType))
                }
                case None => throw new RuntimeException("Type wasnt part of the column description")
              }
            }
        }

        superCols.foreach {
          family =>
            (keyspace.get(family): @unchecked) match {
              case Some(map) => convertMap(map).get(DESCRIBE_CF_TYPE) match {
                case Some(colType) => colType match {
                  case DESCRIBE_CF_TYPE_SUPER => None
                  case _ => throw new RuntimeException("Type of the ColumnFamily was not expected to be:%s".format(colType))
                }
                case None => throw new RuntimeException("Type wasnt part of the column description")
              }
            }
        }
    }
  }

}

object CassandraClient {
  val logger = Logger(this.getClass)

  implicit def getDestinationKey(destination: ActiveMQDestination): String = {
    destination.getQualifiedName
  }

  implicit def destinationBytes(destination: ActiveMQDestination): Array[Byte] = {
    bytes(getDestinationKey(destination))
  }

  implicit def bytesToDest(bytes: Array[Byte]): ActiveMQDestination = {
    destinationFromKey(string(bytes))
  }

  implicit def destinationFromKey(key: String): ActiveMQDestination = {
    ActiveMQDestination.createDestination(key, ActiveMQDestination.QUEUE_TYPE)
  }

  private def subscriptionSupercolumn(info: SubscriptionInfo): String = {
    return info.getClientId + SUBSCRIPTIONS_CLIENT_SUBSCRIPTION_DELIMITER + nullSafeGetSubscriptionName(info)
  }


  private def nullSafeGetSubscriptionName(info: SubscriptionInfo): String = {
    return if (info.getSubscriptionName != null) info.getSubscriptionName else SUBSCRIPTIONS_DEFAULT_SUBSCRIPTION_NAME
  }


  private def getSubscriptionSuperColumnName(clientId: String, subscriptionName: String): String = {
    return clientId + SUBSCRIPTIONS_CLIENT_SUBSCRIPTION_DELIMITER + (if (subscriptionName != null) subscriptionName else SUBSCRIPTIONS_DEFAULT_SUBSCRIPTION_NAME)
  }


  private def getClientIdFromSubscriptionKey(key: String): String = {
    var split: Array[String] = key.split(SUBSCRIPTIONS_CLIENT_SUBSCRIPTION_DELIMITER)
    return split(0)
  }


  private def getSubscriptionNameFromSubscriptionKey(key: String): String = {
    var split: Array[String] = key.split(SUBSCRIPTIONS_CLIENT_SUBSCRIPTION_DELIMITER)
    if (split(1).equals(SUBSCRIPTIONS_DEFAULT_SUBSCRIPTION_NAME)) {
      return null
    }
    else {
      return split(1)
    }
  }

  def safeGetLong(bytes: Array[Byte]): Long = {
    if (bytes.length != 8) {
      logger.debug("bytes length was %d, not 8, returning -1".format(bytes.length))
      return -1L
    }
    else {
      return long(bytes)
    }
  }

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


  val DESCRIBE_CF_TYPE = "Type"
  val DESCRIBE_CF_TYPE_STANDARD = "Standard"
  val DESCRIBE_CF_TYPE_SUPER = "Super"


  /*Subscriptions Column Family Constants*/


  val SUBSCRIPTIONS_CLIENT_SUBSCRIPTION_DELIMITER: String = "~~~~~"
  val SUBSCRIPTIONS_DEFAULT_SUBSCRIPTION_NAME: String = "@NOT_SET@"

}

object CassandraClientUtil {
  def getDestinationKey(destination: ActiveMQDestination): String = {
    CassandraClient.getDestinationKey(destination)
  }

  val MESSAGES_FAMILY = CassandraClient.MESSAGES_FAMILY
}