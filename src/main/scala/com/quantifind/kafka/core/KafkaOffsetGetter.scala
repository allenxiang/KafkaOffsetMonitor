package com.quantifind.kafka.core

import java.lang.Long
import java.nio.ByteBuffer
import java.util
import java.util.Properties

import com.quantifind.kafka.OffsetGetter.{BrokerInfo, OffsetInfo}
import com.quantifind.kafka.offsetapp.{OffsetGetterArgs, OffsetGetterWeb, OffsetInfoReporter}
import com.quantifind.kafka.{Node, OffsetGetter}
import com.quantifind.utils.Utils.retryTask
import com.twitter.util.Time
import kafka.admin.AdminClient
import kafka.common.OffsetAndMetadata
import kafka.coordinator.group._
import kafka.utils.Logging
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.{PartitionInfo, TopicPartition}

import scala.collection.JavaConversions._
import scala.collection.{JavaConversions, _}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, duration}

/**
  * Created by rcasey on 11/16/2016.
  */
class KafkaOffsetGetter extends OffsetGetter {

  import KafkaOffsetGetter._

  override def getOffsetInfoByTopic(group: String, topic: String): Seq[OffsetInfo] = {
    val partitionInfoList: Option[util.List[PartitionInfo]] = topicPartitionsMap.get(topic)
    partitionInfoList match {
      case None => Seq()
      case Some(partitionInfoListValue) =>
        val offsetInfoSeq = partitionInfoListValue.flatMap(partitionInfo => {
          val topicPartition = new TopicPartition(topic, partitionInfo.partition)
          val offsetMetaData = committedOffsetMap.get(GroupTopicPartition(group, topicPartition))

          offsetMetaData match {
            case None => None
            case Some(offsetMetaDataValue) =>
              getOffsetInfoByPartition(GroupTopicPartition(group, topicPartition), offsetMetaDataValue)
          }
        })

        offsetInfoSeq
    }
  }

  private def getOffsetInfoByPartition(groupTopicPartition: GroupTopicPartition, offsetMetaData: OffsetAndMetadata): Option[OffsetInfo] = {
    val logEndOffset: Option[Long] = topicPartitionOffsetsMap.get(groupTopicPartition.topicPartition)

    logEndOffset match {
      case None => None
      case Some(logEnOffsetValue) =>
        val committedOffset: Long = offsetMetaData.offset
        val lag: Long = logEnOffsetValue - committedOffset
        val logEndOffsetReported: Long = if (lag < 0) committedOffset else logEnOffsetValue

        // Get client information if we can find an associated client
        var clientString: Option[String] = Option("NA")
        val filteredClients = clients.filter(c => c.group == groupTopicPartition.group && c.topicPartitions.contains(groupTopicPartition.topicPartition))
        if (filteredClients.nonEmpty) {
          val client: ClientGroup = filteredClients.head
          clientString = Option(client.clientId + client.clientHost)
        }

        Some(OffsetInfo(group = groupTopicPartition.group,
          topic = groupTopicPartition.topicPartition.topic,
          partition = groupTopicPartition.topicPartition.partition,
          offset = committedOffset,
          logSize = logEndOffsetReported,
          owner = clientString,
          creation = Time.fromMilliseconds(offsetMetaData.commitTimestamp),
          modified = Time.fromMilliseconds(offsetMetaData.expireTimestamp)))
    }
  }

  override def getGroups: Seq[String] = {
    topicAndGroups.groupBy(_.group).keySet.toSeq.sorted
  }

  override def getTopicList(group: String): List[String] = {
    topicAndGroups.filter(_.group == group).groupBy(_.topic).keySet.toList.sorted
  }

  override def getActiveTopicMap: Map[String, Seq[String]] = {
    getTopicMap
  }

  override def getTopicMap: Map[String, scala.Seq[String]] = {
    topicAndGroups.groupBy(_.topic).mapValues(_.map(_.group).toSeq)
  }

  override def getTopics: Seq[String] = {
    topicPartitionsMap.keys.toSeq.sorted
  }

  override def getClusterViz: Node = {
    val clusterNodes = topicPartitionsMap.values.map(partition => {
      Node(partition.get(0).leader().host() + ":" + partition.get(0).leader().port(), Seq())
    }).toSet.toSeq.sortWith(_.name < _.name)
    Node("KafkaCluster", clusterNodes)
  }

  override def reportOffsets(reporters: mutable.Set[OffsetInfoReporter]) {
    val offsetInfoSeq: IndexedSeq[OffsetInfo] = committedOffsetMap.flatMap { case (groupTopicPartition: GroupTopicPartition, offsetMetaData: OffsetAndMetadata) =>
      getOffsetInfoByPartition(groupTopicPartition, offsetMetaData)
    }.toIndexedSeq

    reporters.foreach(reporter => retryTask {
      reporter.report(offsetInfoSeq)
    })
  }

  override def getBrokerInfo(topics: Seq[String]): Iterable[BrokerInfo] = {
    for {
      partitions: util.List[PartitionInfo] <- topicPartitionsMap.filterKeys(topics.contains(_)).values
      partition: PartitionInfo <- partitions
    } yield BrokerInfo(id = partition.leader.id, host = partition.leader.host, port = partition.leader.port)
  }
}

object KafkaOffsetGetter extends Logging {
  val kafkaClientRequestTimeout: Duration = duration.pairIntToDuration(10, duration.SECONDS)

  //For these vars, we swap the whole object when update is needed. Should be thread safe.
  var committedOffsetMap: immutable.Map[GroupTopicPartition, OffsetAndMetadata] = immutable.HashMap()
  var topicAndGroups: immutable.Set[TopicAndGroup] = immutable.HashSet()
  var clients: immutable.Set[ClientGroup] = immutable.HashSet()
  var activeTopicPartitions: immutable.Set[TopicPartition] = immutable.HashSet()
  var topicPartitionOffsetsMap: immutable.Map[TopicPartition, Long] = immutable.HashMap()
  var topicPartitionsMap: immutable.Map[String, util.List[PartitionInfo]] = immutable.HashMap()

  // Retrieve ConsumerGroup, Consumer, Topic, and Partition information
  def startAdminClient(args: OffsetGetterArgs): Unit = {

    val sleepOnDataRetrieval: Int = 10000
    var adminClient: AdminClient = null

    while (true) {
      try {
        info("Retrieving consumer group overviews")

        if (null == adminClient) {
          adminClient = createNewAdminClient(args)
        }
        val groupOverviews: Seq[GroupOverview] = adminClient.listAllConsumerGroupsFlattened()

        val newTopicAndGroups: mutable.Set[TopicAndGroup] = mutable.HashSet()
        val newClients: mutable.Set[ClientGroup] = mutable.HashSet()
        val newActiveTopicPartitions: mutable.HashSet[TopicPartition] = mutable.HashSet()
        val newCommittedOffsetMap: mutable.Map[GroupTopicPartition, OffsetAndMetadata] = mutable.HashMap()

        groupOverviews.foreach((groupOverview: GroupOverview) => {
          val groupId = groupOverview.groupId
          try {
            val consumerGroupSummary = adminClient.describeConsumerGroup(groupId)

            consumerGroupSummary.consumers match {
              case None =>
              case Some(consumerGroupSummaryValue) =>
                consumerGroupSummaryValue.foreach(consumerSummary => {
                  val clientId = consumerSummary.clientId
                  val clientHost = consumerSummary.host

                  val topicPartitions: List[TopicPartition] = consumerSummary.assignment

                  topicPartitions.foreach(topicPartition => {
                    if (!topicPartition.topic.startsWith("_")) {
                      newActiveTopicPartitions += topicPartition
                      newTopicAndGroups += TopicAndGroup(topicPartition.topic(), groupId)
                    }
                  })

                  newClients += ClientGroup(groupId, clientId, clientHost, topicPartitions.toSet)
                })
            }
          } catch {
            case e: Throwable => warn(s"Failed to describe consumer group $groupId. ${e.getMessage}")
          }

          try {
            adminClient.listGroupOffsets(groupId).foreach { case (topicPartition, offset) =>
              newCommittedOffsetMap.put(GroupTopicPartition(groupId, topicPartition), OffsetAndMetadata(offset))
            }
          } catch {
            case e: Throwable => warn(s"Failed to list offsets for consumer group $groupId. ${e.getMessage}")
          }
        })

        activeTopicPartitions = newActiveTopicPartitions.toSet
        topicAndGroups = newTopicAndGroups.toSet
        clients = newClients.toSet
        committedOffsetMap = newCommittedOffsetMap.toMap

        info("Retrieved consumer group overviews: " + groupOverviews.size)
      }
      catch {
        case e: Throwable =>
          error("Error occurred while retrieving consumer groups.", e)
          if (null != adminClient) {
            adminClient.close()
            adminClient = null
          }
      }

      Thread.sleep(sleepOnDataRetrieval)
    }
  }

  def startTopicPartitionOffsetGetter(args: OffsetGetterArgs): Unit = {
    val sleepOnDataRetrieval: Int = 10000
    var topicPartitionOffsetGetter: KafkaConsumer[Array[Byte], Array[Byte]] = null

    while (true) {
      try {
        info("Retrieving topics/partition offsets")

        if (null == topicPartitionOffsetGetter) {
          val group: String = "KafkaOffsetMonitor-TopicPartOffsetGetter-" + System.currentTimeMillis()
          topicPartitionOffsetGetter = createNewKafkaConsumer(args, group)
        }

        info("Retrieving topics")
        val f = Future {
          topicPartitionsMap = JavaConversions.mapAsScalaMap(topicPartitionOffsetGetter.listTopics).toMap.filter(!_._1.startsWith("_"))
        }
        Await.result(f, kafkaClientRequestTimeout)
        info("Retrieved topics: " + topicPartitionsMap.size)

        if (activeTopicPartitions.nonEmpty) {
          info("Retrieving partition offsets for " + activeTopicPartitions.size + " partitions")

          val newTopicPartitionOffsetsMap: mutable.HashMap[TopicPartition, Long] = mutable.HashMap()

          activeTopicPartitions.foreach(topicPartition => {
            debug("Retrieving partition offsets for " + topicPartition.topic + " " + topicPartition.partition)
            val f = Future {
              val partitionOffset = topicPartitionOffsetGetter.endOffsets(List(topicPartition)).get(topicPartition)

              newTopicPartitionOffsetsMap.put(topicPartition, partitionOffset)
            }
            Await.result(f, kafkaClientRequestTimeout)
          })

          topicPartitionOffsetsMap = newTopicPartitionOffsetsMap.toMap

          info("Retrieved partition offsets: " + activeTopicPartitions.size)
        }
      }
      catch {
        case e: Throwable =>
          error("Error occurred while retrieving topic partition offsets.", e)
          if (null != topicPartitionOffsetGetter) {
            topicPartitionOffsetGetter.close()
            topicPartitionOffsetGetter = null
          }
      }

      Thread.sleep(sleepOnDataRetrieval)
    }
  }

  private def createNewKafkaConsumer(args: OffsetGetterArgs, group: String): KafkaConsumer[Array[Byte], Array[Byte]] = {
    val sleepAfterFailedKafkaConsumerConnect: Int = 20000
    var kafkaConsumer: KafkaConsumer[Array[Byte], Array[Byte]] = null
    val props: Properties = new Properties
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, args.kafkaBrokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, group)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, if (args.kafkaOffsetForceFromStart) "earliest" else "latest")

    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, args.kafkaSecurityProtocol)
    if (args.kafkaSecurityProtocol.equals("SSL")) {
      props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, args.kafkaSslKeystoreLocation)
      props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, args.kafkaSslKeystorePassword)
      props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, args.kafkaSslKeyPassword)
      props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, args.kafkaSslTruststoreLocation)
      props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, args.kafkaSslTruststorePassword)
    }

    while (null == kafkaConsumer) {
      try {
        info("Creating Kafka consumer " + group)
        kafkaConsumer = new KafkaConsumer[Array[Byte], Array[Byte]](props)
      }
      catch {
        case e: Throwable =>
          if (null != kafkaConsumer) {
            kafkaConsumer.close()
            kafkaConsumer = null
          }
          val errorMsg = "Error creating an Kafka conusmer.  Will attempt to re-create in %d seconds".format(sleepAfterFailedKafkaConsumerConnect)
          error(errorMsg, e)
          Thread.sleep(sleepAfterFailedKafkaConsumerConnect)
      }
    }

    info("Created Kafka consumer: " + group)
    kafkaConsumer
  }

  private def createNewAdminClient(args: OffsetGetterArgs): AdminClient = {
    val sleepAfterFailedAdminClientConnect: Int = 20000
    var adminClient: AdminClient = null

    val props: Properties = new Properties
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, args.kafkaBrokers)
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, args.kafkaSecurityProtocol)
    if (args.kafkaSecurityProtocol.equals("SSL")) {
      props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, args.kafkaSslKeystoreLocation)
      props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, args.kafkaSslKeystorePassword)
      props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, args.kafkaSslKeyPassword)
      props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, args.kafkaSslTruststoreLocation)
      props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, args.kafkaSslTruststorePassword)
    }

    while (null == adminClient) {
      try {
        info("Creating new Kafka AdminClient to get consumer and group info.")
        adminClient = AdminClient.create(props)
      }
      catch {
        case e: Throwable =>
          if (null != adminClient) {
            adminClient.close
            adminClient = null
          }
          val errorMsg = "Error creating an AdminClient.  Will attempt to re-create in %d seconds".format(sleepAfterFailedAdminClientConnect)
          error(errorMsg, e)
          Thread.sleep(sleepAfterFailedAdminClientConnect)
      }
    }

    info("Created admin client: " + adminClient)
    adminClient
  }

  case class TopicAndGroup(topic: String, group: String)

  case class ClientGroup(group: String, clientId: String, clientHost: String, topicPartitions: Set[TopicPartition])

}