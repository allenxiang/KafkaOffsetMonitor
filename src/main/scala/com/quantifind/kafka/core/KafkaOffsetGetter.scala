package com.quantifind.kafka.core

import java.nio.ByteBuffer
import java.util
import java.util.{Arrays, Properties}

import com.quantifind.kafka.OffsetGetter.{BrokerInfo, OffsetInfo}
import com.quantifind.kafka.offsetapp.{OffsetGetterArgs, OffsetGetterWeb, OffsetInfoReporter}
import com.quantifind.kafka.{Node, OffsetGetter}
import com.quantifind.utils.Utils.retryTask
import com.twitter.util.Time
import kafka.admin.AdminClient
import kafka.common.OffsetAndMetadata
import kafka.coordinator._
import kafka.utils.Logging
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.{PartitionInfo, TopicPartition}

import scala.collection.JavaConversions._
import scala.collection.{JavaConversions, _}
import scala.concurrent.ExecutionContext.Implicits.global
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
        val offsetInfoSeq = partitionInfoListValue.map(partitionInfo => {
          val topicPartition = new TopicPartition(topic, partitionInfo.partition)
          val offsetMetaData = committedOffsetMap.get(GroupTopicPartition(group, topicPartition))

          offsetMetaData match {
            case None => None
            case Some(offsetMetaDataValue) =>
              getOffsetInfoByPartition(GroupTopicPartition(group, topicPartition), offsetMetaDataValue)
          }
        }).flatten

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
        val filteredClients = clients.filter(c => (c.group == groupTopicPartition.group && c.topicPartitions.contains(groupTopicPartition.topicPartition)))
        if (!filteredClients.isEmpty) {
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
    val offsetInfoSeq: IndexedSeq[OffsetInfo] = committedOffsetMap.map { case (groupTopicPartition: GroupTopicPartition, offsetMetaData: OffsetAndMetadata) =>
      getOffsetInfoByPartition(groupTopicPartition, offsetMetaData)
    }.flatten.toIndexedSeq

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
  val kafkaClientRequestTimeout = duration.pairIntToDuration(10, duration.SECONDS)
  val committedOffsetMap: concurrent.Map[GroupTopicPartition, OffsetAndMetadata] = concurrent.TrieMap()

  //For these vars, we swap the whole object when update is needed. Should be thread safe.
  var topicAndGroups: immutable.Set[TopicAndGroup] = immutable.HashSet()
  var clients: immutable.Set[ClientGroup] = immutable.HashSet()
  var activeTopicPartitions: immutable.Set[TopicPartition] = immutable.HashSet()
  var topicPartitionOffsetsMap: immutable.Map[TopicPartition, Long] = immutable.HashMap()
  var topicPartitionsMap: immutable.Map[String, util.List[PartitionInfo]] = immutable.HashMap()

  def startCommittedOffsetListener(args: OffsetGetterArgs) = {
    var offsetConsumer: KafkaConsumer[Array[Byte], Array[Byte]] = null;
    val consumerOffsetTopic = "__consumer_offsets"

    while (null == offsetConsumer) {
      info("Creating new Kafka Client to get consumer group committed offsets")

      val group: String = "kafka-offset-getter-client-" + System.currentTimeMillis
      offsetConsumer = createNewKafkaConsumer(args, group)

      try {
        val f = Future {
          offsetConsumer.subscribe(Arrays.asList(consumerOffsetTopic))
        }
        Await.result(f, kafkaClientRequestTimeout)

        info("Subscribed to " + consumerOffsetTopic)
      }
      catch {
        case e: Throwable =>
          error("Error subscribing to consumer groups.", e)
          offsetConsumer.close
          offsetConsumer = null
      }
    }

    while (true) {
      debug("Pulling consumer group committed offsets")
      val records: ConsumerRecords[Array[Byte], Array[Byte]] = offsetConsumer.poll(100)

      debug("Consumer group committed offsets pulled: " + records.count)
      if (0 != records.count) {
        val iter = records.iterator()
        while (iter.hasNext()) {
          val record: ConsumerRecord[Array[Byte], Array[Byte]] = iter.next()
          val baseKey: BaseKey = GroupMetadataManager.readMessageKey(ByteBuffer.wrap(record.key))
          baseKey match {
            case b: OffsetKey =>
              try {
                val offsetAndMetadata: OffsetAndMetadata = GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(record.value))
                val gtp: GroupTopicPartition = b.key

                val existingCommittedOffsetMap: Option[OffsetAndMetadata] = committedOffsetMap.get(gtp)

                // Update only if the new message brings a change in offset
                if (!existingCommittedOffsetMap.isDefined || existingCommittedOffsetMap.get.offset != offsetAndMetadata.offset) {
                  val group: String = gtp.group
                  val topic: String = gtp.topicPartition.topic
                  val partition: Long = gtp.topicPartition.partition
                  val offset: Long = offsetAndMetadata.offset
                  debug(s"Updating committed offset: g:$group,t:$topic,p:$partition: $offset")
                  OffsetGetterWeb.consumer_offset.labels(topic, partition.toString, group).set(offset)

                  committedOffsetMap += (gtp -> offsetAndMetadata)
                }
              }
              catch {
                case e: Throwable =>
                  error("Error occurred while reading consumer offset.", e)
              }

            case _ => // do nothing with these other messages
          }
        }
      }
    }
  }

  // Retrieve ConsumerGroup, Consumer, Topic, and Partition information
  def startAdminClient(args: OffsetGetterArgs) = {

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

        groupOverviews.foreach((groupOverview: GroupOverview) => {
          val groupId = groupOverview.groupId;
          val consumerGroupSummary = adminClient.describeConsumerGroup(groupId)

          consumerGroupSummary match {
            case None =>
            case Some(consumerGroupSummaryValue) =>
              consumerGroupSummaryValue.foreach(consumerSummary => {
                val clientId = consumerSummary.clientId
                val clientHost = consumerSummary.clientHost

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
        })

        activeTopicPartitions = newActiveTopicPartitions.toSet
        topicAndGroups = newTopicAndGroups.toSet
        clients = newClients.toSet

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

  def startTopicPartitionOffsetGetter(args: OffsetGetterArgs) = {
    val sleepOnDataRetrieval: Int = 10000
    var topicPartitionOffsetGetter: KafkaConsumer[Array[Byte], Array[Byte]] = null;

    while (true) {
      try {
        info("Retrieving topics/partition offsets")

        if (null == topicPartitionOffsetGetter) {
          val group: String = "KafkaOffsetMonitor-TopicPartOffsetGetter-" + System.currentTimeMillis()
          topicPartitionOffsetGetter = createNewKafkaConsumer(args, group)
        }

        info("Retrieving topics")
        topicPartitionsMap = JavaConversions.mapAsScalaMap(topicPartitionOffsetGetter.listTopics).toMap.filter(!_._1.startsWith("_"))
        info("Retrieved topics: " + topicPartitionsMap.size)

        if (activeTopicPartitions.nonEmpty) {
          info("Retrieving partition offsets for " + activeTopicPartitions.size + " partitions")

          info("Assigning topicPartitionOffsetGetter to " + activeTopicPartitions.size + " partitions")
          val f = Future {
            topicPartitionOffsetGetter.assign(activeTopicPartitions)
          }
          Await.result(f, kafkaClientRequestTimeout)
          info("Seeking to the end of  " + activeTopicPartitions.size + " partitions")
          topicPartitionOffsetGetter.seekToEnd(new util.ArrayList[TopicPartition]())

          val newTopicPartitionOffsetsMap: mutable.HashMap[TopicPartition, Long] = mutable.HashMap()

          activeTopicPartitions.foreach(topicPartition => {
            debug("Retrieving partition offsets for " + topicPartition.topic + " " + topicPartition.partition)
            val f = Future {
              val parititionOffset = topicPartitionOffsetGetter.position(topicPartition)
              OffsetGetterWeb.topic_partition_offset.labels(topicPartition.topic, topicPartition.partition.toString).set(parititionOffset)

              newTopicPartitionOffsetsMap.put(topicPartition, parititionOffset)
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
            kafkaConsumer.close
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
        info("Creating new Kafka AdminClient to get consumer and group info.");
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