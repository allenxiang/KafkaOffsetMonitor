package com.quantifind.kafka.core

import java.nio.ByteBuffer
import java.util
import java.util.{Arrays, Properties}

import com.quantifind.kafka.OffsetGetter.{BrokerInfo, OffsetInfo}
import com.quantifind.kafka.offsetapp.{OffsetGetterArgs, OffsetInfoReporter}
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

import scala.collection.{JavaConversions, _}
import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


/**
  * Created by rcasey on 11/16/2016.
  */
class KafkaOffsetGetter extends OffsetGetter {

  import KafkaOffsetGetter._

  override def getOffsetInfoByTopic(group: String, topic: String): Seq[OffsetInfo] = {
    val partitionInfoList: util.List[PartitionInfo] = topicPartitionsMap.get(topic)
    if (null == partitionInfoList)
      Seq()
    else {
      val offsetInfoSeq = scala.collection.JavaConversions.asScalaIterator(topicPartitionsMap.get(topic).iterator()).map(partitionInfo => {
        val topicPartition = new TopicPartition(topic, partitionInfo.partition)
        val offsetMetaData = committedOffsetMap.get(GroupTopicPartition(group, topicPartition))

        offsetMetaData match {
          case None => None
          case Some(offsetMetaDataValue) =>
            getOffsetInfoByPartition(GroupTopicPartition(group, topicPartition), offsetMetaDataValue)
        }
      }).flatten.toSeq

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
    scala.collection.JavaConversions.asScalaIterator(topicPartitionsMap.keySet().iterator()).toSeq.sorted
  }

  override def getClusterViz: Node = {
    val clusterNodes = scala.collection.JavaConversions.mapAsScalaMap(topicPartitionsMap).values.map(partition => {
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
      partitions: util.List[PartitionInfo] <- JavaConversions.mapAsScalaMap(topicPartitionsMap).retain((k, v) => topics.contains(k)).values
      partition: PartitionInfo <- JavaConversions.iterableAsScalaIterable(partitions)
    } yield BrokerInfo(id = partition.leader.id, host = partition.leader.host, port = partition.leader.port)
  }
}

object KafkaOffsetGetter extends Logging {
  val committedOffsetMap: concurrent.Map[GroupTopicPartition, OffsetAndMetadata] = concurrent.TrieMap()

  //For these vars, we swap the whole object when update is needed. Should be thread safe.
  var topicAndGroups: mutable.Set[TopicAndGroup] = mutable.HashSet()
  var clients: mutable.Set[ClientGroup] = mutable.HashSet()
  var topicPartitionOffsetsMap: mutable.Map[TopicPartition, Long] = mutable.HashMap()
  var topicPartitionsMap: util.Map[String, util.List[PartitionInfo]] = new util.HashMap[String, util.List[PartitionInfo]]();

  def startCommittedOffsetListener(args: OffsetGetterArgs) = {
    var offsetConsumer: KafkaConsumer[Array[Byte], Array[Byte]] = null;

    if (null == offsetConsumer) {
      info("Creating new Kafka Client to get consumer group committed offsets")

      val group: String = "kafka-offset-getter-client-" + System.currentTimeMillis
      val consumerOffsetTopic = "__consumer_offsets"

      offsetConsumer = createNewKafkaConsumer(args, group)
      offsetConsumer.subscribe(Arrays.asList(consumerOffsetTopic))
    }

    Future {
      while (true) {
        debug("Pulling consumer group committed offsets")
        val records: ConsumerRecords[Array[Byte], Array[Byte]] = offsetConsumer.poll(1000)

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
                    logger.debug(s"Updating committed offset: g:$group,t:$topic,p:$partition: $offset")

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
  }

  // Retrieve ConsumerGroup, Consumer, Topic, and Partition information
  def startAdminClient(args: OffsetGetterArgs) = {

    val sleepOnDataRetrieval: Int = 10000
    var adminClient: AdminClient = null

    Future {
      while (true) {
        try {
          info("Retrieving consumer group overviews")

          if (null == adminClient) {
            adminClient = createNewAdminClient(args)
          }
          val groupOverviews = adminClient.listAllConsumerGroupsFlattened()

          val newTopicAndGroups: mutable.Set[TopicAndGroup] = mutable.HashSet()
          val newClients: mutable.Set[ClientGroup] = mutable.HashSet()

          groupOverviews.foreach((groupOverview: GroupOverview) => {
            val groupId = groupOverview.groupId;
            val consumerGroupSummary = adminClient.describeConsumerGroup(groupId)

            consumerGroupSummary match {
              case None =>
              case Some(consumerGroupSummaryValue) =>
                consumerGroupSummaryValue.foreach((consumerSummary) => {
                  val clientId = consumerSummary.clientId
                  val clientHost = consumerSummary.clientHost

                  val topicPartitions: List[TopicPartition] = consumerSummary.assignment

                  topicPartitions.map(topicPartition =>
                    newTopicAndGroups += TopicAndGroup(topicPartition.topic(), groupId)
                  )

                  newClients += ClientGroup(groupId, clientId, clientHost, topicPartitions.toSet)
                })
            }
          })

          topicAndGroups = newTopicAndGroups
          clients = newClients

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
  }

  def startTopicPartitionOffsetGetter(args: OffsetGetterArgs) = {
    val sleepOnDataRetrieval: Int = 10000
    var topicPartitionOffsetGetter: KafkaConsumer[Array[Byte], Array[Byte]] = null;

    Future {
      while (true) {
        try {
          info("Retrieving topics/partition offsets")

          if (null == topicPartitionOffsetGetter) {
            val group: String = "KafkaOffsetMonitor-TopicPartOffsetGetter-" + System.currentTimeMillis()
            topicPartitionOffsetGetter = createNewKafkaConsumer(args, group)
          }

          info("Retrieving topics")
          topicPartitionsMap = topicPartitionOffsetGetter.listTopics
          info("Retrieved topics: " + topicPartitionsMap.size )

          val distinctTopicPartitions: List[TopicPartition] = topicPartitionsMap.values().flatMap(partitionInfoList =>
            partitionInfoList.map( partInfo => new TopicPartition(partInfo.topic, partInfo.partition))).toList

          info("Retrieving partition offsets")
          topicPartitionOffsetsMap = topicPartitionOffsetGetter.endOffsets(distinctTopicPartitions).map(kv => {
            (kv._1, kv._2.toLong)
          })

          info("Retrieved partition offsets: " + topicPartitionOffsetsMap.size)
        }
        catch {
          case e: Throwable =>
            error("Error occurred while retrieving topic partition offsets.", e)
        }

        Thread.sleep(sleepOnDataRetrieval)
      }
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
    props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, args.kafkaSslKeystoreLocation)
    props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, args.kafkaSslKeystorePassword)
    props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, args.kafkaSslKeyPassword)
    props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, args.kafkaSslTruststoreLocation)
    props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, args.kafkaSslTruststorePassword)

    while (null == kafkaConsumer) {
      try {
        info("Creating Kafka consumer + " + group )
        kafkaConsumer= new KafkaConsumer[Array[Byte], Array[Byte]](props)
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

    info("Created Kafka consumer: " + group )
    kafkaConsumer
  }

  private def createNewAdminClient(args: OffsetGetterArgs): AdminClient = {
    val sleepAfterFailedAdminClientConnect: Int = 20000
    var adminClient: AdminClient = null

    val props: Properties = new Properties
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, args.kafkaBrokers)
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, args.kafkaSecurityProtocol)
    props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, args.kafkaSslKeystoreLocation)
    props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, args.kafkaSslKeystorePassword)
    props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, args.kafkaSslKeyPassword)
    props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, args.kafkaSslTruststoreLocation)
    props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, args.kafkaSslTruststorePassword)

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