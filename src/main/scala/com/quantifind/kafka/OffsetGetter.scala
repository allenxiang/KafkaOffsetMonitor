package com.quantifind.kafka

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

import com.quantifind.kafka.OffsetGetter.{BrokerInfo, KafkaInfo, OffsetInfo}
import com.quantifind.kafka.core.KafkaOffsetGetter
import com.quantifind.kafka.offsetapp.{OffsetGetterArgs, OffsetInfoReporter}
import com.twitter.util.Time
import kafka.utils.Logging

import scala.collection._
import scala.concurrent.{ExecutionContext, Future}

case class Node(name: String, children: Seq[Node] = Seq())

case class TopicDetails(consumers: Seq[ConsumerDetail])

case class TopicDetailsWrapper(consumers: TopicDetails)

case class TopicAndConsumersDetails(active: Seq[KafkaInfo], inactive: Seq[KafkaInfo])

case class TopicAndConsumersDetailsWrapper(consumers: TopicAndConsumersDetails)

case class ConsumerDetail(name: String)

trait OffsetGetter extends Logging {

  //  kind of interface methods
  def getTopicList(group: String): List[String]

  def getGroups: Seq[String]

  def getTopicMap: Map[String, Seq[String]]

  def getActiveTopicMap: Map[String, Seq[String]]

  // get list of all topics
  def getTopics: Seq[String]

  def getClusterViz: Node

  def reportOffsets(reporters: mutable.Set[OffsetInfoReporter])

  def getOffsetInfoByTopic(group: String, topic: String): Seq[OffsetInfo]

  def getBrokerInfo(topics: Seq[String]): Iterable[BrokerInfo]

  /**
    * Returns details for a given topic such as the consumers pulling off of it
    */
  def getTopicDetail(topic: String): TopicDetails = {
    val topicMap = getActiveTopicMap

    if (topicMap.contains(topic)) {
      TopicDetails(topicMap(topic).map(consumer => {
        ConsumerDetail(consumer.toString)
      }))
    } else {
      TopicDetails(Seq(ConsumerDetail("Unable to find Active Consumers")))
    }
  }

  def mapConsumerDetails(consumers: Seq[String]): Seq[ConsumerDetail] =
    consumers.map(consumer => ConsumerDetail(consumer.toString))

  /**
    * Returns details for a given topic such as the active consumers pulling off of it
    * and for each of the active consumers it will return the consumer data
    */
  def getTopicAndConsumersDetail(topic: String): TopicAndConsumersDetailsWrapper = {
    val topicMap = getTopicMap
    val activeTopicMap = getActiveTopicMap

    val activeConsumers = if (activeTopicMap.contains(topic)) {
      mapConsumersToKafkaInfo(activeTopicMap(topic), topic)
    } else {
      Seq()
    }

    val inactiveConsumers = if (!activeTopicMap.contains(topic) && topicMap.contains(topic)) {
      mapConsumersToKafkaInfo(topicMap(topic), topic)
    } else if (activeTopicMap.contains(topic) && topicMap.contains(topic)) {
      mapConsumersToKafkaInfo(topicMap(topic).diff(activeTopicMap(topic)), topic)
    } else {
      Seq()
    }

    TopicAndConsumersDetailsWrapper(TopicAndConsumersDetails(activeConsumers, inactiveConsumers))
  }

  def mapConsumersToKafkaInfo(consumers: Seq[String], topic: String): Seq[KafkaInfo] = {
    info("mapConsumersToKafkaInfo" + topic)
    consumers.map(getInfo(_, Seq(topic)))
  }

  // get information about a consumer group and the topics it consumes
  def getInfo(group: String, topics: Seq[String] = Seq()): KafkaInfo = {
    val topicList = if (topics.isEmpty) {
      getTopicList(group)
    } else {
      topics
    }

    val off = topicList.sorted.flatMap(getOffsetInfoByTopic(group, _))
    val brok = getBrokerInfo(topicList).toList.distinct

    KafkaInfo(
      name = group,
      brokers = brok,
      offsets = off
    )
  }

  def getActiveTopics: Node = {
    val topicMap = getActiveTopicMap

    Node("ActiveTopics", topicMap.map {
      case (s: String, ss: Seq[String]) => {
        Node(s, ss.map(consumer => Node(consumer)))

      }
    }.toSeq)
  }

  def close(): Unit = {
    // Anything to clean up?
  }
}

object OffsetGetter {

  val kafkaOffsetGetter = new KafkaOffsetGetter

  def getInstance(args: OffsetGetterArgs): OffsetGetter = {
    kafkaOffsetGetter
  }

  def startGetters(args: OffsetGetterArgs) ={
    val executor1 = Executors.newSingleThreadExecutor()
    executor1.submit(new Runnable() {
      def run() = KafkaOffsetGetter.startAdminClient(args)
    })

    val executor2 = Executors.newSingleThreadExecutor()
    executor2.submit(new Runnable() {
      def run() = KafkaOffsetGetter.startTopicPartitionOffsetGetter(args)
    })

    val executor3 = Executors.newSingleThreadExecutor()
    executor3.submit(new Runnable() {
      def run() = KafkaOffsetGetter.startCommittedOffsetListener(args)
    })
  }

  case class KafkaInfo(name: String, brokers: Seq[BrokerInfo], offsets: Seq[OffsetInfo])

  case class BrokerInfo(id: Int, host: String, port: Int)

  case class OffsetInfo(group: String,
                        topic: String,
                        partition: Int,
                        offset: Long,
                        logSize: Long,
                        owner: Option[String],
                        creation: Time,
                        modified: Time) {
    val lag = logSize - offset
  }
}
