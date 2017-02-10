Kafka Offset Monitor
===========
This is a heavily modified version of Kafka Offset Monitor forked from [here](https://github.com/quantifind/KafkaOffsetMonitor)

This is an app to monitor your kafka consumers and their position (offset) in the queue.

You can see the current consumer groups, for each group the topics that they are consuming and the position of the group in each topic queue. This is useful to understand how quick you are consuming from a queue and how fast the queue is growing. It allows for debuging kafka producers and consumers or just to have an idea of what is going on in  your system.

The app keeps an history of queue position and lag of the consumers so you can have an overview of what has happened in the last days.

Here are a few screenshots:

List of Consumer Groups
-----------------------

![Consumer Groups](http://quantifind.github.io/KafkaOffsetMonitor/img/groups.png)

List of Topics for a Group
--------------------------

![Topic List](http://quantifind.github.io/KafkaOffsetMonitor/img/topics.png)

History of Topic position
-------------------------

![Position Graph](http://quantifind.github.io/KafkaOffsetMonitor/img/graph.png)

Offset Types
===========

Kafka is flexible on how the offsets are managed. Consumer can choose arbitrary storage and format to persist offsets. This forked version of KafkaOffsetMonitor currently 
only supports kafka built-in offset management API (based on Kafka internal topic).

Secure Kafka Support
===========

This forked version requires SSL connections.

Running It
===========

This is a small webapp, you can run it locally or on a server, as long as you have access to the kafka nodes.

```
java -cp KafkaOffsetMonitor-assembly-0.2.1.jar \
     com.quantifind.kafka.offsetapp.OffsetGetterWeb \
     --kafkaBrokers localhost:9093 \
     --kafkaSslPropertyFile client.config \
     --port 8080 \
     --refresh 10.seconds \
     --retain 2.days
```

The arguments are:

- **port** on what port will the app be available
- **refresh** how often should the app refresh and store a point in the DB
- **retain** how long should points be kept in the DB
- **dbName** where to store the history (default 'offsetapp')
- **kafkaOffsetForceFromStart** only applies to ''kafka'' format. Force KafkaOffsetMonitor to scan the commit messages from start (see notes below)
- **pluginsArgs** additional arguments used by extensions (see below)

Special Notes on Kafka Format
===============================
With Kafka built-in offset management API, offsets are saved in an internal topic ''__consumer_offsets'' as ''commit'' messages. Because there is no place 
to directly query existing consumers, KafkaOffsetMonitor needs to ''discover'' consumers by examining those ''commit'' messages.  If consumers are active, 
KafkaOffsetMonitor could just listen to new ''commit'' messages and active consumers should be ''discovered'' after a short while.  If in case you want to 
see the consumers without much load, you can use flag '''kafkaOffsetForceFromStart''' to scan all ''commit'' messages.

Writing and using plugins
============================

Kafka Offset Monitor allows you to plug-in additional offset info reporters in case you want this information to be logged or stored somewhere. In order to write your own plugin,
all you need to do is to implement OffsetInfoReporter trait:

```
trait OffsetInfoReporter {
  def report(info: IndexedSeq[OffsetInfo])
  def cleanupOldData() = {}
}
```

It is also required, that implementation has a constructor with String as the only parameter, and this parameter will be set to pluginsArgs argument value.
Its up to you how you want to utilize this argument and configure your plugin.

When building a plugin you may find it difficult to set up dependency to Kafka Offset Monitor classes, as currently artifacts are not published to public repos.
As long as this is true you will need to use local maven repo and just publish Kafka Offset Monitor artifact with: ```sbt publishM2```

Assuming you have a custom implementation of OffsetInfoReporter in a jar file, running it is as simple as adding the jar to the classpath when running app:

```
java -cp KafkaOffsetMonitor-assembly-0.3.0.jar:kafka-offset-monitor-another-db-reporter.jar \
     com.quantifind.kafka.offsetapp.OffsetGetterWeb \
     --kafkaBrokers localhost:9093 \
     --kafkaSslPropertyFile client.config \
     --port 8080 \
     --refresh 10.seconds \
     --retain 2.days
     --pluginsArgs anotherDbHost=host1,anotherDbPort=555
```

For complete working example you can check [kafka-offset-monitor-graphite](https://github.com/allegro/kafka-offset-monitor-graphite), a plugin reporting offset information to Graphite.
