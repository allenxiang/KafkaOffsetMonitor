name := "KafkaOffsetMonitor"
version := "0.4.0-20170207"
scalaVersion := "2.11.8"
organization := "com.quantifind"

scalacOptions ++= Seq("-deprecation", "-unchecked", "-optimize", "-feature")

mainClass in Compile := Some("com.quantifind.kafka.offsetapp.OffsetGetterWeb")

packAutoSettings

libraryDependencies ++= Seq(
  "log4j" % "log4j" % "1.2.17",
  "net.databinder" %% "unfiltered-filter" % "0.8.4",
  "net.databinder" %% "unfiltered-jetty" % "0.8.4",
  "net.databinder" %% "unfiltered-json4s" % "0.8.4",
  "com.quantifind" %% "sumac" % "0.3.0",
  "org.apache.kafka" %% "kafka" % "0.10.1.1",
  "org.reflections" % "reflections" % "0.9.10",
  "com.twitter" % "util-core_2.11" % "6.40.0",
  "com.typesafe.slick" %% "slick" % "2.1.0",
  "org.xerial" % "sqlite-jdbc" % "3.7.2",
  "org.mockito" % "mockito-all" % "1.10.19" % "test",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "io.prometheus" % "simpleclient_dropwizard" % "0.0.20",
  "io.prometheus" % "simpleclient_hotspot" % "0.0.20",
  "io.prometheus" % "simpleclient_common" % "0.0.20"
)
