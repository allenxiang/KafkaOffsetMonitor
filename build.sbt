name := "kafka-offset-monitor"
scalaVersion := "2.11.11"
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
  "org.apache.kafka" %% "kafka" % "0.11.0.0",
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

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

publishPackArchiveTgz

val nexusBaseUrl = System.getenv("nexusBaseUrl")
val snapshotRepo = System.getenv("snapshotRepo")
val releaseRepo = System.getenv("releaseRepo")

resolvers ++= Seq(
  Resolver.jcenterRepo,
  "Sonatype Nexus Repository Manager" at nexusBaseUrl + releaseRepo
)


publishTo := {
  if (isSnapshot.value)
    Some("Snapshot Repo" at nexusBaseUrl + snapshotRepo)
  else
    Some("Release Repo" at nexusBaseUrl + releaseRepo)
}


