import com.typesafe.sbt.packager.docker.Cmd

Jvm.`1.8`.required

inThisBuild(Def.settings(
  organization := "com.mediative",
  scalaVersion := "2.11.8",
  publishArtifact in (Compile, packageDoc) := false,
  publishArtifact in (Compile, packageSrc) := false,
  doctestTestFramework := DoctestTestFramework.ScalaTest,
  doctestWithDependencies := false,
  resolvers ++= Seq(
    Resolver.bintrayRepo("ypg-data", "maven"),
    Resolver.jcenterRepo
  )
))

lazy val Versions = new {
  val kafka = "0.8.2.2"
  val spark = "2.0.0"
  val prometheus = "0.0.19"
  val hadoop = "2.7.2"
}

lazy val amadou = project.in(file("."))
  .enablePlugins(MediativeGitHubPlugin, MediativeReleasePlugin)
  .aggregate(runtime, core, bigquery)
  .settings(noPublishSettings)

val runtime = project
  .enablePlugins(MediativeDockerPlugin)
  .settings(
    name := "amadou-runtime",
    dockerRepository := Some("ypg-data-docker-container-registry.bintray.io/amadou"),
    // XXX: Hack to make Snappy work with Alpine
    dockerCommands += Cmd("RUN", "ln /lib/ld-musl-x86_64.so.1 /lib/ld-linux-x86-64.so.2"),
    packageName in Docker := "runtime",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"  % Versions.spark,
      "org.apache.spark" %% "spark-mllib" % Versions.spark,
      "org.apache.spark" %% "spark-sql"   % Versions.spark,
      "org.apache.spark" %% "spark-hive"  % Versions.spark,
      "org.apache.hadoop" % "hadoop-aws"  % Versions.hadoop
        exclude("joda-time", "joda-time"),
      "joda-time" % "joda-time" % "2.9.7", // XXX: Use explicit joda-time dependency for AWS SDK
      "org.apache.kafka" % "kafka-clients" % Versions.kafka
        exclude("org.slf4j", "slf4j-log4j12"),
      "com.holdenkarau" %% "spark-testing-base" % s"${Versions.spark}_0.4.7" % Test,
      "org.apache.hadoop" % "hadoop-mapreduce-client-core"  % Versions.hadoop % Test force()
    )
  )

// This project should be the only one to depend on runtime. It
// marks the runtime dependencies as "provided" so downstream
// projects won't include them in the final docker image.
val core = project
  .enablePlugins(MediativeBintrayPlugin)
  .dependsOn(runtime % "test->test;provided->compile")
  .settings(
    name := "amadou-core",
    libraryDependencies ++= Seq(
      "com.typesafe" % "config" % "1.3.0",
      "com.iheart" %% "ficus" % "1.2.6",
      "com.lihaoyi" %% "upickle" % "0.3.6",
      "io.prometheus" % "simpleclient_common" % Versions.prometheus,
      "io.prometheus" % "simpleclient_hotspot" % Versions.prometheus,
      "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"
    )
  )

val bigquery = project
  .enablePlugins(MediativeBintrayPlugin)
  .dependsOn(core % "test->test;compile->compile;provided->provided")
  .settings(
    name := "amadou-bigquery",
    libraryDependencies ++= Seq(
      "com.google.cloud.bigdataoss" % "bigquery-connector" % "0.7.8-hadoop2"
        exclude("com.google.apis", "google-api-services-bigquery"),
      "com.google.apis" % "google-api-services-bigquery" % "v2-rev320-1.22.0",
      "com.google.oauth-client" % "google-oauth-client-jetty" % "1.20.0"
        exclude("org.mortbay.jetty", "servlet-api")
    )
  )
