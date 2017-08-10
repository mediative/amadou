import com.typesafe.sbt.packager.docker.Cmd

Jvm.`1.8`.required

inThisBuild(
  Def.settings(
    organization := "com.mediative",
    scalaVersion := "2.11.11",
    scalacOptions += "-Ywarn-unused-import",
    publishArtifact in (Compile, packageDoc) := false,
    publishArtifact in (Compile, packageSrc) := false,
    doctestTestFramework := DoctestTestFramework.ScalaTest,
    doctestWithDependencies := false,
    resolvers ++= Seq(
      Resolver.bintrayRepo("mediative", "maven"),
      Resolver.jcenterRepo
    ),
    envVars in Test ++= Map(
      "AMADOU_RETRY_MAX" -> "1",
      "SPARK_LOCAL_IP"   -> "localhost"
    ),
    envVars in IntegrationTest ++= (envVars in Test).value
  ))

lazy val Versions = new {
  val kafka            = "0.8.2.2"
  val spark            = "2.1.1"
  val sparkTestingBase = "2.1.0_0.6.0" // FIXME: when available s"${spark}_0.6.0"
  val prometheus       = "0.0.23"
  val hadoop           = "2.7.2"
}

lazy val amadou = project
  .in(file("."))
  .enablePlugins(MediativeGitHubPlugin, MediativeReleasePlugin)
  .aggregate(base, testkit, core, bigquery, runtime)
  .settings(
    noPublishSettings,
    postReleaseSteps ++= Seq(base, runtime).map(p => releaseStepTask(publish in Docker in p))
  )

val base = project
  .enablePlugins(MediativeBintrayPlugin, MediativeDockerPlugin)
  .settings(
    name := "amadou-base",
    dockerRepository := Some("mediative-docker-container-registry.bintray.io/amadou"),
    packageName := "base",
    libraryDependencies ++= Seq(
      "com.iheart"                 %% "ficus"               % "1.4.0",
      "com.lihaoyi"                %% "upickle"             % "0.3.6",
      "io.prometheus"              % "simpleclient_common"  % Versions.prometheus,
      "io.prometheus"              % "simpleclient_hotspot" % Versions.prometheus,
      "com.typesafe.scala-logging" %% "scala-logging"       % "3.1.0",
      "org.apache.spark"           %% "spark-core"          % Versions.spark,
      "org.apache.spark"           %% "spark-mllib"         % Versions.spark,
      "org.apache.spark"           %% "spark-sql"           % Versions.spark,
      "org.apache.spark"           %% "spark-hive"          % Versions.spark,
      "org.apache.hadoop"          % "hadoop-aws"           % Versions.hadoop
        exclude ("joda-time", "joda-time"),
      "joda-time"        % "joda-time"     % "2.9.7", // XXX: Use explicit joda-time dependency for AWS SDK
      "org.apache.kafka" % "kafka-clients" % Versions.kafka
        exclude ("org.slf4j", "slf4j-log4j12")
    )
  )

val testkit = project
  .enablePlugins(MediativeBintrayPlugin)
  .dependsOn(base)
  .settings(
    name := "amadou-testkit",
    libraryDependencies ++= Seq(
      "com.holdenkarau"   %% "spark-testing-base"          % Versions.sparkTestingBase,
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % Versions.hadoop force ()
    )
  )

val core = project
  .enablePlugins(MediativeBintrayPlugin)
  .dependsOn(base, testkit % Test)
  .settings(
    name := "amadou-core"
  )

val bigquery = project
  .enablePlugins(MediativeBintrayPlugin)
  .dependsOn(core, testkit % Test)
  .settings(
    name := "amadou-bigquery",
    libraryDependencies ++= Seq(
      "com.google.cloud.bigdataoss" % "bigquery-connector" % "0.7.8-hadoop2"
        exclude ("com.google.apis", "google-api-services-bigquery"),
      "com.google.apis"         % "google-api-services-bigquery" % "v2-rev320-1.22.0",
      "com.google.oauth-client" % "google-oauth-client-jetty"    % "1.20.0"
        exclude ("org.mortbay.jetty", "servlet-api")
    )
  )

val runtime = project
  .enablePlugins(MediativeBintrayPlugin, MediativeDockerPlugin)
  .dependsOn(core, bigquery)
  .settings(
    name := "amadou-runtime",
    dockerBaseImage := (dockerAlias in base).value.versioned,
    dockerRepository := Some("mediative-docker-container-registry.bintray.io/amadou"),
    packageName := "runtime"
  )
