import com.typesafe.sbt.SbtMultiJvm.multiJvmSettings

name := "svedeb"

version in ThisBuild := "0.1"
scalaVersion in ThisBuild := "2.11.8"

// Projects

lazy val global = project
  .in(file("."))
  .enablePlugins(MultiJvmPlugin) // use the plugin
  .configs(MultiJvm) // load the multi-jvm configuration
  .settings(multiJvmSettings: _*) // apply the default settings
  .settings(commonSettings)
  .aggregate(
    benchmarks,
    database
  )

lazy val database = project
  .settings(
    name := "database",
    commonSettings,
    libraryDependencies ++= commonDependencies
  )

lazy val benchmarks = project
  .settings(
    name := "benchmarks",
    commonSettings,
    libraryDependencies ++= (commonDependencies :+ dependencies.spark),
    mainClass in assembly := Some("de.hpi.svedeb.Main"),
    assemblyJarName in assembly := "SvedeB-Fat.jar",
    test in assembly := {},
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat
      case x => MergeStrategy.first
    }
  ).dependsOn(database)

// Dependencies
lazy val dependencies =
  new {
    val akkaV       = "2.5.16"
    val scalatestV  = "3.0.5"
    val sparkV      = "2.3.1"
    val kryoV       = "0.5.1"
    val chillAkkaV   = "0.9.3"

    val kryo        = "com.github.romix.akka"     %%  "akka-kryo-serialization" % kryoV
    val chillAkka  = "com.twitter"       %%  "chill-akka"    % chillAkkaV

    val spark       = "org.apache.spark"  %%  "spark-sql"     % sparkV
    val akkaActor   = "com.typesafe.akka" %%  "akka-actor"    % akkaV
    val akkaCluster = "com.typesafe.akka" %%  "akka-cluster"  % akkaV
    val akkaClusterMetrics =  "com.typesafe.akka" %%  "akka-cluster-metrics"  % akkaV
    val akkaClusterTools =    "com.typesafe.akka" %%  "akka-cluster-tools"    % akkaV
    val akkaMultiNodeTestkit =  "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaV
    val akkaRemote  = "com.typesafe.akka" %%  "akka-remote"   % akkaV

    val akkaTestkit = "com.typesafe.akka" %%  "akka-testkit"  % akkaV       % Test
    val scalatest   = "org.scalatest"     %%  "scalatest"     % scalatestV  % Test
  }

lazy val commonDependencies = Seq(
  dependencies.akkaActor,
  dependencies.akkaCluster,
  dependencies.akkaClusterMetrics,
  dependencies.akkaClusterTools,
  dependencies.akkaMultiNodeTestkit,
  dependencies.akkaRemote,
  dependencies.akkaTestkit,
  dependencies.chillAkka,
  dependencies.scalatest
)

parallelExecution in ThisBuild := false

// Settings
lazy val commonSettings = Seq(
  parallelExecution in Test := false, // do not run test cases in parallel
  test in assembly := {}
)