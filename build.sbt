name := "svedeb"

version in ThisBuild := "0.1"
scalaVersion in ThisBuild := "2.11.8"

// Projects

lazy val global = project
  .in(file("."))
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
    mainClass in assembly := Some("de.hpi.svedeb.BenchmarkRunner"),
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    }
  ).dependsOn(database)

// Dependencies
lazy val dependencies =
  new {
    val akkaV       = "2.5.12"
    val scalatestV  = "3.0.5"
    val sparkV      = "2.3.1"

    val spark       = "org.apache.spark"  %%  "spark-sql"     % sparkV
    val akkaActor   = "com.typesafe.akka" %%  "akka-actor"    % akkaV
    val akkaRemote  = "com.typesafe.akka" %%  "akka-remote"   % akkaV

    val akkaTestkit = "com.typesafe.akka" %%  "akka-testkit"  % akkaV       % Test
    val scalatest   = "org.scalatest"     %%  "scalatest"     % scalatestV  % Test
  }

lazy val commonDependencies = Seq(
  dependencies.akkaActor,
  dependencies.akkaRemote,
  dependencies.akkaTestkit,
  dependencies.scalatest
)

// Settings
lazy val commonSettings = Seq(
  test in assembly := {}
)
