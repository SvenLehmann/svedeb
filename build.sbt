name := "svedeb"

version in ThisBuild := "0.1"
scalaVersion in ThisBuild := "2.11.8"

// Projects

lazy val global = project
  .in(file("."))
  .settings(settings)
  .aggregate(
    benchmarks,
    database
  )

lazy val database = project
  .settings(
    name := "database",
    settings,
    libraryDependencies ++= commonDependencies
  )

lazy val benchmarks = project
  .settings(
    name := "benchmarks",
    settings,
    libraryDependencies ++= (commonDependencies :+ dependencies.spark)
  ).dependsOn(database)

// Dependencies
lazy val dependencies =
  new {
    val akkaV       = "2.5.12"
    val scalatestV  = "3.0.5"
    val sparkV      = "2.3.1"

    val spark       = "org.apache.spark"  %%  "spark-sql"     % sparkV
    val akkaActor   = "com.typesafe.akka" %%  "akka-actor"    % akkaV
    val akkaTestkit = "com.typesafe.akka" %%  "akka-testkit"  % akkaV       % Test
    val scalatest   = "org.scalatest"     %%  "scalatest"     % scalatestV  % Test
  }

lazy val commonDependencies = Seq(
  dependencies.akkaActor,
  dependencies.akkaTestkit,
  dependencies.scalatest
)

// Settings
lazy val settings = Seq()
