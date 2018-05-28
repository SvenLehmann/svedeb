name := "svedeb"

version := "0.1"

scalaVersion := "2.12.5"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.5.12",
  "com.typesafe.akka" %% "akka-testkit" % "2.5.12" % Test,
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "org.scala-lang" % "scala-reflect" % "2.12.5"
)