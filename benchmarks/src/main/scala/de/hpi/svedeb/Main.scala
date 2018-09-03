package de.hpi.svedeb

import akka.pattern.ask
import de.hpi.svedeb.ClusterNode.{ClusterIsUp, IsClusterUp}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

object Main extends App {

  println("Starting SvedeB with following arguments")
  println(args.toList)

  if (args.length == 0) {
    println("Starting ClusterNode")
    val clusterNode = ClusterNode.start()
    while (!Await.result(clusterNode.ask(IsClusterUp()) (5 seconds), 5 seconds).asInstanceOf[ClusterIsUp].bool){}

    println("Cluster is up")
  } else {
    args(0) match {
      case "benchmark" =>
        println("Starting Benchmark")
        val benchmarkRunner = new BenchmarkRunner()
        benchmarkRunner.run()
      case _ =>
        println("Undefined arg, can only understand 'benchmark'")
    }
  }
}
