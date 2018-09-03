package de.hpi.svedeb

import akka.pattern.ask
import de.hpi.svedeb.ClusterNode.{ClusterIsUp, FetchAPI, FetchedAPI, IsClusterUp}
import de.hpi.svedeb.api.API.Shutdown

import scala.concurrent.Await
import scala.concurrent.duration._

object BenchmarkRunner extends App {

  val joinBenchmarks = List(
    HashJoinBenchmark,
    NestedLoopJoinBenchmark,
    NonActorNestedLoopJoin,
    NonActorHashJoin
//    NonActorSparkBasedJoin
  )

  val scanBenchmarks = List(
    ScanBenchmark,
    NonActorScan
  )

  val throughputBenchmark = List(
    new ThroughputBenchmark(numberOfQueries = 1000)
  )

  val numberOfIterations = 5

  private def time[R](block: => R): Long = {
    val t0 = System.nanoTime()
    block    // call-by-name
    val t1 = System.nanoTime()
    t1 -t0
  }

  private def nanosecondsToMilliseconds(time: Long): Double = time/1000000.0

  def runBenchmark(benchmark: AbstractBenchmark, tableSize: Int): Unit = {
    // Hacky way to wait for cluster start
    while (!Await.result(clusterNode.ask(IsClusterUp()) (5 seconds), 5 seconds).asInstanceOf[ClusterIsUp].bool) {}

    val apiFuture = clusterNode.ask(FetchAPI()) (5 seconds)
    import scala.concurrent.Await
    val api = Await.result(apiFuture, 5 seconds).asInstanceOf[FetchedAPI].api

    try {
      val times = (1 to numberOfIterations).map(_ => {
        // Reinitialize in every iteration to avoid caching effects
        benchmark.setup(api, tableSize)
        val executionTime = time(benchmark.runBenchmark(api))
        benchmark.tearDown(api)
        executionTime
      })
      val avg = times.sum / times.size
      val median = times.sorted.apply(times.size/2)

      println(s"${benchmark.name} \t $tableSize \t ${nanosecondsToMilliseconds(avg)} \t ${nanosecondsToMilliseconds(median)}")
    } finally {
//      api ! Shutdown()
    }
  }

  val clusterNode = ClusterNode.start()

  try {


    println("JoinBenchmarks")
    println(s"Benchmark \t TableSize \t Average in ms \t Median in ms")
    for {
      benchmark <- joinBenchmarks
      tableSize <- Seq(
        100, 200, 300, 400, 500, 600, 700, 800, 900,
              1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000,
              10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000,
              100000//, 200000, 300000, 400000, 500000, 600000, 700000, 800000, 900000,
        //1000000, 2000000, 3000000, 4000000, 5000000, 6000000, 7000000, 8000000, 9000000, 10000000
      )
    } yield runBenchmark(benchmark, tableSize)

    println("ScanBenchmarks")
    println(s"Benchmark \t TableSize \t Average in ms \t Median in ms")
//    for {
//      benchmark <- scanBenchmarks
//      tableSize <- Seq(
//        100, 200, 300, 400, 500, 600, 700, 800, 900
////              1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000,
////              10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000,
//        //      100000, 200000, 300000, 400000, 500000, 600000, 700000, 800000, 900000,
//        //      1000000, 2000000, 3000000, 4000000, 5000000, 6000000, 7000000, 8000000, 9000000,
//        //      10000000//, 20000000, 30000000, 40000000, 50000000, 60000000, 70000000, 80000000, 90000000, 100000000
//      )
//    } yield runBenchmark(benchmark, tableSize)

    println("ThroughputBenchmarks")
    println(s"Benchmark \t TableSize \t Average in ms \t Median in ms")
    //  for {
    //    benchmark <- throughputBenchmark
    //    tableSize <- Seq(
    //      100, 200, 300, 400, 500, 600, 700, 800, 900,
    //      1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000,
    //      10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000,
    //      100000 //, 200000, 300000, 400000, 500000, 600000, 700000, 800000, 900000,
    //      //1000000, 2000000, 3000000, 4000000, 5000000, 6000000, 7000000, 8000000, 9000000, 10000000
    //    )
    //  } yield runBenchmark(benchmark, tableSize)

  } finally {
    val apiFuture = clusterNode.ask(FetchAPI()) (5 seconds)
    import scala.concurrent.Await
    val api = Await.result(apiFuture, 5 seconds).asInstanceOf[FetchedAPI].api
    api ! Shutdown()
  }




}
