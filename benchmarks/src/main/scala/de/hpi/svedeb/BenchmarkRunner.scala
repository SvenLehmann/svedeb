package de.hpi.svedeb

import de.hpi.svedeb.api.API.Shutdown

object BenchmarkRunner extends App {

  val benchmarks = List(
    JoinBenchmark,
    NonActorNestedLoopJoin,
    NonActorHashJoin,
    NonActorSparkBasedJoin,
    ScanBenchmark,
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
    val api = SvedeB.start()

    try {
      benchmark.setup(api, tableSize)

      val times = (1 to numberOfIterations).map(_ => time(benchmark.runBenchmark(api)))
      val avg = times.sum / times.size
      val median = times.sorted.apply(times.size/2)

      println(s"${benchmark.name} \t $tableSize \t ${nanosecondsToMilliseconds(avg)} \t ${nanosecondsToMilliseconds(median)}")
      benchmark.tearDown(api)
    } finally {
      api ! Shutdown()
    }
  }

  println(s"Benchmark \t TableSize \t Average in ms \t Median in ms")
  for {
    benchmark <- benchmarks
    tableSize <- Seq(
      100, 200, 300, 400, 500, 600, 700, 800, 900,
      1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000,
      10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000,
      100000//, 200000, 300000, 400000, 500000, 600000, 700000, 800000, 900000,
      //1000000, 2000000, 3000000, 4000000, 5000000, 6000000, 7000000, 8000000, 9000000, 10000000
     )
  } yield runBenchmark(benchmark, tableSize)
}
