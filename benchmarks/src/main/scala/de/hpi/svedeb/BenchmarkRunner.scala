package de.hpi.svedeb

import de.hpi.svedeb.api.API.Shutdown

object BenchmarkRunner extends App {

  val benchmarks = List(JoinBenchmark, ScanBenchmark)
  val numberOfIterations = 3

  private def time[R](block: => R): Long = {
    val t0 = System.nanoTime()
    block    // call-by-name
    val t1 = System.nanoTime()
    t1 -t0
  }

  private def nanosecondsToMilliseconds(time: Long): Double = time/1000000.0

  def runBenchmark(benchmark: AbstractBenchmark): Unit = {
    val api = SvedeB.start()

    try {
      benchmark.setup(api)

      val times = (1 to numberOfIterations).map(_ => time(benchmark.runBenchmark(api)))
      val avg = times.sum / times.size

      println(s"${benchmark.name} took: ${nanosecondsToMilliseconds(avg)}ms")
      benchmark.tearDown(api)
    } finally {
      api ! Shutdown()
    }
  }

  benchmarks.foreach(benchmark => runBenchmark(benchmark))

}
