package de.hpi.svedeb

object Utils {
  val numberOfIterations = 3

  def time[R](description: String, block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println(s"Elapsed time in $description: ${(t1 - t0)/1000000.0}ms")
    result
  }
}
