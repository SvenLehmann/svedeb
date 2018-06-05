package de.hpi.svedeb.utils

object Utils {
  final val defaultPartitionSize = 10

  type RowId = Int
  type ValueType = Int

  def time[R](description: String, block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println(s"Elapsed time in $description: ${(t1 - t0)/1000000.0}ms")
    result
  }
}
