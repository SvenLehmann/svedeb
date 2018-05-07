package de.hpi.svedeb.table

/**
  * ColumnType is optimized for index-lookups.
  * We assume that filtering by index will happen more often than scanning the sequence.
  * In an usual query only one column will be scanned, whereas all the other columns need to be filtered by index.
  *
  * @param values the sequence of values
  */
case class ColumnType(values: IndexedSeq[String] = Vector.empty[String]) {
  def append(value: String): ColumnType = {
    ColumnType(this.values :+ value)
  }

  def filterByPredicate(predicate: String => Boolean): Seq[Int] = {
    values.zipWithIndex.filter { case (value, index) => predicate(value) }.map(_._2)
  }

  def filterByIndizes(indizes: Seq[Int]): ColumnType = {
    ColumnType(indizes.map(values).toIndexedSeq)
  }

  def size(): Int = {
    values.size
  }
}
