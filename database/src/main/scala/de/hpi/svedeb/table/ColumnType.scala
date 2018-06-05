package de.hpi.svedeb.table

object ColumnType {
  def apply(values: String*): ColumnType = ColumnType(values.toIndexedSeq)
}
/**
  * ColumnType is optimized for index-lookups.
  * We assume that filtering by index will happen more often than scanning the sequence.
  * In an usual query only one column will be scanned, whereas all the other columns need to be filtered by index.
  * TODO: Evaluate whether this is true
  *
  */
case class ColumnType(values: IndexedSeq[String]) {

  def append(value: String): ColumnType = {
    ColumnType(this.values :+ value)
  }

  def filterByPredicate(predicate: String => Boolean): Seq[Int] = {
    values.zipWithIndex.filter { case (value, index) => predicate(value) }.map(_._2)
  }

  def filterByIndices(indices: Seq[Int]): ColumnType = {
    ColumnType(indices.map(values).toIndexedSeq)
  }

  def size(): Int = {
    values.size
  }
}


