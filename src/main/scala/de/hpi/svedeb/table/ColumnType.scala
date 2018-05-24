package de.hpi.svedeb.table

import de.hpi.svedeb.DataType

object ColumnType {
  def apply[T <: DataType](values: T*): ColumnType[T] = ColumnType[T](values.toIndexedSeq)
}
/**
  * ColumnType is optimized for index-lookups.
  * We assume that filtering by index will happen more often than scanning the sequence.
  * In an usual query only one column will be scanned, whereas all the other columns need to be filtered by index.
  * TODO: Evaluate whether this is true
  *
  */
case class ColumnType[T <: DataType](values: IndexedSeq[T]) {

  def append(value: T): ColumnType[T] = {
    ColumnType(this.values :+ value)
  }

  def filterByPredicate(predicate: T => Boolean): Seq[Int] = {
    values.zipWithIndex.filter { case (value, _) => predicate(value) }.map(_._2)
  }

  def filterByIndices(indices: Seq[Int]): ColumnType[T] = {
    ColumnType(indices.map(values).toIndexedSeq)
  }

  def size(): Int = {
    values.size
  }
}


