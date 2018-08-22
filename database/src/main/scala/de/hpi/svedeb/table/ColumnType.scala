package de.hpi.svedeb.table

import de.hpi.svedeb.utils.Utils.{RowId, ValueType}

object ColumnType {
  def apply(values: Int*): ColumnType = ColumnType(values.toIndexedSeq)
}
/**
  * ColumnType is optimized for index-lookups.
  * We assume that filtering by index will happen more often than scanning the sequence.
  * In an usual query only one column will be scanned, whereas all the other columns need to be filtered by index.
  * TODO: Evaluate whether this is true
  *
  */
case class ColumnType(values: IndexedSeq[ValueType]) {

  def append(value: ValueType): ColumnType = {
    ColumnType(this.values :+ value)
  }

  def filterByPredicate(predicate: ValueType => Boolean): Seq[RowId] = {
    values.zipWithIndex.filter { case (value, _) => predicate(value) }.map(_._2)
  }

  def filterByIndices(indices: Seq[RowId]): ColumnType = {
    ColumnType(indices.map(values).toIndexedSeq)
  }

  def filterByIndicesWithOptional(indices: Seq[RowId]): OptionalColumnType = {
    val optionalValues = values.zipWithIndex.map {
      case (value, rowId) => if (indices.contains(rowId)) Some(value) else None
    }.toIndexedSeq

    OptionalColumnType(optionalValues)
  }

  def size(): Int = {
    values.size
  }
}


