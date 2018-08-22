package de.hpi.svedeb.table

import de.hpi.svedeb.utils.Utils.{RowId, ValueType}

object OptionalColumnType {
  def apply(values: Option[Int]*): OptionalColumnType = OptionalColumnType(values.toIndexedSeq)
}

/**
  * This class is used to query a column but keeping the original RowIds.
  *
  * Even though this class may stores a lot of unnecessary values, we argue that it is useful for the HashJoin.
  * Additionally, using a Sequence of Options allows compressing, e.g. using RunLengthEncoding
  *
  * The second option would have been to store a Map[RowID, ValueType].
  * Depending on the selectivity this might end up in less RowIDs,
  * but we hope that in the case of sparse data encoding will help us in the first case.
  *
  * @param values
  */
case class OptionalColumnType(values: IndexedSeq[Option[ValueType]]) {}


