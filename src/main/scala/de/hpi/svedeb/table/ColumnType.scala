package de.hpi.svedeb.table

import de.hpi.svedeb.DataType
import scala.reflect.runtime.universe.TypeTag

sealed class DataTypeEnum()
case class IntType() extends DataTypeEnum
case class StringType() extends DataTypeEnum

object ColumnTypeFactory {
  def createTyped[T](values: Seq[T])
               (implicit ev$1: T => DataType[T], evT: TypeTag[T]): ColumnType[_] = {
    ColumnType[T](values:_*)
  }

  def create(dataType: DataTypeEnum): ColumnType[_] = {
    dataType match {
      case IntType() => createTyped[Int](Seq.empty)
      case StringType() => createTyped[String](Seq.empty)
    }
  }
}

/**
  * ColumnType is optimized for index-lookups.
  * We assume that filtering by index will happen more often than scanning the sequence.
  * In an usual query only one column will be scanned, whereas all the other columns need to be filtered by index.
  * TODO: Evaluate whether this is true
  */
case class ColumnType[T](values: T*)(implicit ev$1: T => DataType[T], evT: TypeTag[T]) {

  def append[S](value: S)(implicit ev$2: S => DataType[S], evS: TypeTag[S]): ColumnType[T] = {
    if (evS != evT) {
      throw new Exception("Type mismatch")
    }

    ColumnType(values :+ value.asInstanceOf[T] :_*)
  }

  def filterByPredicate(predicate: T => Boolean): Seq[Int] = {
    values.zipWithIndex.filter { case (value, _) => predicate(value) }.map(_._2)
  }

  def filterByIndices(indices: Seq[Int]): ColumnType[DataType[T]] = {
    ColumnType(indices.map(values).map(ev$1(_)) :_*)
  }

  def size(): Int = {
    values.size
  }
}


