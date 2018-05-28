package de.hpi.svedeb.table

import akka.actor.{Actor, ActorLogging, Props}
import de.hpi.svedeb.DataType
import de.hpi.svedeb.table.Column._

import scala.reflect.ClassTag

object Column {
  case class AppendValue[T](value: T)(implicit ev$1: T => DataType[T])
  case class FilterColumn[T](predicate: T => Boolean)(implicit ev$1: T => DataType[T])
  // None returns all values
  case class ScanColumn(indices: Option[Seq[Int]] = None)
  case class GetColumnName()
  case class GetColumnSize()

  // Result events
  case class FilteredRowIndices(partitionId: Int, columnName: String, indices: Seq[Int])
  case class ScannedValues(partitionId: Int, columnName: String, values: ColumnType[DataType[_]])
  case class ValueAppended(partitionId: Int, columnName: String)
  case class ColumnName(name: String)
  case class ColumnSize(partitionId: Int, size: Int)

  def props[T](partitionId: Int, columnName: String, values: ColumnType[T] = ColumnType[T]())
              (implicit ev$1: T => DataType[T]): Props = {
    import de.hpi.svedeb.DataTypeImplicits._
    Props(new Column(partitionId, columnName, values))
  }
}

class Column[T](partitionId: Int, columnName: String, initialValues: ColumnType[T])(implicit ev$1: T => DataType[T]) extends Actor with ActorLogging {
  override def receive: Receive = active(initialValues)

  private def handleFilter(values: ColumnType[T], predicate: T => Boolean): Unit = {
    val filteredIndices = values.filterByPredicate(predicate)
    sender() ! FilteredRowIndices(partitionId, columnName, filteredIndices)
  }

  private def handleScan(values: ColumnType[T], indices: Option[Seq[Int]]): Unit = {
    if (indices.isDefined) {
      val scannedValues = values.filterByIndices(indices.get)
      sender() ! ScannedValues(partitionId, columnName, scannedValues)
    } else {
      sender() ! ScannedValues(partitionId, columnName, values)
    }
  }

  private def handleAppendValue(values: ColumnType[T], value: T): Unit = {
    log.debug("Appending value: {}", value)
    context.become(active(values.append(value)))

    sender() ! ValueAppended(partitionId, columnName)
  }

  private def active[U: ClassTag](values: ColumnType[T]): Receive = {
    case AppendValue(value: T) => handleAppendValue(values, value)
    case FilterColumn(predicate: (T => Boolean)) => handleFilter(values, predicate)
    case ScanColumn(indices) => handleScan(values, indices)
    case GetColumnName() => sender() ! ColumnName(columnName)
    case GetColumnSize() => sender() ! ColumnSize(partitionId, values.size())
    case m => throw new Exception(s"Message not understood: $m")
  }
}

