package de.hpi.svedeb

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestActor, TestKit, TestProbe}
import de.hpi.svedeb.table.Column._
import de.hpi.svedeb.table.ColumnType
import de.hpi.svedeb.table.Partition._
import de.hpi.svedeb.table.Table._
import org.scalatest.Matchers._

abstract class AbstractActorTest(name: String) extends TestKit(ActorSystem(name))
  with ImplicitSender with AbstractTest {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def checkColumnsValues(column: ActorRef, expectedValues: ColumnType): Unit = {
    column ! ScanColumn()
    val scannedValues = expectMsgType[ScannedValues]
    scannedValues.values shouldEqual expectedValues
  }

  def checkPartition(partition: ActorRef, expectedPartition: Map[String, ColumnType]): Unit = {
    partition ! GetColumns()
    val columns = expectMsgType[ColumnsRetrieved]
    val actualPartition = columns.columns.mapValues(columnRef => {
      columnRef ! ScanColumn()
      val values = expectMsgType[ScannedValues]
      values.values
    })

    actualPartition shouldEqual expectedPartition
  }

  def checkTable(table: ActorRef, expectedTable: Seq[Map[String, ColumnType]]): Unit = {
    table ! GetPartitions()
    val partitions = expectMsgType[PartitionsInTable]
    partitions.partitions.size shouldEqual expectedTable.size
    partitions.partitions.zipWithIndex.foreach{ case (partition, id) => checkPartition(partition, expectedTable(id))}
  }

  def generateColumnTestProbe(partitionId: Int, columnName: String, columnDefinition: ColumnType): ActorRef = {
    val column = TestProbe("Column")

    column.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(None) => sender.tell(ScannedValues(partitionId, columnName, columnDefinition), column.ref); TestActor.KeepRunning
      case ScanColumn(Some(indices)) => sender.tell(ScannedValues(partitionId, columnName, columnDefinition.filterByIndices(indices)), column.ref); TestActor.KeepRunning
      case AppendValue(_) => sender.tell(ValueAppended(partitionId, columnName), column.ref); TestActor.KeepRunning
      case FilterColumn(predicate) => sender.tell(FilteredRowIndices(partitionId, columnName, columnDefinition.filterByPredicate(predicate)), column.ref); TestActor.KeepRunning
    })

    column.ref
  }

  def generatePartitionTestProbe(partitionId: Int, partitionDefinition: Map[String, ColumnType]): (ActorRef, Map[String, ActorRef]) = {
    val partition = TestProbe("Partition")

    val columns = partitionDefinition.map{ case (columnName, values) => (columnName, generateColumnTestProbe(partitionId, columnName, values))}

    partition.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ListColumnNames() => sender.tell(ColumnNameList(partitionDefinition.keys.toSeq), partition.ref); TestActor.KeepRunning
      case GetColumns() => sender.tell(ColumnsRetrieved(columns), partition.ref); TestActor.KeepRunning
      case GetColumn(columnName) => sender.tell(ColumnRetrieved(partitionId, columnName, columns(columnName)), partition.ref); TestActor.KeepRunning
      case m => throw new Exception("Unexpected message type")
    })

    (partition.ref, columns)
  }

  def generateTableTestProbe(tableDefinition: Seq[Map[String, ColumnType]]): ActorRef = {
    val table = TestProbe("Table")

    val partitionsWithColumns = tableDefinition.zipWithIndex.map { case (partitionDefinition, partitionId) => generatePartitionTestProbe(partitionId, partitionDefinition)}
    val partitions = partitionsWithColumns.map(_._1)

    table.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case GetPartitions() => sender.tell(PartitionsInTable(partitions), table.ref); TestActor.KeepRunning
      case ListColumnsInTable() => {
        if (tableDefinition.isEmpty) {
          sender.tell(ColumnList(Seq.empty), table.ref)
        } else {
          val columnNames = tableDefinition.head.keys.toSeq
          sender.tell(ColumnList(columnNames), table.ref)
        }
      }; TestActor.KeepRunning
      case GetColumnFromTable(columnName) => {
        val columns = partitionsWithColumns.map {case (_, map) => map(columnName)}
        sender.tell(ActorsForColumn(columnName, columns), table.ref)
      }; TestActor.KeepRunning
    })

    table.ref
  }
}

