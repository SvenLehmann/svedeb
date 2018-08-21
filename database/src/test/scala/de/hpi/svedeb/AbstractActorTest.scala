package de.hpi.svedeb

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestActor, TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import de.hpi.svedeb.management.TableManager._
import de.hpi.svedeb.table.Column._
import de.hpi.svedeb.table.ColumnType
import de.hpi.svedeb.table.Partition._
import de.hpi.svedeb.table.Table._
import org.scalatest.Matchers._

object TestKitSpec {
  def config: String = """
  akka.actor.provider = "cluster"
  akka.remote.netty.tcp.port = 0
  akka.remote.artery.canonical.port = 0
  akka.loglevel = "DEBUG"
"""
}

abstract class AbstractActorTest(name: String)
  extends TestKit(ActorSystem(name, ConfigFactory.parseString(TestKitSpec.config)))
  with ImplicitSender with AbstractTest {

  case class PartitionTestProbe(partition: ActorRef, columns: Map[String, ActorRef])

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def materializeColumn(column: ActorRef): ColumnType = {
    column ! ScanColumn()
    expectMsgType[ScannedValues].values
  }

  def materializePartition(partition: ActorRef): Map[String, ColumnType] = {
    partition ! GetColumns()
    val columns = expectMsgType[ColumnsRetrieved]
    columns.columns.mapValues(columnRef => materializeColumn(columnRef))
  }

  def materializeTable(table: ActorRef): Map[Int, Map[String, ColumnType]] = {
    table ! GetPartitions()
    val partitions = expectMsgType[PartitionsInTable]
    partitions.partitions.mapValues(partition => materializePartition(partition))
  }

  def checkColumnsValues(column: ActorRef, expectedValues: ColumnType): Unit = {
    val actualColumn = materializeColumn(column)
    actualColumn shouldEqual expectedValues
  }

  def checkPartition(partition: ActorRef, expectedPartition: Map[String, ColumnType]): Unit = {
    val actualPartition = materializePartition(partition)
    actualPartition shouldEqual expectedPartition
  }

  def checkTable(table: ActorRef, expectedTable: Map[Int, Map[String, ColumnType]]): Unit = {
    val actualTable = materializeTable(table)
    actualTable shouldEqual expectedTable
  }

  def checkTableIgnoreOrder(table: ActorRef, expectedTable: Map[Int, Map[String, ColumnType]]): Unit = {
    val actualTable = materializeTable(table)

    actualTable.foreach {
      case (_, actualPartition) => expectedTable.exists {
        case (_, expectedPartition) => actualPartition == expectedPartition
      }
    }
  }

  def generateColumnTestProbe(partitionId: Int, columnName: String, columnDefinition: ColumnType): ActorRef = {
    val column = TestProbe("Column")

    column.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): TestActor.AutoPilot =
        msg match {
          case ScanColumn(None) =>
            sender.tell(ScannedValues(partitionId, columnName, columnDefinition), column.ref)
            TestActor.KeepRunning
          case ScanColumn(Some(indices)) =>
            sender.tell(ScannedValues(partitionId, columnName, columnDefinition.filterByIndices(indices)), column.ref)
            TestActor.KeepRunning
          case AppendValue(_) =>
            sender.tell(ValueAppended(partitionId, columnName), column.ref); TestActor.KeepRunning
          case FilterColumn(predicate) =>
            val filteredIndices = columnDefinition.filterByPredicate(predicate)
            sender.tell(FilteredRowIndices(partitionId, columnName, filteredIndices), column.ref)
            TestActor.KeepRunning
          case m => throw new Exception(s"Message not understood: $m")
        }
    })

    column.ref
  }

  def generatePartitionTestProbe(partitionId: Int,
                                 partitionDefinition: Map[String, ColumnType]): PartitionTestProbe = {
    val partition = TestProbe("Partition")

    val columns = partitionDefinition.map {
      case (columnName, values) => (columnName, generateColumnTestProbe(partitionId, columnName, values))
    }

    partition.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): TestActor.AutoPilot =
        msg match {
          case ListColumnNames() =>
            sender.tell(ColumnNameList(partitionDefinition.keys.toSeq), partition.ref)
            TestActor.KeepRunning
          case GetColumns() =>
            sender.tell(ColumnsRetrieved(partitionId, columns), partition.ref)
            TestActor.KeepRunning
          case GetColumn(columnName) =>
            sender.tell(ColumnRetrieved(partitionId, columnName, columns(columnName)), partition.ref)
            TestActor.KeepRunning
          case ScanColumns(indices) =>
            sender.tell(ScannedColumns(partitionId, partitionDefinition.mapValues(_.filterByIndicesWithOptional(indices))), partition.ref)
            TestActor.KeepRunning
          case m => throw new Exception(s"Message not understood: $m")
        }
    })

    PartitionTestProbe(partition.ref, columns)
  }

  def generateTableTestProbe(tableDefinition: Seq[Map[String, ColumnType]]): ActorRef = {
    val table = TestProbe("Table")

    val partitionsWithColumns = tableDefinition.zipWithIndex.map {
      case (partitionDefinition, partitionId) => (partitionId, generatePartitionTestProbe(partitionId, partitionDefinition))
    }.toMap
    val partitions = partitionsWithColumns.mapValues(_.partition)

    table.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): TestActor.AutoPilot =
        msg match {
          case GetPartitions() => sender.tell(PartitionsInTable(partitions), table.ref); TestActor.KeepRunning
          case ListColumnsInTable() =>
            if (tableDefinition.isEmpty) {
              sender.tell(ColumnList(Seq.empty), table.ref)
            } else {
              val columnNames = tableDefinition.head.keys.toSeq
              sender.tell(ColumnList(columnNames), table.ref)
            }
            TestActor.KeepRunning
          case GetColumnFromTable(columnName) =>
            val columns = partitionsWithColumns.mapValues { _.columns(columnName) }
            sender.tell(ActorsForColumn(columnName, columns), table.ref)
            TestActor.KeepRunning
          case AddRowToTable(_) =>
            sender.tell(RowAddedToTable(), table.ref)
            TestActor.KeepRunning
          case m => throw new Exception(s"Message not understood: $m")
        }
    })

    table.ref
  }

  def generateTableManagerTestProbe(tables: Seq[ActorRef]): ActorRef = {
    val tableManager = TestProbe("TableManager")
    tableManager.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): TestActor.AutoPilot =
        msg match {
          case AddPartition(partitionId, partitionData, _) =>
            sender ! PartitionCreated(partitionId, generatePartitionTestProbe(partitionId, partitionData).partition)
            TestActor.KeepRunning
          case FetchTable(_) => sender ! TableFetched(tables.head); TestActor.KeepRunning
          case RemoveTable(_) => sender ! TableRemoved(); TestActor.KeepRunning
          case AddTable(_, _, _) =>
            sender ! TableAdded(tables.headOption.getOrElse(ActorRef.noSender))
            TestActor.KeepRunning
          case AddRemoteTable(_, _) => sender ! RemoteTableAdded(); TestActor.KeepRunning
          case m => throw new Exception(s"Message not understood: $m")
        }
    })

    tableManager.ref
  }
}

