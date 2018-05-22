package de.hpi.svedeb

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import de.hpi.svedeb.table.Column.{ScanColumn, ScannedValues}
import de.hpi.svedeb.table.ColumnType
import de.hpi.svedeb.table.Partition.{ColumnsRetrieved, GetColumns}
import de.hpi.svedeb.table.Table.{GetPartitions, PartitionsInTable}
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
}

