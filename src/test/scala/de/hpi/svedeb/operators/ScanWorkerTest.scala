package de.hpi.svedeb.operators

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.operators.workers.ScanWorker
import de.hpi.svedeb.operators.workers.ScanWorker.{ScanJob, ScanWorkerResult}
import de.hpi.svedeb.table.Column.{FilterColumn, FilteredRowIndizes, ScanColumn, ScannedValues}
import de.hpi.svedeb.table.ColumnType
import de.hpi.svedeb.table.Partition.{ColumnsRetrieved, GetColumns}
import org.scalatest.Matchers._

// TODO: Consider splitting up this test into multiple smaller ones that do not have so many dependencies
class ScanWorkerTest extends AbstractActorTest("ScanWorker") {
  "A scan worker" should "return scanned partition" in {
    val column = TestProbe()
    column.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(indizes) => sender ! ScannedValues("columnA", ColumnType(IndexedSeq("a", "b"))); TestActor.KeepRunning
      case FilterColumn(predicate) ⇒ sender ! FilteredRowIndizes(Seq(0, 1)); TestActor.KeepRunning
    })

    val partition = TestProbe()
    partition.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case GetColumns() ⇒ sender ! ColumnsRetrieved(Map("columnA" -> column.ref)); TestActor.KeepRunning
    })

    val scanWorker = system.actorOf(ScanWorker.props(partition.ref))

    scanWorker ! ScanJob("columnA", _ => true)
    val workerResult = expectMsgType[ScanWorkerResult]
    workerResult.partiton ! GetColumns()

    val columns = expectMsgType[ColumnsRetrieved]
    columns.columns.foreach{ case (_, columnRef) => columnRef ! ScanColumn(None)}

    val scannedValues = expectMsgType[ScannedValues]
    scannedValues.values shouldEqual ColumnType(IndexedSeq("a", "b"))
  }

  it should "return filtered partition" in {
    val columnA = TestProbe()
    columnA.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(indizes) => sender ! ScannedValues("columnA", ColumnType(IndexedSeq("b"))); TestActor.KeepRunning
      case FilterColumn(predicate) ⇒ sender ! FilteredRowIndizes(Seq(1)); TestActor.KeepRunning
    })

    val columnB = TestProbe()
    columnB.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(indizes) => sender ! ScannedValues("columnB", ColumnType(IndexedSeq("d"))); TestActor.KeepRunning
    })

    val partition = TestProbe()
    partition.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case GetColumns() ⇒ {
        val columnMap = Map("columnA" -> columnA.ref, "columnB" -> columnB.ref)
        sender ! ColumnsRetrieved(columnMap)
      }; TestActor.KeepRunning
    })

    val scanWorker = system.actorOf(ScanWorker.props(partition.ref))

    scanWorker ! ScanJob("columnA", value => value == "b")
    val workerResult = expectMsgType[ScanWorkerResult]

    workerResult.partiton ! GetColumns()
    val columns = expectMsgType[ColumnsRetrieved]

    columns.columns.size shouldEqual 2

    columns.columns.foreach{ case (_, columnRef) => columnRef ! ScanColumn(None)}
    val scannedValues = expectMsgType[ScannedValues]

    scannedValues.values.size() shouldEqual 1
  }
}
