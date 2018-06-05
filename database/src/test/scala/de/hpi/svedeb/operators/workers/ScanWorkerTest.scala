package de.hpi.svedeb.operators.workers

import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.operators.workers.ScanWorker.{ScanJob, ScanWorkerResult}
import de.hpi.svedeb.table.ColumnType
import org.scalatest.Matchers._

class ScanWorkerTest extends AbstractActorTest("ScanWorker") {
  "A scan worker" should "return scanned partition" in {
    val partition = generatePartitionTestProbe(0, Map("columnA" -> ColumnType("a", "b"))).partition
    val scanWorker = system.actorOf(ScanWorker.props(partition, 0, "columnA", _ => true))

    scanWorker ! ScanJob()
    val workerResult = expectMsgType[ScanWorkerResult]

    assert(workerResult.partition.isDefined)
    checkPartition(workerResult.partition.get, Map("columnA" -> ColumnType("a", "b")))
  }

  it should "return filtered partition" in {
    val inputData = Map("columnA" -> ColumnType("b", "c"), "columnB" -> ColumnType("d", "e"))
    val partition = generatePartitionTestProbe(0, inputData).partition
    val scanWorker = system.actorOf(ScanWorker.props(partition, 0, "columnA", value => value == "b"))

    scanWorker ! ScanJob()
    val workerResult = expectMsgType[ScanWorkerResult]

    checkPartition(workerResult.partition.get, Map("columnA" -> ColumnType("b"), "columnB" -> ColumnType("d")))
  }
}
