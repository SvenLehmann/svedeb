package de.hpi.svedeb.table.worker

import akka.actor.ActorRef
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.table.{ColumnType, OptionalColumnType}
import de.hpi.svedeb.table.worker.PartitionWorker.{InternalScanColumns, InternalScannedValues}
import org.scalatest.Matchers._

class PartitionWorkerTest extends AbstractActorTest("PartitionWorkerTest") {

  "A partition worker" should "scan all columns" in {

    val columns = Map("a" -> generateColumnTestProbe(1, "a", ColumnType(1, 2, 3, 4)))

    val partitionWorker = system.actorOf(PartitionWorker.props(columns))
    partitionWorker ! InternalScanColumns(ActorRef.noSender, Seq(1, 3))
    val response = expectMsgType[InternalScannedValues]

    response.values shouldEqual Map("a" -> OptionalColumnType(None, Some(2), None, Some(4)))
  }

}
