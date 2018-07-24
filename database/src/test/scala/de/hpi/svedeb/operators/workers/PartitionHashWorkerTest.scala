package de.hpi.svedeb.operators.workers

import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.operators.workers.PartitionHashWorker.{FetchValuesForKey, FetchedValues, HashPartition, HashedPartitionKeys}
import de.hpi.svedeb.table.ColumnType
import org.scalatest.Matchers._

class PartitionHashWorkerTest extends AbstractActorTest("PartitionHashWorkerTest") {
  "A partition hash worker" should "hash correctly" in {
    val partition = generatePartitionTestProbe(0, Map("columnA" -> ColumnType(1, 2, 2, 3))).partition
    val worker = system.actorOf(PartitionHashWorker.props(partition, "columnA"))
    worker ! HashPartition()
    val result = expectMsgType[HashedPartitionKeys]
    // TODO change hash function
    result.hashKeys.sorted shouldEqual Seq(1, 2, 3)

    worker ! FetchValuesForKey(1)
    val values1 = expectMsgType[FetchedValues].values
    values1 shouldEqual Seq((0, 0, 1))
    worker ! FetchValuesForKey(2)
    val values2 = expectMsgType[FetchedValues].values
    values2.sortBy(_._2) shouldEqual Seq((0, 1, 2), (0, 2, 2))
    worker ! FetchValuesForKey(3)
    val values3 = expectMsgType[FetchedValues].values
    values3 shouldEqual Seq((0, 3, 3))
  }
}
