package de.hpi.svedeb.operators.workers

import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.operators.HashJoinOperator.LeftJoinSide
import de.hpi.svedeb.operators.helper.PartitionedHashTableActor.{ListValues, ListedValues}
import de.hpi.svedeb.operators.helper.PartitionedHashTableEntry
import de.hpi.svedeb.operators.workers.HashWorker.{HashJob, HashedTable}
import de.hpi.svedeb.table.ColumnType
import org.scalatest.Matchers._

class HashWorkerTest extends AbstractActorTest("HashWorker") {
  "A HashWorker" should "hash a table" in {
    val inputTable = generateTableTestProbe(Seq(Map("columnA" -> ColumnType(1, 2, 2, 3))))

    // TODO  change hash function
    val expectedOutput = Map(
      1 -> Seq(PartitionedHashTableEntry(0, 0, 1)),
      2 -> Seq(PartitionedHashTableEntry(0, 1, 2), PartitionedHashTableEntry(0, 2, 2)),
      3 -> Seq(PartitionedHashTableEntry(0, 3, 3)))

    val hashWorker = system.actorOf(HashWorker.props(inputTable, "columnA", LeftJoinSide))
    hashWorker ! HashJob()

    val actualOutput = expectMsgType[HashedTable]

    actualOutput.side shouldEqual LeftJoinSide
    actualOutput.result.size shouldEqual expectedOutput.size
    actualOutput.result.foreach{ case (hash, actor) =>
      actor ! ListValues()
      val result = expectMsgType[ListedValues].values
      result shouldEqual expectedOutput(hash)
    }
  }

}
