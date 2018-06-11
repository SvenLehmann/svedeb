package de.hpi.svedeb

import akka.actor.ActorRef
import de.hpi.svedeb.api.API.Result
import de.hpi.svedeb.table.ColumnType

object NonActorNestedLoopJoin extends AbstractBenchmark {

  val columns = Seq("a", "b")
  val partitionSize = 1000

  private var left: Map[Int, Map[String, ColumnType]] = _
  private var right: Map[Int, Map[String, ColumnType]] = _

  override def setup(api: ActorRef, tableSize: Int): Unit = {
    left = DataGenerator.generateData(columns, tableSize, partitionSize)
    right = DataGenerator.generateData(columns, tableSize/10, partitionSize)
  }

  override def runBenchmark(api: ActorRef): Result = {
    val result = left.flatMap {
      case (_, leftPartition) =>
        right.map {
          case (_, rightPartition) =>
            val leftValues = leftPartition("a")
            val rightValues = rightPartition("a")
            for {
              l <- leftValues.values
              r <- rightValues.values
              if l == r
            } yield (l, r)
        }
    }

    val flattened = result.flatten
    val flattenedSeq = flattened.toSeq

    Result(ActorRef.noSender)
  }

  override def tearDown(api: ActorRef): Unit = {}

  override val name: String = "NonActorNestedLoopJoin"
}
