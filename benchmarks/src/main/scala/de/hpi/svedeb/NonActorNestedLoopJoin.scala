package de.hpi.svedeb

import akka.actor.ActorRef
import de.hpi.svedeb.table.ColumnType

object NonActorNestedLoopJoin extends AbstractBenchmark {

  val columns = Seq("a")

  private var left: Map[Int, Map[String, ColumnType]] = _
  private var right: Map[Int, Map[String, ColumnType]] = _

  override def setup(api: ActorRef, tableSize: Int, numberOfColumns: Int, partitionSize: Int, distinctValues: Int, tableRatio: Double): Unit = {
    left = DataGenerator.generateData(columns, tableSize, partitionSize, distinctValues)
    right = DataGenerator.generateData(columns, tableSize/10, partitionSize, distinctValues)
  }

  override def runBenchmark(api: ActorRef): Unit = {
    left.flatMap {
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
  }

  override def tearDown(api: ActorRef): Unit = {}

  override val name: String = "NonActorNestedLoopJoin"
}
