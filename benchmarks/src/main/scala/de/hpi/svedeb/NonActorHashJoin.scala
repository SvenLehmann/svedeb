package de.hpi.svedeb

import akka.actor.ActorRef
import de.hpi.svedeb.table.ColumnType

object NonActorHashJoin extends AbstractBenchmark {

  val columns = Seq("a")

  private var left: Map[Int, Map[String, ColumnType]] = _
  private var right: Map[Int, Map[String, ColumnType]] = _

  override def setup(api: ActorRef, tableSize: Int, numberOfColumns: Int, partitionSize: Int, distinctValues: Int, tableRatio: Double): Unit = {
    left = DataGenerator.generateData(columns, tableSize, partitionSize, distinctValues)
    right = DataGenerator.generateData(columns, tableSize/10, partitionSize, distinctValues)
  }

  override def runBenchmark(api: ActorRef): Unit = {
    // Build hash table of right
    val hashTable = right.mapValues { partition =>
      partition("a")
        .values
        .groupBy(f => f)
        .withDefaultValue(Seq())
    }

    // Probe with left
    left.flatMap {
      case (_, leftPartition) =>
        hashTable.map {
          case (_, rightPartitionHashTable) =>
            val leftValues = leftPartition("a")
            for {
              l <- leftValues.values
              r <- rightPartitionHashTable(l)
              if l == r
            } yield {
              (l, r)
            }
        }
    }
  }

  override def tearDown(api: ActorRef): Unit = {}

  override val name: String = "NonActorHashJoin"
}
